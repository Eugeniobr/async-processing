"""Manual-ack RabbitMQ delivery control around the queue-agnostic processor."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol

from application.transaction_processor import ProcessingOutcome
from domain.outcomes import Duplicate, PermanentFailure, Processed, RetryableFailure
from domain.transaction import RetryEnvelope, TransactionEvent
from infrastructure.tracing import NoopTransactionTracer, TransactionTracer

RETRY_DELAYS_SECONDS = (1, 2, 4, 8, 16, 32, 60)


class Delivery(Protocol):
    body: bytes
    headers: object

    def ack(self) -> Awaitable[None]: ...


DeliveryRecovery = Callable[[], Awaitable[None]]


class Processor(Protocol):
    def process(self, event: TransactionEvent) -> ProcessingOutcome: ...


class Publisher(Protocol):
    def publish_retry(self, envelope: RetryEnvelope, delay_seconds: int) -> Awaitable[None]: ...

    def publish_failure(self, envelope: RetryEnvelope, reason: str) -> Awaitable[None]: ...

    def publish_invalid_envelope(self, body: bytes, reason: str) -> Awaitable[None]: ...


class ProcessingFailureMetrics(Protocol):
    """The narrow observability surface owned by the composition root."""

    def record_processing_failure(self, kind: str) -> None: ...


class TransactionConsumer:
    """Acknowledge only completed persistence or confirmed durable forwarding."""

    def __init__(
        self,
        processor: Processor,
        publisher: Publisher,
        *,
        metrics: ProcessingFailureMetrics | None = None,
        after_processing: Callable[[], Awaitable[None]] | None = None,
        after_retry_publish: Callable[[], Awaitable[None]] | None = None,
        recover_unconfirmed: DeliveryRecovery | None = None,
        tracer: TransactionTracer | None = None,
    ) -> None:
        self._processor = processor
        self._publisher = publisher
        self._metrics = metrics
        self._after_processing = after_processing
        self._after_retry_publish = after_retry_publish
        self._recover_unconfirmed = recover_unconfirmed
        self._tracer = tracer or NoopTransactionTracer()

    async def handle(self, delivery: Delivery) -> None:
        """Handle one manual-ack delivery without requeueing it in-process."""

        try:
            envelope = decode_envelope(delivery.body)
        except asyncio.CancelledError:
            raise
        except ValueError:
            try:
                await self._publisher.publish_invalid_envelope(delivery.body, "invalid_envelope")
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._recover(delivery)
                return
            try:
                self._record_failure("permanent")
                await self._ack_completed(delivery)
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._recover(delivery)
            return

        parent_context = self._tracer.extract(getattr(delivery, "headers", None))
        with self._tracer.span(
            "transaction.consume",
            stage="broker_delivery",
            transaction_id=envelope.event.transaction_id,
            attempt=envelope.attempt,
            parent_context=parent_context,
        ) as delivery_span:
            try:
                with self._tracer.span(
                    "transaction.conversion",
                    stage="conversion",
                    transaction_id=envelope.event.transaction_id,
                    attempt=envelope.attempt,
                ) as processing_span:
                    outcome = self._processor.process(envelope.event)
                    processing_span.set_outcome(type(outcome).__name__.lower())
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._recover(delivery)
                return
            if isinstance(outcome, (Processed, Duplicate)):
                try:
                    if self._after_processing:
                        await self._after_processing()
                    await delivery.ack()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    await self._recover(delivery)
                    return
                delivery_span.set_outcome(
                    "processed" if isinstance(outcome, Processed) else "duplicate"
                )
                return
            if isinstance(outcome, RetryableFailure):
                try:
                    retry_envelope = envelope.for_confirmed_retry(outcome.reason)
                    await self._publisher.publish_retry(
                        retry_envelope, self._retry_delay(retry_envelope.attempt)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    await self._recover(delivery)
                    return
                try:
                    self._record_failure("retryable")
                    if self._after_retry_publish:
                        await self._after_retry_publish()
                    await delivery.ack()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    await self._recover(delivery)
                    return
                delivery_span.set_outcome("retry_forwarded")
                return
            if isinstance(outcome, PermanentFailure):
                try:
                    await self._publisher.publish_failure(envelope, outcome.reason)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    await self._recover(delivery)
                    return
                try:
                    self._record_failure("permanent")
                    await delivery.ack()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    await self._recover(delivery)
                    return
                delivery_span.set_outcome("failure_forwarded")
                return

            await self._recover(delivery)

    async def _ack_completed(self, delivery: Delivery) -> None:
        """ACK only a completed result; recover when the broker cannot accept it."""

        try:
            await delivery.ack()
        except asyncio.CancelledError:
            raise
        except Exception:
            await self._recover(delivery)

    async def _recover(self, delivery: Delivery) -> None:
        """Close the delivery channel so RabbitMQ redelivers instead of pinning it.

        No negative acknowledgement is sent here: immediate requeue would create a hot loop
        when a dependency or publication path is still unavailable. Closing the channel is a
        controlled consumer stop; RabbitMQ requeues its unacknowledged deliveries and the
        process supervisor may restart this bounded-prefetch consumer.
        """

        if self._recover_unconfirmed is not None:
            await self._recover_unconfirmed()
            return

        channel = getattr(delivery, "channel", None)
        close = getattr(channel, "close", None)
        if not callable(close):
            # Narrow fakes used by contract tests have no broker channel. They still
            # observe the essential invariant (no ACK); production composition always
            # supplies ``recover_unconfirmed=channel.close`` explicitly.
            return
        await close()

    def _record_failure(self, kind: str) -> None:
        """Observe only a confirmed durable forwarding, immediately before the ACK."""

        if self._metrics is not None:
            self._metrics.record_processing_failure(kind)

    @staticmethod
    def _retry_delay(attempt: int) -> int:
        return RETRY_DELAYS_SECONDS[min(attempt - 1, len(RETRY_DELAYS_SECONDS) - 1)]


def decode_envelope(body: bytes) -> RetryEnvelope:
    """Decode the publisher's versioned JSON envelope without Decimal float coercion."""

    try:
        payload = json.loads(body, parse_float=Decimal)
        if payload["schema_version"] != 1:
            raise ValueError("unsupported transaction retry envelope schema version")
        event_payload = payload["event"]
        event = TransactionEvent(
            transaction_id=event_payload["transaction_id"],
            user_id=event_payload["user_id"],
            original_amount=Decimal(str(event_payload["original_amount"])),
            currency=event_payload["currency"],
            event_timestamp=datetime.fromisoformat(event_payload["event_timestamp"]),
            currency_allowlist_version=event_payload["currency_allowlist_version"],
        )
        return RetryEnvelope(
            schema_version=payload["schema_version"],
            event=event,
            attempt=payload["attempt"],
            accepted_at=datetime.fromisoformat(payload["accepted_at"]),
            currency_allowlist_version=payload["currency_allowlist_version"],
            last_failure_kind=payload.get("last_failure_kind"),
        )
    except (
        KeyError,
        TypeError,
        ValueError,
        OverflowError,
        InvalidOperation,
        json.JSONDecodeError,
    ) as error:
        raise ValueError("invalid transaction retry envelope") from error
