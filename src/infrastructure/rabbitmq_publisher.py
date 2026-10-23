"""Confirmed, mandatory RabbitMQ publication for transaction envelopes."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Protocol

import aiormq
from aio_pika import DeliveryMode, Message
from aio_pika.exceptions import DeliveryError
from pamqp.commands import Basic

from domain.transaction import RetryEnvelope, TransactionEvent
from infrastructure.tracing import NoopTransactionTracer, TransactionTracer


class PublishExchange(Protocol):
    """The small exchange surface needed to test confirmed publication."""

    def publish(
        self, message: Message, routing_key: str, *, mandatory: bool, timeout: float
    ) -> Awaitable[object | None]: ...


class PublicationError(RuntimeError):
    """A message must not be considered durably accepted."""


class PublicationNacked(PublicationError):
    """RabbitMQ negatively confirmed the publication."""


class UnroutablePublication(PublicationError):
    """Mandatory routing returned the publication without a destination."""


class PublicationTimeout(PublicationError):
    """A confirm was not received before the configured timeout."""


class PublicationIndeterminate(PublicationError):
    """The connection failed before confirmation; durable outcome is unknown."""


class TransactionPublisher:
    """Serialize envelopes and accept only positive RabbitMQ publisher confirms."""

    def __init__(
        self,
        work_exchange: PublishExchange,
        retry_exchange: PublishExchange,
        *,
        confirm_timeout_seconds: float,
        now: Callable[[], datetime] | None = None,
        tracer: TransactionTracer | None = None,
    ) -> None:
        if confirm_timeout_seconds <= 0:
            raise ValueError("confirm_timeout_seconds must be positive")
        self._work_exchange = work_exchange
        self._retry_exchange = retry_exchange
        self._confirm_timeout_seconds = confirm_timeout_seconds
        self._now = now or (lambda: datetime.now(UTC))
        self._tracer = tracer or NoopTransactionTracer()

    async def publish_work(self, event: TransactionEvent) -> RetryEnvelope:
        """Publish a newly accepted event to the durable work route."""

        envelope = RetryEnvelope(
            schema_version=1,
            event=event,
            attempt=0,
            accepted_at=self._now(),
            currency_allowlist_version=event.currency_allowlist_version,
        )
        with self._tracer.span(
            "transaction.publish",
            stage="broker_publish",
            transaction_id=event.transaction_id,
            attempt=0,
        ) as span:
            await self._publish(self._work_exchange, envelope, routing_key="work")
            span.set_outcome("confirmed")
        return envelope

    async def publish_retry(self, envelope: RetryEnvelope, delay_seconds: int) -> None:
        """Publish a pre-built retry envelope to one approved retry route."""

        if delay_seconds not in (1, 2, 4, 8, 16, 32, 60):
            raise ValueError("delay_seconds must be one of the approved retry delays")
        with self._tracer.span(
            "transaction.publish_retry",
            stage="retry_forwarding",
            transaction_id=envelope.event.transaction_id,
            attempt=envelope.attempt,
        ) as span:
            await self._publish(
                self._retry_exchange,
                envelope,
                routing_key=f"retry.{delay_seconds}s",
            )
            span.set_outcome("confirmed")

    async def publish_failure(self, envelope: RetryEnvelope, reason: str) -> None:
        """Publish a permanent-failure envelope only after validating its reason."""

        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be nonempty")
        with self._tracer.span(
            "transaction.publish_failure",
            stage="failure_forwarding",
            transaction_id=envelope.event.transaction_id,
            attempt=envelope.attempt,
        ) as span:
            await self._publish(
                self._work_exchange,
                envelope,
                routing_key="failure",
                failure_reason=reason,
            )
            span.set_outcome("confirmed")

    async def publish_invalid_envelope(self, body: bytes, reason: str) -> None:
        """Durably retain an undecodable delivery before its original is acknowledged."""

        if not isinstance(body, bytes):
            raise ValueError("body must be bytes")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be nonempty")
        await self._confirm_publish(
            self._work_exchange,
            Message(
                body,
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                headers={"failure_reason": reason},
            ),
            routing_key="failure",
        )

    async def _publish(
        self,
        exchange: PublishExchange,
        envelope: RetryEnvelope,
        *,
        routing_key: str,
        failure_reason: str | None = None,
    ) -> None:
        await self._confirm_publish(
            exchange,
            self._message(envelope, failure_reason=failure_reason),
            routing_key=routing_key,
        )

    async def _confirm_publish(
        self, exchange: PublishExchange, message: Message, *, routing_key: str
    ) -> None:
        try:
            confirmation = await exchange.publish(
                message,
                routing_key,
                mandatory=True,
                timeout=self._confirm_timeout_seconds,
            )
        except TimeoutError as error:
            raise PublicationTimeout(
                "RabbitMQ did not confirm publication before the timeout"
            ) from error
        except DeliveryError as error:
            if type(error.frame).__name__ == "Return":
                raise UnroutablePublication(
                    "RabbitMQ returned an unroutable mandatory message"
                ) from error
            raise PublicationNacked("RabbitMQ rejected publication") from error
        except (aiormq.exceptions.AMQPError, ConnectionError, OSError) as error:
            raise PublicationIndeterminate(
                "RabbitMQ connection failed before publication confirmation"
            ) from error

        if isinstance(confirmation, Basic.Ack):
            return
        if isinstance(confirmation, (Basic.Nack, Basic.Reject)):
            raise PublicationNacked("RabbitMQ negatively confirmed publication")
        raise PublicationIndeterminate("RabbitMQ publication completed without a positive confirm")

    def _message(self, envelope: RetryEnvelope, *, failure_reason: str | None) -> Message:
        event = envelope.event
        body = {
            "schema_version": envelope.schema_version,
            "event": {
                "transaction_id": event.transaction_id,
                "user_id": event.user_id,
                "original_amount": format(event.original_amount, "f"),
                "currency": event.currency,
                "event_timestamp": event.event_timestamp.isoformat(),
                "currency_allowlist_version": event.currency_allowlist_version,
            },
            "attempt": envelope.attempt,
            "accepted_at": envelope.accepted_at.isoformat(),
            "currency_allowlist_version": envelope.currency_allowlist_version,
            "last_failure_kind": envelope.last_failure_kind,
        }
        if failure_reason is not None:
            body["failure_reason"] = failure_reason
        headers = {
            "schema_version": envelope.schema_version,
            "transaction_id": event.transaction_id,
            "attempt": envelope.attempt,
            "currency_allowlist_version": envelope.currency_allowlist_version,
        }
        self._tracer.inject(headers)
        return Message(
            json.dumps(body, separators=(",", ":"), sort_keys=True).encode(),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=event.transaction_id,
            headers=headers,
        )
