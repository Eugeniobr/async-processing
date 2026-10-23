from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from domain.outcomes import Duplicate, PermanentFailure, Processed, RetryableFailure
from infrastructure.rabbitmq_consumer import TransactionConsumer, decode_envelope
from infrastructure.rabbitmq_publisher import PublicationIndeterminate


class Delivery:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.ack_count = 0

    async def ack(self) -> None:
        self.ack_count += 1


class OrderedDelivery(Delivery):
    def __init__(self, calls: list[str]) -> None:
        super().__init__(body())
        self.calls = calls

    async def ack(self) -> None:
        self.calls.append("ack")
        await super().ack()


class Processor:
    def __init__(self, outcome: object, calls: list[str]) -> None:
        self.outcome = outcome
        self.calls = calls

    def process(self, event: object) -> object:
        self.calls.append("process")
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


class Publisher:
    def __init__(self, calls: list[str], error: Exception | None = None) -> None:
        self.calls = calls
        self.error = error
        self.retry: tuple[object, int] | None = None
        self.failure: tuple[object, str] | None = None
        self.invalid: tuple[bytes, str] | None = None

    async def publish_retry(self, envelope: object, delay: int) -> None:
        self.calls.append("retry")
        self.retry = (envelope, delay)
        if self.error:
            raise self.error

    async def publish_failure(self, envelope: object, reason: str) -> None:
        self.calls.append("failure")
        self.failure = (envelope, reason)
        if self.error:
            raise self.error

    async def publish_invalid_envelope(self, raw_body: bytes, reason: str) -> None:
        self.calls.append("invalid")
        self.invalid = (raw_body, reason)
        if self.error:
            raise self.error


class Metrics:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def record_processing_failure(self, kind: str) -> None:
        self.calls.append(f"metric:{kind}")


class FailingMetrics:
    def record_processing_failure(self, _kind: str) -> None:
        raise ConnectionError("metrics sink unavailable")


def body() -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "event": {
                "transaction_id": "tx-consumer-1",
                "user_id": "user-1",
                "original_amount": "3.00000000",
                "currency": "EUR",
                "event_timestamp": "2026-10-20T10:00:00+00:00",
                "currency_allowlist_version": "currencies-v1",
            },
            "attempt": 0,
            "accepted_at": "2026-10-20T10:01:00+00:00",
            "currency_allowlist_version": "currencies-v1",
            "last_failure_kind": None,
        }
    ).encode()


def test_processed_and_duplicate_ack_after_processor_returns() -> None:
    for outcome in (Processed("tx-consumer-1"), Duplicate("tx-consumer-1")):
        calls: list[str] = []
        delivery = Delivery(body())
        consumer = TransactionConsumer(Processor(outcome, calls), Publisher(calls))

        asyncio.run(consumer.handle(delivery))

        assert calls == ["process"]
        assert delivery.ack_count == 1


def test_retry_forwards_incremented_envelope_before_acknowledgement() -> None:
    calls: list[str] = []
    delivery = Delivery(body())
    publisher = Publisher(calls)
    consumer = TransactionConsumer(
        Processor(RetryableFailure("database offline"), calls), publisher
    )

    asyncio.run(consumer.handle(delivery))

    envelope, delay = publisher.retry
    assert calls == ["process", "retry"]
    assert delay == 1
    assert envelope.attempt == 1
    assert envelope.last_failure_kind == "database offline"
    assert envelope.event.transaction_id == "tx-consumer-1"
    assert delivery.ack_count == 1


def test_permanent_failure_forwards_before_acknowledgement() -> None:
    calls: list[str] = []
    delivery = Delivery(body())
    publisher = Publisher(calls)
    consumer = TransactionConsumer(
        Processor(PermanentFailure("no eligible rate"), calls), publisher
    )

    asyncio.run(consumer.handle(delivery))

    assert calls == ["process", "failure"]
    assert publisher.failure[1] == "no eligible rate"
    assert delivery.ack_count == 1


def test_failure_metric_is_recorded_after_confirmed_forwarding_and_before_acknowledgement() -> None:
    calls: list[str] = []
    delivery = OrderedDelivery(calls)
    consumer = TransactionConsumer(
        Processor(RetryableFailure("database offline"), calls),
        Publisher(calls),
        metrics=Metrics(calls),
    )

    asyncio.run(consumer.handle(delivery))

    assert calls == ["process", "retry", "metric:retryable", "ack"]


def test_uncertain_forwarding_or_cancellation_leaves_delivery_unacknowledged() -> None:
    calls: list[str] = []

    async def recover() -> None:
        calls.append("recover")

    retry_delivery = Delivery(body())
    retry_consumer = TransactionConsumer(
        Processor(RetryableFailure("database offline"), calls),
        Publisher(calls, PublicationIndeterminate("connection lost")),
        recover_unconfirmed=recover,
    )
    cancelled_delivery = Delivery(body())
    cancelled_consumer = TransactionConsumer(
        Processor(asyncio.CancelledError(), calls), Publisher(calls)
    )

    asyncio.run(retry_consumer.handle(retry_delivery))
    try:
        asyncio.run(cancelled_consumer.handle(cancelled_delivery))
    except asyncio.CancelledError:
        pass

    assert calls == ["process", "retry", "recover", "process"]
    assert retry_delivery.ack_count == 0
    assert cancelled_delivery.ack_count == 0


def test_hooks_run_after_real_outcome_boundary_and_before_ack() -> None:
    calls: list[str] = []
    delivery = Delivery(body())

    async def checkpoint() -> None:
        assert delivery.ack_count == 0
        calls.append("checkpoint")

    consumer = TransactionConsumer(
        Processor(Processed("tx-consumer-1"), calls), Publisher(calls), after_processing=checkpoint
    )

    asyncio.run(consumer.handle(delivery))

    assert calls == ["process", "checkpoint"]
    assert delivery.ack_count == 1


def test_invalid_envelope_is_confirmed_to_failure_before_acknowledgement() -> None:
    calls: list[str] = []
    delivery = Delivery(b'{"schema_version":2}')
    publisher = Publisher(calls)

    asyncio.run(
        TransactionConsumer(Processor(Processed("unused"), calls), publisher).handle(delivery)
    )

    assert calls == ["invalid"]
    assert publisher.invalid == (delivery.body, "invalid_envelope")
    assert delivery.ack_count == 1


def test_invalid_envelope_remains_unacknowledged_when_failure_forwarding_is_uncertain() -> None:
    calls: list[str] = []
    delivery = Delivery(b"not json")
    publisher = Publisher(calls, PublicationIndeterminate("connection lost"))

    async def recover() -> None:
        calls.append("recover")

    asyncio.run(
        TransactionConsumer(
            Processor(Processed("unused"), calls), publisher, recover_unconfirmed=recover
        ).handle(delivery)
    )

    assert calls == ["invalid", "recover"]
    assert delivery.ack_count == 0


def test_processing_or_ack_failure_closes_the_consumer_recovery_boundary() -> None:
    for processor_outcome, ack_fails in (
        (RuntimeError("processor bug"), False),
        (Processed("tx-consumer-1"), True),
    ):
        calls: list[str] = []

        class FailingAckDelivery(Delivery):
            async def ack(self) -> None:
                await super().ack()
                if ack_fails:
                    raise ConnectionError("ack connection lost")

        async def recover() -> None:
            calls.append("recover")

        delivery = FailingAckDelivery(body())
        consumer = TransactionConsumer(
            Processor(processor_outcome, calls), Publisher(calls), recover_unconfirmed=recover
        )

        asyncio.run(consumer.handle(delivery))

        assert calls[-1] == "recover"


def test_metrics_failure_leaves_confirmed_forwarding_recoverable() -> None:
    calls: list[str] = []

    async def recover() -> None:
        calls.append("recover")

    delivery = Delivery(body())
    consumer = TransactionConsumer(
        Processor(RetryableFailure("database offline"), calls),
        Publisher(calls),
        metrics=FailingMetrics(),
        recover_unconfirmed=recover,
    )

    asyncio.run(consumer.handle(delivery))

    assert calls == ["process", "retry", "recover"]
    assert delivery.ack_count == 0


@pytest.mark.parametrize(
    "raw_body",
    [
        b"not json",
        b'{"schema_version": 1, "event": []}',
        b'{"schema_version": true, "event": {}}',
        b'{"schema_version": 1, "event": {"original_amount": 1e9999999999999999999999999999}}',
        b'{"schema_version": 1, "event": {"original_amount": {"unexpected": "object"}}}',
        (
            b'{"schema_version": 1, "event": {"original_amount": "1.00", '
            b'"event_timestamp": "999999999999-01-01T00:00:00+00:00"}}'
        ),
    ],
)
def test_decode_envelope_classifies_extreme_or_unexpected_input_as_invalid(raw_body: bytes) -> None:
    with pytest.raises(ValueError, match="invalid transaction retry envelope"):
        decode_envelope(raw_body)


def test_decode_envelope_preserves_decimal_without_float_coercion() -> None:
    # Construct the JSON fixture from a literal rather than a Python float so the
    # assertion detects accidental json-decoder float coercion.
    encoded = body().replace(b'"3.00000000"', b"12345678.12345678")
    assert decode_envelope(encoded).event.original_amount == Decimal("12345678.12345678")


def test_decode_envelope_rejects_boolean_schema_version() -> None:
    encoded = body().replace(b'"schema_version": 1', b'"schema_version": true')

    with pytest.raises(ValueError, match="invalid transaction retry envelope"):
        decode_envelope(encoded)


@pytest.mark.parametrize(
    ("attempt", "delay_seconds"),
    [(1, 1), (2, 2), (3, 4), (4, 8), (5, 16), (6, 32), (7, 60), (8, 60)],
)
def test_retry_delay_selects_the_approved_sequence_and_cap(
    attempt: int, delay_seconds: int
) -> None:
    assert TransactionConsumer._retry_delay(attempt) == delay_seconds
