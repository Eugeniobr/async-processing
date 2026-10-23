from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pamqp.commands import Basic

from domain.transaction import RetryEnvelope, TransactionEvent
from infrastructure.rabbitmq_publisher import (
    PublicationIndeterminate,
    PublicationNacked,
    PublicationTimeout,
    TransactionPublisher,
)


class FakeExchange:
    def __init__(self, result: object | None = None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: list[tuple[object, str, bool, float]] = []

    async def publish(
        self, message: object, routing_key: str, *, mandatory: bool, timeout: float
    ) -> object:
        self.calls.append((message, routing_key, mandatory, timeout))
        if self.error:
            raise self.error
        return self.result


def event() -> TransactionEvent:
    return TransactionEvent(
        transaction_id="tx-1",
        user_id="user-1",
        original_amount=Decimal("12.34000000"),
        currency="EUR",
        event_timestamp=datetime(2026, 10, 22, 10, 30, tzinfo=UTC),
        currency_allowlist_version="v1",
    )


def envelope() -> RetryEnvelope:
    return RetryEnvelope(
        schema_version=1,
        event=event(),
        attempt=2,
        accepted_at=datetime(2026, 10, 22, 10, 31, tzinfo=UTC),
        currency_allowlist_version="v1",
        last_failure_kind="provider_unavailable",
    )


def test_positive_confirm_publishes_a_persistent_versioned_work_envelope() -> None:
    exchange = FakeExchange(Basic.Ack(delivery_tag=1, multiple=False))
    publisher = TransactionPublisher(
        exchange,
        exchange,
        confirm_timeout_seconds=3,
        now=lambda: datetime(2026, 10, 22, 10, 31, tzinfo=UTC),
    )

    published = asyncio.run(publisher.publish_work(event()))

    message, routing_key, mandatory, timeout = exchange.calls[0]
    assert published.attempt == 0
    assert routing_key == "work"
    assert mandatory is True
    assert timeout == 3
    assert message.delivery_mode == 2
    assert message.message_id == "tx-1"
    assert message.content_type == "application/json"
    assert message.headers == {
        "schema_version": 1,
        "transaction_id": "tx-1",
        "attempt": 0,
        "currency_allowlist_version": "v1",
    }
    assert json.loads(message.body) == {
        "schema_version": 1,
        "event": {
            "transaction_id": "tx-1",
            "user_id": "user-1",
            "original_amount": "12.34000000",
            "currency": "EUR",
            "event_timestamp": "2026-10-22T10:30:00+00:00",
            "currency_allowlist_version": "v1",
        },
        "attempt": 0,
        "accepted_at": "2026-10-22T10:31:00+00:00",
        "currency_allowlist_version": "v1",
        "last_failure_kind": None,
    }


@pytest.mark.parametrize(
    "result", [Basic.Nack(delivery_tag=1, multiple=False, requeue=False), None]
)
def test_negative_or_missing_confirm_is_not_accepted(result: object | None) -> None:
    exchange = FakeExchange(result)
    publisher = TransactionPublisher(exchange, exchange, confirm_timeout_seconds=1)

    error = PublicationNacked if result is not None else PublicationIndeterminate
    with pytest.raises(error):
        asyncio.run(publisher.publish_retry(envelope(), 1))


def test_timeout_is_not_accepted() -> None:
    exchange = FakeExchange(error=TimeoutError())
    publisher = TransactionPublisher(exchange, exchange, confirm_timeout_seconds=1)

    with pytest.raises(PublicationTimeout):
        asyncio.run(publisher.publish_retry(envelope(), 1))


def test_connection_loss_before_confirm_is_indeterminate() -> None:
    exchange = FakeExchange(error=ConnectionError("closed"))
    publisher = TransactionPublisher(exchange, exchange, confirm_timeout_seconds=1)

    with pytest.raises(PublicationIndeterminate, match="connection failed"):
        asyncio.run(publisher.publish_failure(envelope(), "invalid_rate"))


def test_rejects_unapproved_retry_delay_before_publication() -> None:
    exchange = FakeExchange(Basic.Ack(delivery_tag=1, multiple=False))
    publisher = TransactionPublisher(exchange, exchange, confirm_timeout_seconds=1)

    with pytest.raises(ValueError, match="approved retry delays"):
        asyncio.run(publisher.publish_retry(envelope(), 3))
    assert exchange.calls == []
