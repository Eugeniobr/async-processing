"""Real-broker proofs for retry recovery and durable permanent-failure routing."""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

import aio_pika
import pytest
from sqlalchemy import create_engine

from application.money_converter import MoneyConverter
from application.ports import PermanentRateError, RetryableRateError
from application.transaction_processor import TransactionProcessor
from domain.rates import RateQuote
from infrastructure.postgres_repository import PostgresTransactionRepository
from infrastructure.rabbitmq_consumer import TransactionConsumer, decode_envelope
from infrastructure.rabbitmq_publisher import TransactionPublisher
from tests.recovery.support import RecoveryEnvironment, assert_queues_empty

pytestmark = pytest.mark.skipif(
    not os.getenv("RABBITMQ_URL"), reason="RABBITMQ_URL is not configured"
)


class Provider:
    """Deterministically emulate one unavailable/invalid provider response then recovery."""

    def __init__(self, *results: RateQuote | Exception) -> None:
        self._results = list(results)

    def get_usd_rate(self, _currency: str, _requested_date: date) -> RateQuote:
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class FailOnceRepository:
    """Expose a temporary database-facing failure before delegating to PostgreSQL."""

    def __init__(self, repository: PostgresTransactionRepository) -> None:
        self._repository = repository
        self._failed = False

    def record_first(self, transaction: object) -> object:
        if not self._failed:
            self._failed = True
            raise ConnectionError("test database outage")
        return self._repository.record_first(transaction)  # type: ignore[arg-type]


def _quote() -> RateQuote:
    return RateQuote(
        base_currency="EUR",
        quote_currency="USD",
        rate=Decimal("1.250000000000"),
        requested_date=date(2026, 10, 22),
        rate_date=date(2026, 10, 22),
        provider="integration-test",
        fetched_at=datetime.now(UTC),
    )


async def _consume_one(
    environment: RecoveryEnvironment, processor: TransactionProcessor
) -> tuple[int, str | None]:
    connection = await aio_pika.connect(environment.rabbitmq_url)
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        publisher = TransactionPublisher(
            await channel.get_exchange("transactions"),
            await channel.get_exchange("transactions.retry"),
            confirm_timeout_seconds=5,
        )
        delivery = await (await channel.get_queue("transactions.work")).get(fail=True, no_ack=False)
        envelope = decode_envelope(delivery.body)
        await TransactionConsumer(processor, publisher).handle(delivery)
        return envelope.attempt, envelope.event.transaction_id
    finally:
        await connection.close()


async def _wait_and_consume_one(
    environment: RecoveryEnvironment, processor: TransactionProcessor
) -> tuple[int, str | None]:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        connection = await aio_pika.connect(environment.rabbitmq_url)
        try:
            channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
            queue = await channel.get_queue("transactions.work")
            delivery = await queue.get(fail=False, no_ack=False)
            if delivery is None:
                await asyncio.sleep(0.05)
                continue
            envelope = decode_envelope(delivery.body)
            publisher = TransactionPublisher(
                await channel.get_exchange("transactions"),
                await channel.get_exchange("transactions.retry"),
                confirm_timeout_seconds=5,
            )
            await TransactionConsumer(processor, publisher).handle(delivery)
            return envelope.attempt, envelope.event.transaction_id
        finally:
            await connection.close()
    raise AssertionError("retry delivery did not return to the work queue before the deadline")


async def _failure_message(environment: RecoveryEnvironment) -> aio_pika.IncomingMessage:
    connection = await aio_pika.connect(environment.rabbitmq_url)
    try:
        channel = await connection.channel()
        message = await (await channel.get_queue("transactions.failure")).get(
            fail=True, no_ack=True
        )
        return message
    finally:
        await connection.close()


@pytest.fixture()
def environment(tmp_path: object) -> RecoveryEnvironment:
    value = RecoveryEnvironment(tmp_path)  # type: ignore[arg-type]
    value.start()
    try:
        yield value
    finally:
        value.close()


@pytest.mark.parametrize(
    "failure", [RetryableRateError("provider unavailable"), ConnectionError("db")]
)
def test_temporary_provider_or_database_failure_retries_then_persists(
    environment: RecoveryEnvironment, failure: Exception
) -> None:
    transaction_id = f"retry-recovery-{uuid.uuid4().hex}"
    environment.publish_event(transaction_id)
    engine = create_engine(environment.database_url)
    try:
        repository = PostgresTransactionRepository(engine)
        if isinstance(failure, RetryableRateError):
            processor = TransactionProcessor(
                Provider(failure, _quote()), MoneyConverter(), repository
            )
        else:
            processor = TransactionProcessor(
                Provider(_quote(), _quote()),
                MoneyConverter(),
                FailOnceRepository(repository),  # type: ignore[arg-type]
            )

        assert asyncio.run(_consume_one(environment, processor)) == (0, transaction_id)
        assert asyncio.run(_wait_and_consume_one(environment, processor)) == (1, transaction_id)
        assert environment.row_count() == 1
        assert_queues_empty(environment)
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "reason",
    ["no_eligible_rate", "unsupported_currency", "provider_coverage_changed"],
)
def test_permanent_provider_results_are_forwarded_with_the_original_allowlist_version(
    environment: RecoveryEnvironment, reason: str
) -> None:
    transaction_id = f"permanent-{uuid.uuid4().hex}"
    environment.publish_event(transaction_id)
    engine = create_engine(environment.database_url)
    try:
        processor = TransactionProcessor(
            Provider(PermanentRateError(reason)),
            MoneyConverter(),
            PostgresTransactionRepository(engine),
        )
        assert asyncio.run(_consume_one(environment, processor)) == (0, transaction_id)
        message = asyncio.run(_failure_message(environment))
        payload = json.loads(message.body)
        assert payload["event"]["transaction_id"] == transaction_id
        assert payload["currency_allowlist_version"] == "currencies-v1"
        assert payload["failure_reason"] == reason
        assert environment.row_count() == 0
        assert_queues_empty(environment)
    finally:
        engine.dispose()


def test_malformed_envelope_is_retained_in_the_failure_queue(
    environment: RecoveryEnvironment,
) -> None:
    raw_body = b'{"schema_version":2}'

    async def publish_and_consume() -> None:
        connection = await aio_pika.connect(environment.rabbitmq_url)
        try:
            channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
            await (await channel.get_exchange("transactions")).publish(
                aio_pika.Message(raw_body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key="work",
                mandatory=True,
            )
            delivery = await (await channel.get_queue("transactions.work")).get(
                fail=True, no_ack=False
            )
            processor = TransactionProcessor(
                Provider(_quote()),
                MoneyConverter(),
                PostgresTransactionRepository(create_engine(environment.database_url)),
            )
            publisher = TransactionPublisher(
                await channel.get_exchange("transactions"),
                await channel.get_exchange("transactions.retry"),
                confirm_timeout_seconds=5,
            )
            await TransactionConsumer(processor, publisher).handle(delivery)
        finally:
            await connection.close()

    asyncio.run(publish_and_consume())
    message = asyncio.run(_failure_message(environment))
    assert message.body == raw_body
    assert message.headers == {"failure_reason": "invalid_envelope"}
    assert environment.row_count() == 0
    assert_queues_empty(environment)
