"""Real-infrastructure proof that processed transactions become query-visible."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import aio_pika
import pytest
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry
from sqlalchemy import create_engine

from api.app import create_app
from application.money_converter import MoneyConverter
from application.transaction_processor import TransactionProcessor
from domain.rates import RateQuote
from domain.transaction import TransactionEvent
from infrastructure.metrics import MetricsRecorder
from infrastructure.postgres_repository import PostgresTransactionRepository
from infrastructure.rabbitmq_consumer import TransactionConsumer
from infrastructure.rabbitmq_publisher import TransactionPublisher
from tests.recovery.support import RecoveryEnvironment, assert_queues_empty

pytestmark = pytest.mark.skipif(
    not os.getenv("RABBITMQ_URL"), reason="RABBITMQ_URL is not configured"
)


class DeterministicRateProvider:
    """A real processor dependency with a fixed, validated historical quote."""

    def get_usd_rate(self, currency: str, requested_date: date) -> RateQuote:
        return RateQuote(
            base_currency=currency,
            quote_currency="USD",
            rate=Decimal("1.250000000000"),
            requested_date=requested_date,
            rate_date=requested_date,
            provider="t026-deterministic",
            fetched_at=datetime.now(UTC),
        )


class QueryResources:
    """Bind the HTTP app to PostgreSQL; the test controller owns engine disposal."""

    def __init__(self, repository: PostgresTransactionRepository) -> None:
        self.metrics = MetricsRecorder(registry=CollectorRegistry())
        self.repository = repository

    def close(self) -> None:
        """The test owns the shared engine because consumer and HTTP use it together."""


@pytest.fixture()
def environment(tmp_path: object) -> RecoveryEnvironment:
    value = RecoveryEnvironment(tmp_path)  # type: ignore[arg-type]
    value.start()
    try:
        yield value
    finally:
        value.close()


def _event(
    transaction_id: str,
    *,
    user_id: str,
    amount: str,
    timestamp: datetime,
) -> TransactionEvent:
    return TransactionEvent.from_accepted_input(
        supported_currencies=frozenset({"EUR", "USD"}),
        currency_allowlist_version="currencies-v1",
        transaction_id=transaction_id,
        user_id=user_id,
        original_amount=Decimal(amount),
        currency="EUR",
        event_timestamp=timestamp,
    )


async def _publish_work(environment: RecoveryEnvironment, event: TransactionEvent) -> None:
    """Use the confirmed application publisher, rather than a raw AMQP publish."""

    connection = await aio_pika.connect(environment.rabbitmq_url)
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        publisher = TransactionPublisher(
            await channel.get_exchange("transactions"),
            await channel.get_exchange("transactions.retry"),
            confirm_timeout_seconds=5,
        )
        await publisher.publish_work(event)
    finally:
        await connection.close()


async def _consume_one(environment: RecoveryEnvironment, processor: TransactionProcessor) -> None:
    """Run one actual broker delivery through the real consumer and publisher."""

    connection = await aio_pika.connect(environment.rabbitmq_url)
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        publisher = TransactionPublisher(
            await channel.get_exchange("transactions"),
            await channel.get_exchange("transactions.retry"),
            confirm_timeout_seconds=5,
        )
        delivery = await (await channel.get_queue("transactions.work")).get(fail=True, no_ack=False)
        await TransactionConsumer(processor, publisher).handle(delivery)
    finally:
        await connection.close()


def _summary(client: TestClient, user_id: str) -> dict[str, object]:
    response = client.get(f"/users/{user_id}/summary")
    assert response.status_code == 200, response.text
    return response.json()


def test_processed_transactions_are_eventually_visible_through_user_query_routes(
    environment: RecoveryEnvironment,
) -> None:
    """T026: only committed records appear in exact, isolated query projections."""

    target_user = f"query-user-{uuid.uuid4().hex}"
    other_user = f"other-user-{uuid.uuid4().hex}"
    initial = _event(
        "e2e-transaction",
        user_id=target_user,
        amount="12.34567890",
        timestamp=datetime(2026, 10, 22, 7, 30, tzinfo=timezone(-timedelta(hours=3))),
    )
    same_timestamp = datetime(2026, 10, 22, 10, 30, tzinfo=UTC)
    later_events = (
        _event("e2e-a", user_id=target_user, amount="1.00000000", timestamp=same_timestamp),
        _event("e2e-b", user_id=target_user, amount="2.00000000", timestamp=same_timestamp),
        _event(
            "e2e-end-exclusive",
            user_id=target_user,
            amount="4.00000000",
            timestamp=datetime(2026, 10, 22, 11, tzinfo=UTC),
        ),
        _event("e2e-other-user", user_id=other_user, amount="9.00000000", timestamp=same_timestamp),
    )
    engine = create_engine(environment.database_url, pool_pre_ping=True)
    repository = PostgresTransactionRepository(engine)
    processor = TransactionProcessor(DeterministicRateProvider(), MoneyConverter(), repository)

    try:
        with TestClient(create_app(QueryResources(repository))) as client:
            assert _summary(client, target_user) == {
                "user_id": target_user,
                "total_usd": "0",
                "transaction_count": 0,
            }

            asyncio.run(_publish_work(environment, initial))
            environment.wait_for(
                lambda: environment.queue_states()["transactions.work"]["ready"] == 1,
                "the confirmed work publication to be ready without a consumer",
            )
            # Acceptance is only a durable handoff: no record is query-visible yet.
            assert _summary(client, target_user)["transaction_count"] == 0

            asyncio.run(_consume_one(environment, processor))
            environment.wait_for(
                lambda: _summary(client, target_user)["transaction_count"] == 1,
                "the committed transaction to become visible through the HTTP summary",
            )

            for event in later_events:
                asyncio.run(_publish_work(environment, event))
                asyncio.run(_consume_one(environment, processor))

            # A confirmed resend reaches the real consumer, which resolves it as a duplicate.
            asyncio.run(_publish_work(environment, initial))
            asyncio.run(_consume_one(environment, processor))
            environment.wait_for(
                lambda: _summary(client, target_user)["transaction_count"] == 4,
                "all distinct committed transactions to become query-visible",
            )

            assert environment.row_count() == 5
            assert _summary(client, target_user) == {
                "user_id": target_user,
                "total_usd": "24.18209862",
                "transaction_count": 4,
            }
            assert _summary(client, other_user) == {
                "user_id": other_user,
                "total_usd": "11.25000000",
                "transaction_count": 1,
            }

            response = client.get(
                f"/users/{target_user}/transactions",
                params={
                    "from": "2026-10-22T10:00:00Z",
                    "to": "2026-10-22T11:00:00Z",
                    "limit": 100,
                    "offset": 0,
                },
            )
            assert response.status_code == 200, response.text
            page = response.json()
            assert page["limit"] == 100
            assert page["offset"] == 0
            assert [item["id"] for item in page["items"]] == [
                "e2e-a",
                "e2e-b",
                "e2e-transaction",
            ]
            persisted_initial = page["items"][-1]
            assert persisted_initial == {
                "id": "e2e-transaction",
                "user_id": target_user,
                "original_amount": "12.34567890",
                "currency": "EUR",
                "timestamp": "2026-10-22T10:30:00+00:00",
                "rate": "1.250000000000",
                "rate_requested_date": "2026-10-22",
                "rate_date": "2026-10-22",
                "converted_usd_amount": "15.43209862",
            }

            offset_response = client.get(
                f"/users/{target_user}/transactions",
                params={
                    "from": "2026-10-22T10:00:00Z",
                    "to": "2026-10-22T11:00:00Z",
                    "limit": 1,
                    "offset": 1,
                },
            )
            assert offset_response.status_code == 200, offset_response.text
            assert [item["id"] for item in offset_response.json()["items"]] == ["e2e-b"]
    finally:
        try:
            assert_queues_empty(environment)
        finally:
            engine.dispose()
