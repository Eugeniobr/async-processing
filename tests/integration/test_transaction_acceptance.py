"""Compose-backed proof of the durable HTTP acceptance boundary."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import date
from decimal import Decimal

import aio_pika
import httpx
import pytest
from prometheus_client import CollectorRegistry

from api.app import create_app
from composition.settings import CurrencyAllowlist
from domain.queries import TransactionHistoryQuery, UserSummary
from domain.transaction import PersistedTransaction
from infrastructure.metrics import MetricsRecorder
from infrastructure.rabbitmq_publisher import TransactionPublisher
from tests.recovery.support import RecoveryEnvironment

pytestmark = pytest.mark.skipif(
    not os.getenv("RABBITMQ_URL"), reason="RABBITMQ_URL is not configured"
)


class EmptyRepository:
    """Acceptance does not require processed-query state."""

    def get_summary(self, user_id: str) -> UserSummary:
        return UserSummary(user_id, Decimal("0"), 0)

    def list_history(self, _query: TransactionHistoryQuery) -> list[PersistedTransaction]:
        return []


class AcceptanceResources:
    """Own the isolated broker connection used by the HTTP application."""

    def __init__(
        self, connection: aio_pika.abc.AbstractConnection, publisher: TransactionPublisher
    ) -> None:
        self.connection = connection
        self.publisher = publisher
        self.metrics = MetricsRecorder(registry=CollectorRegistry())
        self.repository = EmptyRepository()
        self.currency_allowlist = CurrencyAllowlist(
            version="currencies-v1",
            provider="ecb",
            source_url="https://example.invalid/currencies",
            retrieved_on=date(2026, 10, 22),
            codes=frozenset({"EUR", "USD"}),
        )

    async def close(self) -> None:
        await self.connection.close()


async def _resources(environment: RecoveryEnvironment) -> AcceptanceResources:
    connection = await aio_pika.connect(environment.rabbitmq_url)
    channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    publisher = TransactionPublisher(
        await channel.get_exchange("transactions"),
        await channel.get_exchange("transactions.retry"),
        confirm_timeout_seconds=5,
    )
    return AcceptanceResources(connection, publisher)


async def _get_work_delivery(
    environment: RecoveryEnvironment,
) -> aio_pika.abc.AbstractIncomingMessage:
    connection = await aio_pika.connect(environment.rabbitmq_url)
    try:
        channel = await connection.channel()
        delivery = await (await channel.get_queue("transactions.work")).get(fail=True, no_ack=False)
        # The caller only inspects this delivery; acknowledge before closing its channel.
        assert delivery.message_id
        await delivery.ack()
        return delivery
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


async def _submit(environment: RecoveryEnvironment, event_id: str) -> httpx.Response:
    resources = await _resources(environment)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(resources)), base_url="http://testserver"
        ) as client:
            return await client.post(
                "/transactions",
                json={
                    "id": event_id,
                    "user_id": "acceptance-user",
                    "amount": "12.34000000",
                    "currency": "EUR",
                    "timestamp": "2026-10-22T07:30:00-03:00",
                },
            )
    finally:
        await resources.close()


def test_http_acceptance_waits_for_real_confirm_and_retains_work_delivery(
    environment: RecoveryEnvironment,
) -> None:
    """A 202 follows a real publisher confirm, not merely handler invocation."""

    event_id = f"accept-{uuid.uuid4().hex}"
    response = asyncio.run(_submit(environment, event_id))

    assert response.status_code == 202, response.text
    assert response.json() == {"id": event_id, "status": "accepted"}
    environment.wait_for(
        lambda: environment.queue_states()["transactions.work"]["ready"] == 1,
        "the confirmed HTTP publication to be retained in the isolated work queue",
    )
    delivery = asyncio.run(_get_work_delivery(environment))
    assert delivery.message_id == event_id
    assert delivery.headers["transaction_id"] == event_id


def test_resubmitted_acceptance_can_create_duplicate_broker_deliveries_with_one_id(
    environment: RecoveryEnvironment,
) -> None:
    """An indeterminate producer retry is intentionally safe for downstream idempotency."""

    event_id = f"resubmit-{uuid.uuid4().hex}"
    first = asyncio.run(_submit(environment, event_id))
    second = asyncio.run(_submit(environment, event_id))

    assert first.status_code == 202, first.text
    assert second.status_code == 202, second.text
    environment.wait_for(
        lambda: environment.queue_states()["transactions.work"]["ready"] == 2,
        "the two confirmed producer submissions to remain as duplicate work deliveries",
    )
