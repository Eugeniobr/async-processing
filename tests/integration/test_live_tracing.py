"""Real local Jaeger evidence for the development-only transaction journey view."""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import aio_pika
import httpx
import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import create_engine

from api.app import create_app
from application.money_converter import MoneyConverter
from application.transaction_processor import TransactionProcessor
from composition.settings import CurrencyAllowlist
from domain.queries import TransactionHistoryQuery, UserSummary
from domain.transaction import PersistedTransaction
from infrastructure.frankfurter_ecb import FrankfurterEcbRateProvider
from infrastructure.metrics import MetricsRecorder
from infrastructure.postgres_repository import PostgresTransactionRepository
from infrastructure.rabbitmq_consumer import TransactionConsumer
from infrastructure.rabbitmq_publisher import TransactionPublisher
from infrastructure.tracing import TransactionTracer, build_local_tracer
from tests.recovery.support import RecoveryEnvironment, assert_queues_empty

JAEGER_QUERY_URL = os.getenv("JAEGER_QUERY_URL", "http://localhost:16686")
JAEGER_OTLP_URL = os.getenv("JAEGER_OTLP_URL", "http://localhost:4318/v1/traces")

pytestmark = pytest.mark.skipif(
    not os.getenv("RABBITMQ_URL"), reason="RABBITMQ_URL is not configured"
)


class EmptyRepository:
    """Acceptance uses the real write repository but needs no query data in this test."""

    def get_summary(self, user_id: str) -> UserSummary:
        return UserSummary(user_id, Decimal("0"), 0)

    def list_history(self, _query: TransactionHistoryQuery) -> list[PersistedTransaction]:
        return []


class TracedResources:
    """Own the API-side broker connection for a single isolated journey."""

    def __init__(
        self,
        connection: aio_pika.abc.AbstractConnection,
        publisher: TransactionPublisher,
        tracer: TransactionTracer,
    ) -> None:
        self.connection = connection
        self.publisher = publisher
        self.tracer = tracer
        self.metrics = MetricsRecorder(registry=CollectorRegistry())
        self.repository = EmptyRepository()
        self.currency_allowlist = CurrencyAllowlist(
            version="currencies-v1",
            provider="ecb",
            source_url="https://example.invalid/currencies",
            retrieved_on=datetime(2026, 10, 22, tzinfo=UTC).date(),
            codes=frozenset({"EUR", "USD"}),
        )

    async def close(self) -> None:
        await self.connection.close()


@pytest.fixture()
def environment(tmp_path: object) -> RecoveryEnvironment:
    value = RecoveryEnvironment(tmp_path)  # type: ignore[arg-type]
    value.start()
    try:
        yield value
    finally:
        value.close()


def _rate_client() -> httpx.Client:
    """Exercise the real rate adapter with a deterministic HTTP response."""

    return httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "base": "EUR",
                    "quote": "USD",
                    "rate": "1.250000000000",
                    "date": request.url.params["date"],
                },
            )
        )
    )


async def _run_journey(
    environment: RecoveryEnvironment, tracer: TransactionTracer, transaction_id: str
) -> tuple[str, int]:
    """Use the actual API route, AMQP delivery, processor, and PostgreSQL repository."""

    api_connection = await aio_pika.connect(environment.rabbitmq_url)
    api_channel = await api_connection.channel(publisher_confirms=True, on_return_raises=True)
    api_publisher = TransactionPublisher(
        await api_channel.get_exchange("transactions"),
        await api_channel.get_exchange("transactions.retry"),
        confirm_timeout_seconds=5,
        tracer=tracer,
    )
    resources = TracedResources(api_connection, api_publisher, tracer)
    engine = create_engine(environment.database_url, pool_pre_ping=True)
    rate_client = _rate_client()
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(resources)), base_url="http://testserver"
        ) as client:
            response = await client.post(
                "/transactions",
                json={
                    "id": transaction_id,
                    "user_id": "trace-user",
                    "amount": "12.34000000",
                    "currency": "EUR",
                    "timestamp": "2026-10-22T07:30:00-03:00",
                },
            )
        assert response.status_code == 202, response.text

        consumer_connection = await aio_pika.connect(environment.rabbitmq_url)
        try:
            channel = await consumer_connection.channel(
                publisher_confirms=True, on_return_raises=True
            )
            publisher = TransactionPublisher(
                await channel.get_exchange("transactions"),
                await channel.get_exchange("transactions.retry"),
                confirm_timeout_seconds=5,
                tracer=tracer,
            )
            delivery = await (await channel.get_queue("transactions.work")).get(
                fail=True, no_ack=False
            )
            traceparent = str(delivery.headers["traceparent"])
            processor = TransactionProcessor(
                FrankfurterEcbRateProvider(rate_client, tracer=tracer),
                MoneyConverter(),
                PostgresTransactionRepository(engine, tracer=tracer),
            )
            await TransactionConsumer(processor, publisher, tracer=tracer).handle(delivery)
        finally:
            await consumer_connection.close()
        return traceparent.split("-")[1], environment.row_count()
    finally:
        rate_client.close()
        engine.dispose()
        await resources.close()


def _spans(trace: dict[str, object]) -> list[dict[str, object]]:
    """Flatten Jaeger v2's OTLP-shaped response without depending on its UI HTML."""

    result = trace["result"]  # type: ignore[index]
    resource_spans = result["resourceSpans"]  # type: ignore[index]
    return [
        span
        for resource_span in resource_spans
        for scope_span in resource_span["scopeSpans"]
        for span in scope_span["spans"]
    ]


def _wait_for_trace(trace_id: str) -> list[dict[str, object]]:
    """Wait for the collector's observable query state, not an arbitrary delay."""

    required = {
        "transaction.accept",
        "transaction.publish",
        "transaction.consume",
        "transaction.conversion",
        "transaction.rate_lookup",
        "transaction.persistence",
    }
    deadline = time.monotonic() + 10
    last_status = 0
    last_names: set[str] = set()
    while time.monotonic() < deadline:
        response = httpx.get(f"{JAEGER_QUERY_URL}/api/v3/traces/{trace_id}", timeout=5)
        last_status = response.status_code
        if response.is_success:
            spans = _spans(response.json())
            last_names = {str(span["name"]) for span in spans}
            if required <= last_names:
                return spans
        time.sleep(0.05)
    raise AssertionError(
        f"timed out waiting for Jaeger trace {trace_id}; "
        f"status={last_status} observed_spans={sorted(last_names)}"
    )


def test_one_accepted_transaction_is_searchable_across_the_local_journey(
    environment: RecoveryEnvironment,
) -> None:
    """T034: Jaeger links API acceptance, broker handoff, conversion, rate, and commit."""

    transaction_id = f"trace-{uuid.uuid4().hex}"
    tracer = build_local_tracer(JAEGER_OTLP_URL, service_name="async-processing-trace-test")
    try:
        trace_id, row_count = asyncio.run(_run_journey(environment, tracer, transaction_id))
        assert row_count == 1
        assert_queues_empty(environment)
        tracer.flush()
        spans = _wait_for_trace(trace_id)
    finally:
        tracer.close()

    assert {span["name"] for span in spans} >= {
        "transaction.accept",
        "transaction.publish",
        "transaction.consume",
        "transaction.conversion",
        "transaction.rate_lookup",
        "transaction.persistence",
    }
    for span in spans:
        attributes = {
            item["key"]: next(iter(item["value"].values())) for item in span.get("attributes", [])
        }
        assert set(attributes) <= {"transaction_id", "stage", "attempt", "outcome"}
        assert attributes["transaction_id"] == transaction_id


def test_exporter_outage_does_not_change_confirmed_acceptance_or_acknowledgement(
    environment: RecoveryEnvironment,
) -> None:
    """Exporter delivery is observational and cannot turn a completed journey into a failure."""

    tracer = build_local_tracer("http://127.0.0.1:1/v1/traces", service_name="outage-test")
    try:
        _, row_count = asyncio.run(_run_journey(environment, tracer, f"outage-{uuid.uuid4().hex}"))
        assert row_count == 1
        assert_queues_empty(environment)
    finally:
        tracer.close()
