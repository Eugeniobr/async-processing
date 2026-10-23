"""Contracts for processing-failure metrics, before T028 instruments the consumer."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient
from pamqp.commands import Basic
from prometheus_client import CollectorRegistry

from api.app import create_app
from domain.outcomes import Duplicate, PermanentFailure, Processed, RetryableFailure
from domain.queries import TransactionHistoryQuery, UserSummary
from domain.transaction import PersistedTransaction, TransactionEvent
from infrastructure.metrics import MetricsRecorder
from infrastructure.rabbitmq_consumer import TransactionConsumer
from infrastructure.rabbitmq_publisher import PublicationIndeterminate, TransactionPublisher


class Delivery:
    """Manual delivery double that exposes acknowledgement ordering to the contract."""

    def __init__(self) -> None:
        self.body = _body()
        self.ack_count = 0

    async def ack(self) -> None:
        self.ack_count += 1


class Processor:
    def __init__(self, outcome: object) -> None:
        self._outcome = outcome

    def process(self, _event: object) -> object:
        return self._outcome


class Publisher:
    def __init__(self, *, forwarding_error: Exception | None = None) -> None:
        self._forwarding_error = forwarding_error
        self.retry_calls = 0
        self.failure_calls = 0

    async def publish_retry(self, _envelope: object, _delay_seconds: int) -> None:
        self.retry_calls += 1
        if self._forwarding_error:
            raise self._forwarding_error

    async def publish_failure(self, _envelope: object, _reason: str) -> None:
        self.failure_calls += 1
        if self._forwarding_error:
            raise self._forwarding_error

    async def publish_invalid_envelope(self, _body: bytes, _reason: str) -> None:
        raise AssertionError("invalid-envelope forwarding is outside this metric contract")


class ConfirmingExchange:
    """The smallest confirmed-publish boundary needed to distinguish acceptance."""

    async def publish(self, *_args: object, **_kwargs: object) -> Basic.Ack:
        return Basic.Ack(delivery_tag=1, multiple=False)


class Resources:
    """Expose the supplied metrics registry through the actual FastAPI application."""

    def __init__(self, metrics: MetricsRecorder) -> None:
        self.metrics = metrics
        self.repository = EmptyRepository()

    def close(self) -> None:
        pass


class EmptyRepository:
    def get_summary(self, user_id: str) -> UserSummary:
        return UserSummary(user_id=user_id, total_usd=Decimal("0"), transaction_count=0)

    def list_history(self, _query: TransactionHistoryQuery) -> list[PersistedTransaction]:
        return []


def _body() -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "event": {
                "transaction_id": "metric-transaction",
                "user_id": "metric-user",
                "original_amount": "1.00000000",
                "currency": "EUR",
                "event_timestamp": "2026-10-22T10:30:00+00:00",
                "currency_allowlist_version": "currencies-v1",
            },
            "attempt": 0,
            "accepted_at": "2026-10-22T10:31:00+00:00",
            "currency_allowlist_version": "currencies-v1",
            "last_failure_kind": None,
        }
    ).encode()


def _metric_lines(metrics: MetricsRecorder) -> list[str]:
    return [
        line
        for line in metrics.render().decode().splitlines()
        if line.startswith("transaction_processing_failures_total{")
    ]


def _event() -> TransactionEvent:
    return TransactionEvent.from_accepted_input(
        supported_currencies=frozenset({"EUR"}),
        currency_allowlist_version="currencies-v1",
        transaction_id="metric-transaction",
        user_id="metric-user",
        original_amount=Decimal("1.00000000"),
        currency="EUR",
        event_timestamp=datetime(2026, 10, 22, 10, 30, tzinfo=UTC),
    )


def _consumer(
    metrics: MetricsRecorder, outcome: object, publisher: Publisher
) -> TransactionConsumer:
    """The T028 contract requires explicit constructor injection of the recorder."""

    return TransactionConsumer(Processor(outcome), publisher, metrics=metrics)


def test_confirmed_retry_and_permanent_forwarding_are_exposed_with_only_kind_labels() -> None:
    metrics = MetricsRecorder(registry=CollectorRegistry())
    retry_delivery = Delivery()
    permanent_delivery = Delivery()

    asyncio.run(
        _consumer(metrics, RetryableFailure("rate provider unavailable"), Publisher()).handle(
            retry_delivery
        )
    )
    asyncio.run(
        _consumer(metrics, PermanentFailure("no eligible rate"), Publisher()).handle(
            permanent_delivery
        )
    )

    assert retry_delivery.ack_count == 1
    assert permanent_delivery.ack_count == 1
    assert _metric_lines(metrics) == [
        'transaction_processing_failures_total{kind="retryable"} 1.0',
        'transaction_processing_failures_total{kind="permanent"} 1.0',
    ]
    exposition = metrics.render().decode()
    assert "transaction_id" not in exposition
    assert "user_id" not in exposition
    assert "metric-transaction" not in exposition
    assert "metric-user" not in exposition


def test_confirmed_acceptance_does_not_increment_a_processing_failure_metric() -> None:
    metrics = MetricsRecorder(registry=CollectorRegistry())
    exchange = ConfirmingExchange()
    publisher = TransactionPublisher(exchange, exchange, confirm_timeout_seconds=1)

    asyncio.run(publisher.publish_work(_event()))

    assert _metric_lines(metrics) == []


def test_successful_and_duplicate_processing_do_not_increment_processing_failure_metrics() -> None:
    metrics = MetricsRecorder(registry=CollectorRegistry())

    for outcome in (Processed("metric-transaction"), Duplicate("metric-transaction")):
        delivery = Delivery()
        asyncio.run(_consumer(metrics, outcome, Publisher()).handle(delivery))
        assert delivery.ack_count == 1

    assert _metric_lines(metrics) == []


def test_unconfirmed_retry_forwarding_is_not_counted_as_a_processing_failure() -> None:
    metrics = MetricsRecorder(registry=CollectorRegistry())
    delivery = Delivery()

    asyncio.run(
        _consumer(
            metrics,
            RetryableFailure("persistence unavailable"),
            Publisher(forwarding_error=PublicationIndeterminate("connection lost")),
        ).handle(delivery)
    )

    assert delivery.ack_count == 0
    assert _metric_lines(metrics) == []


def test_metrics_endpoint_uses_prometheus_text_for_confirmed_processing_failures() -> None:
    metrics = MetricsRecorder(registry=CollectorRegistry())
    delivery = Delivery()
    asyncio.run(
        _consumer(metrics, PermanentFailure("unsupported currency"), Publisher()).handle(delivery)
    )

    with TestClient(create_app(Resources(metrics))) as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert 'transaction_processing_failures_total{kind="permanent"} 1.0' in response.text
