from __future__ import annotations

from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry

from api.app import create_app
from api.errors import register_error_handlers
from application.ports import RetryableRateError
from composition.container import build_container
from composition.settings import Settings
from domain.queries import TransactionHistoryQuery, UserSummary
from domain.transaction import PersistedTransaction
from infrastructure.metrics import MetricsRecorder


class FakeContainer:
    def __init__(self, metrics: MetricsRecorder) -> None:
        self.metrics = metrics
        self.repository = EmptyRepository()
        self.closed = False

    def close(self) -> None:
        self.closed = True


class EmptyRepository:
    def get_summary(self, user_id: str) -> UserSummary:
        return UserSummary(user_id, total_usd=Decimal("0"), transaction_count=0)

    def list_history(self, _query: TransactionHistoryQuery) -> list[PersistedTransaction]:
        return []


def test_metrics_endpoint_exposes_only_classified_processing_failures() -> None:
    metrics = MetricsRecorder(registry=CollectorRegistry())
    metrics.record_processing_failure("retryable")
    metrics.record_processing_failure("permanent")
    container = FakeContainer(metrics)

    with TestClient(create_app(container)) as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert 'transaction_processing_failures_total{kind="retryable"} 1.0' in response.text
    assert 'transaction_processing_failures_total{kind="permanent"} 1.0' in response.text
    assert 'transaction_processing_failures_total{kind="successful"}' not in response.text
    assert container.closed is True


def test_error_handlers_translate_validation_and_temporary_failures() -> None:
    app = FastAPI()
    register_error_handlers(app)

    @app.get("/validation-error")
    def validation_error() -> None:
        raise ValueError("invalid transaction")

    @app.get("/temporary-error")
    def temporary_error() -> None:
        raise RetryableRateError("rate provider unavailable")

    with TestClient(app) as client:
        validation = client.get("/validation-error")
        temporary = client.get("/temporary-error")

    assert validation.status_code == 422
    assert validation.json() == {"detail": "invalid transaction"}
    assert temporary.status_code == 503
    assert temporary.json() == {"detail": "rate provider unavailable"}


def test_metric_rejects_unknown_failure_kind() -> None:
    metrics = MetricsRecorder(registry=CollectorRegistry())

    try:
        metrics.record_processing_failure("published")
    except ValueError as error:
        assert str(error) == "kind must be retryable or permanent"
    else:
        raise AssertionError("unknown metric label must be rejected")


def test_container_owns_and_closes_http_and_database_clients() -> None:
    container = build_container(
        Settings(
            database_url="postgresql+psycopg://async_processing:async_processing@localhost:5433/async_processing",
            rabbitmq_url="amqp://async_processing:async_processing@localhost:5672/",
        )
    )

    container.close()

    assert container.rate_client.is_closed is True
