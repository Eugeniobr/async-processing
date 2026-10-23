"""HTTP acceptance contracts to be implemented by T014 and T015."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry

from api.app import create_app
from composition.settings import CurrencyAllowlist
from domain.queries import TransactionHistoryQuery, UserSummary
from domain.transaction import PersistedTransaction
from infrastructure.metrics import MetricsRecorder
from infrastructure.rabbitmq_publisher import (
    PublicationIndeterminate,
    PublicationNacked,
    PublicationTimeout,
    UnroutablePublication,
)


class Publisher:
    """Confirmed-publish seam used to distinguish acceptance from processing."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.events: list[object] = []
        self.confirmed = False

    async def publish_work(self, event: object) -> object:
        self.events.append(event)
        if self.error is not None:
            raise self.error
        self.confirmed = True
        return object()


class Resources:
    """The future route receives this publisher from application composition."""

    def __init__(self, publisher: Publisher) -> None:
        self.metrics = MetricsRecorder(registry=CollectorRegistry())
        self.publisher = publisher
        self.currency_allowlist = CurrencyAllowlist(
            version="currencies-v1",
            provider="ecb",
            source_url="https://example.invalid/currencies",
            retrieved_on=date(2026, 10, 22),
            codes=frozenset({"EUR", "USD"}),
        )
        self.repository = EmptyRepository()

    def close(self) -> None:
        pass


class EmptyRepository:
    def get_summary(self, user_id: str) -> UserSummary:
        return UserSummary(user_id=user_id, total_usd=Decimal("0"), transaction_count=0)

    def list_history(self, _query: TransactionHistoryQuery) -> list[PersistedTransaction]:
        return []


def _client(publisher: Publisher) -> TestClient:
    return TestClient(create_app(Resources(publisher)))


def _valid_payload() -> dict[str, str]:
    return {
        "id": "accepted-transaction",
        "user_id": "accepted-user",
        "amount": "12.34000000",
        "currency": "EUR",
        "timestamp": "2026-10-22T07:30:00-03:00",
    }


def test_submission_returns_accepted_only_after_confirmed_publish() -> None:
    publisher = Publisher()

    with _client(publisher) as client:
        response = client.post("/transactions", json=_valid_payload())

    assert response.status_code == 202
    assert response.json() == {"id": "accepted-transaction", "status": "accepted"}
    assert publisher.confirmed is True
    assert len(publisher.events) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", ""),
        ("user_id", ""),
        ("amount", "12.340000001"),
        ("amount", "NaN"),
        ("currency", "eur"),
        ("currency", "ZZZ"),
        ("timestamp", "2026-10-22T10:30:00"),
    ],
)
def test_invalid_submission_is_rejected_before_publication(field: str, value: str) -> None:
    publisher = Publisher()
    payload = _valid_payload()
    payload[field] = value

    with _client(publisher) as client:
        response = client.post("/transactions", json=payload)

    assert response.status_code == 422
    assert publisher.events == []


@pytest.mark.parametrize(
    "error",
    [
        PublicationNacked("nacked"),
        PublicationTimeout("timed out"),
        PublicationIndeterminate("connection lost"),
        UnroutablePublication("returned"),
    ],
)
def test_submission_without_positive_confirm_or_routing_returns_temporary_unavailability(
    error: Exception,
) -> None:
    publisher = Publisher(error)

    with _client(publisher) as client:
        response = client.post("/transactions", json=_valid_payload())

    assert response.status_code == 503
    assert response.json() == {"detail": str(error)}
    assert publisher.confirmed is False
    assert len(publisher.events) == 1
