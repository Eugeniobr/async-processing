"""HTTP contracts for completed-transaction summaries and history queries."""

from __future__ import annotations

from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry

from api.app import create_app
from domain.queries import TransactionHistoryQuery, UserSummary
from domain.transaction import PersistedTransaction
from infrastructure.metrics import MetricsRecorder


class Resources:
    """Minimal HTTP resources; query behavior is intentionally implemented in T025."""

    def __init__(self) -> None:
        self.metrics = MetricsRecorder(registry=CollectorRegistry())
        self.repository = EmptyRepository()

    def close(self) -> None:
        pass


class EmptyRepository:
    def get_summary(self, user_id: str) -> UserSummary:
        return UserSummary(user_id, total_usd=Decimal("0"), transaction_count=0)

    def list_history(self, _query: TransactionHistoryQuery) -> list[PersistedTransaction]:
        return []


@pytest.fixture()
def client() -> TestClient:
    with TestClient(create_app(Resources())) as value:
        yield value


def test_summary_returns_zero_total_and_count_for_a_user_without_transactions(
    client: TestClient,
) -> None:
    response = client.get("/users/user-without-records/summary")

    assert response.status_code == 200
    assert response.json() == {
        "user_id": "user-without-records",
        "total_usd": "0",
        "transaction_count": 0,
    }


def test_history_uses_default_page_and_returns_the_contract_shape(client: TestClient) -> None:
    response = client.get(
        "/users/user-without-records/transactions",
        params={"from": "2026-10-20T00:00:00Z", "to": "2026-10-21T00:00:00Z"},
    )

    assert response.status_code == 200
    assert response.json() == {"items": [], "limit": 50, "offset": 0}


def test_history_contract_requires_user_isolation_boundaries_and_stable_order(
    client: TestClient,
) -> None:
    """T025/T026 must make this request expose only matching persisted rows.

    The response contract is deliberately explicit: it must omit another user's
    records and exclude the ``to`` boundary while sorting equal timestamps by id.
    Fixture data belongs to the repository/integration tasks that implement it.
    """

    response = client.get(
        "/users/user-a/transactions",
        params={
            "from": "2026-10-20T00:00:00Z",
            "to": "2026-10-21T00:00:00Z",
            "limit": 100,
            "offset": 0,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["limit"] == 100
    assert payload["offset"] == 0
    assert all(item["user_id"] == "user-a" for item in payload["items"])
    assert all(
        "2026-10-20T00:00:00+00:00" <= item["timestamp"] < "2026-10-21T00:00:00+00:00"
        for item in payload["items"]
    )
    assert [(item["timestamp"], item["id"]) for item in payload["items"]] == sorted(
        (item["timestamp"], item["id"]) for item in payload["items"]
    )


def test_history_contract_returns_requested_page_metadata(client: TestClient) -> None:
    response = client.get(
        "/users/user-a/transactions",
        params={
            "from": "2026-10-20T00:00:00Z",
            "to": "2026-10-21T00:00:00Z",
            "limit": 1,
            "offset": 1,
        },
    )

    assert response.status_code == 200
    assert response.json()["limit"] == 1
    assert response.json()["offset"] == 1


@pytest.mark.parametrize(
    "path",
    [
        "/users/user-a/transactions",
        "/users/user-a/transactions?from=2026-10-20T00:00:00Z",
        "/users/user-a/transactions?to=2026-10-21T00:00:00Z",
        "/users/user-a/transactions?from=not-a-time&to=2026-10-21T00:00:00Z",
        "/users/user-a/transactions?from=2026-10-21T00:00:00Z&to=2026-10-20T00:00:00Z",
        "/users/user-a/transactions?from=2026-10-20T00:00:00Z&to=2026-10-21T00:00:00Z&limit=0",
        "/users/user-a/transactions?from=2026-10-20T00:00:00Z&to=2026-10-21T00:00:00Z&limit=101",
        "/users/user-a/transactions?from=2026-10-20T00:00:00Z&to=2026-10-21T00:00:00Z&offset=-1",
    ],
)
def test_history_rejects_invalid_query_parameters(client: TestClient, path: str) -> None:
    assert client.get(path).status_code == 422
