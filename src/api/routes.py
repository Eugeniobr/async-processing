"""Small HTTP route registration around constructor-injected query reads."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Protocol

from fastapi import Body, FastAPI, Path, Query
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST

from api.transactions import parse_transaction_input
from api.users import summary_response, transaction_response
from composition.settings import CurrencyAllowlist
from domain.queries import TransactionHistoryQuery, UserSummary
from domain.transaction import PersistedTransaction
from infrastructure.tracing import NoopTransactionTracer, TransactionTracer


class Metrics(Protocol):
    """The Prometheus exposition surface supplied by application composition."""

    def render(self) -> bytes: ...


class UserQueryRepository(Protocol):
    """The read-only repository surface needed by the two user query routes."""

    def get_summary(self, user_id: str) -> UserSummary: ...

    def list_history(self, query: TransactionHistoryQuery) -> list[PersistedTransaction]: ...


class TransactionPublisher(Protocol):
    """The confirmed-publish surface required by the acceptance route."""

    async def publish_work(self, event: object) -> object: ...


def register_metrics_route(app: FastAPI, metrics: Metrics) -> None:
    """Expose the process-aware Prometheus registry supplied by the caller."""

    @app.get("/metrics", include_in_schema=False)
    def metrics_endpoint() -> Response:
        return Response(metrics.render(), media_type=CONTENT_TYPE_LATEST)


def register_user_routes(app: FastAPI, repository: UserQueryRepository) -> None:
    """Register query handlers that delegate all filtering and ordering to the repository."""

    @app.get("/users/{user_id}/summary")
    def get_user_summary(user_id: Annotated[str, Path(min_length=1)]) -> dict[str, str | int]:
        return summary_response(repository.get_summary(user_id))

    @app.get("/users/{user_id}/transactions")
    def list_user_transactions(
        user_id: Annotated[str, Path(min_length=1)],
        from_timestamp: Annotated[datetime, Query(alias="from")],
        to_timestamp: Annotated[datetime, Query(alias="to")],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict[str, object]:
        query = TransactionHistoryQuery(
            user_id=user_id,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            limit=limit,
            offset=offset,
        )
        return {
            "items": [transaction_response(item) for item in repository.list_history(query)],
            "limit": query.limit,
            "offset": query.offset,
        }


def register_transaction_routes(
    app: FastAPI,
    publisher: TransactionPublisher,
    allowlist: CurrencyAllowlist,
    *,
    tracer: TransactionTracer | None = None,
) -> None:
    """Register durable acceptance without treating it as completed processing."""

    @app.post("/transactions", status_code=202)
    async def submit_transaction(payload: object = Body(...)) -> dict[str, str]:
        active_tracer = tracer or NoopTransactionTracer()
        candidate_id = payload.get("id") if isinstance(payload, Mapping) else None
        transaction_id = candidate_id if isinstance(candidate_id, str) else None
        with active_tracer.span(
            "transaction.accept", stage="api_acceptance", transaction_id=transaction_id
        ) as span:
            event = parse_transaction_input(payload, allowlist, tracer=active_tracer)
            await publisher.publish_work(event)
            span.set_outcome("accepted")
            return {"id": event.transaction_id, "status": "accepted"}
