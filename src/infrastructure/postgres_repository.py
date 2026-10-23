"""PostgreSQL implementation of the first-write-wins repository port."""

from __future__ import annotations

from decimal import Decimal

from sqlalchemy import Engine, func, select
from sqlalchemy.dialects.postgresql import insert

from domain.outcomes import AlreadyExists, Inserted
from domain.queries import TransactionHistoryQuery, UserSummary
from domain.transaction import PersistedTransaction
from infrastructure.postgres_schema import transactions_table
from infrastructure.tracing import NoopTransactionTracer, TransactionTracer


class PostgresTransactionRepository:
    """Persist one conversion record with the database as the concurrency boundary."""

    def __init__(self, engine: Engine, *, tracer: TransactionTracer | None = None) -> None:
        self._engine = engine
        self._tracer = tracer or NoopTransactionTracer()

    def record_first(self, transaction: PersistedTransaction) -> Inserted | AlreadyExists:
        """Commit an insert or report an existing id without a preceding read."""

        statement = (
            insert(transactions_table)
            .values(
                transaction_id=transaction.event.transaction_id,
                user_id=transaction.event.user_id,
                original_amount=transaction.event.original_amount,
                currency=transaction.event.currency,
                currency_allowlist_version=transaction.event.currency_allowlist_version,
                event_timestamp=transaction.event.event_timestamp,
                rate_provider=transaction.rate_provider,
                rate_requested_date=transaction.rate_requested_date,
                rate_date=transaction.rate_date,
                applied_rate=transaction.applied_rate,
                converted_usd_amount=transaction.converted_usd_amount,
                created_at=transaction.created_at,
            )
            .on_conflict_do_nothing(index_elements=[transactions_table.c.transaction_id])
            .returning(transactions_table.c.transaction_id)
        )
        with self._tracer.span(
            "transaction.persistence",
            stage="persistence",
            transaction_id=transaction.event.transaction_id,
        ) as span:
            with self._engine.begin() as connection:
                inserted_id = connection.execute(statement).scalar_one_or_none()
            if inserted_id is None:
                span.set_outcome("duplicate")
                return AlreadyExists(transaction.event.transaction_id)
            span.set_outcome("persisted")
            return Inserted(inserted_id)

    def get_summary(self, user_id: str) -> UserSummary:
        """Return an exact Decimal aggregate for this user, including the empty result."""

        if not isinstance(user_id, str) or not user_id.strip():
            raise ValueError("user_id must be nonempty")
        statement = select(
            func.coalesce(func.sum(transactions_table.c.converted_usd_amount), Decimal("0")),
            func.count(),
        ).where(transactions_table.c.user_id == user_id)
        with self._engine.connect() as connection:
            total, count = connection.execute(statement).one()
        return UserSummary(user_id=user_id, total_usd=Decimal(total), transaction_count=count)

    def list_history(self, query: TransactionHistoryQuery) -> list[PersistedTransaction]:
        """Read one user's stable `(event_timestamp, transaction_id)` page."""

        statement = (
            select(transactions_table)
            .where(
                transactions_table.c.user_id == query.user_id,
                transactions_table.c.event_timestamp >= query.from_timestamp,
                transactions_table.c.event_timestamp < query.to_timestamp,
            )
            .order_by(
                transactions_table.c.event_timestamp.asc(),
                transactions_table.c.transaction_id.asc(),
            )
            .limit(query.limit)
            .offset(query.offset)
        )
        with self._engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()
        return [self._persisted_transaction(row) for row in rows]

    @staticmethod
    def _persisted_transaction(row: object) -> PersistedTransaction:
        """Rehydrate existing domain facts without recomputing money or dates."""

        values = dict(row)  # type: ignore[arg-type]
        from domain.transaction import TransactionEvent

        event = TransactionEvent(
            transaction_id=values["transaction_id"],
            user_id=values["user_id"],
            original_amount=values["original_amount"],
            currency=values["currency"],
            event_timestamp=values["event_timestamp"],
            currency_allowlist_version=values["currency_allowlist_version"],
        )
        return PersistedTransaction(
            event=event,
            rate_provider=values["rate_provider"],
            rate_requested_date=values["rate_requested_date"],
            rate_date=values["rate_date"],
            applied_rate=values["applied_rate"],
            converted_usd_amount=values["converted_usd_amount"],
            created_at=values["created_at"],
        )
