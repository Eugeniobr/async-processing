from __future__ import annotations

import importlib.util
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError, ProgrammingError

from domain.outcomes import AlreadyExists, Inserted
from domain.queries import TransactionHistoryQuery
from domain.transaction import PersistedTransaction, TransactionEvent
from infrastructure.postgres_repository import PostgresTransactionRepository
from infrastructure.postgres_schema import transactions_table

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://async_processing:async_processing@localhost:5433/async_processing",
)


def _migration_module():
    migration_path = Path("migrations/versions/001_transactions.py")
    spec = importlib.util.spec_from_file_location("migration_001", migration_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def engine() -> Engine:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        with engine.begin() as connection:
            connection.execute(text("DROP SCHEMA IF EXISTS transactions CASCADE"))
            _migration_module().upgrade(connection)
    except Exception as error:
        engine.dispose()
        pytest.fail(f"Compose PostgreSQL is required for this integration test: {error}")
    yield engine
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS transactions CASCADE"))
    engine.dispose()


def persisted_transaction(
    transaction_id: str = "transaction-1",
    *,
    user_id: str = "user-1",
    amount: str = "12.34567890",
    event_timestamp: datetime = datetime(2026, 10, 22, 10, 30, tzinfo=UTC),
    converted_amount: str = "15.24157875",
) -> PersistedTransaction:
    event = TransactionEvent.from_accepted_input(
        supported_currencies=frozenset({"USD", "EUR"}),
        currency_allowlist_version="currencies-v1",
        transaction_id=transaction_id,
        user_id=user_id,
        original_amount=Decimal(amount),
        currency="EUR",
        event_timestamp=event_timestamp,
    )
    return PersistedTransaction(
        event=event,
        rate_provider="ecb",
        rate_requested_date=date(2026, 10, 22),
        rate_date=date(2026, 10, 21),
        applied_rate=Decimal("1.234567890123"),
        converted_usd_amount=Decimal(converted_amount),
        created_at=datetime(2026, 10, 22, 11, 0, tzinfo=UTC),
    )


def test_record_first_persists_exact_decimal_values(engine: Engine) -> None:
    repository = PostgresTransactionRepository(engine)
    record = persisted_transaction()

    assert repository.record_first(record) == Inserted("transaction-1")
    with engine.connect() as connection:
        row = connection.execute(select(transactions_table)).mappings().one()

    assert row["original_amount"] == Decimal("12.34567890")
    assert row["applied_rate"] == Decimal("1.234567890123")
    assert row["converted_usd_amount"] == Decimal("15.24157875")
    assert row["event_timestamp"] == datetime(2026, 10, 22, 10, 30, tzinfo=UTC)


def test_sequential_and_conflicting_duplicates_preserve_the_first_record(engine: Engine) -> None:
    repository = PostgresTransactionRepository(engine)
    first = persisted_transaction(amount="12.34567890")
    conflicting = persisted_transaction(user_id="other-user", amount="99.00000000")

    assert repository.record_first(first) == Inserted("transaction-1")
    assert repository.record_first(conflicting) == AlreadyExists("transaction-1")
    with engine.connect() as connection:
        row = connection.execute(select(transactions_table)).mappings().one()

    assert row["user_id"] == "user-1"
    assert row["original_amount"] == Decimal("12.34567890")


def test_has_transaction_id_only_observes_committed_records(engine: Engine) -> None:
    repository = PostgresTransactionRepository(engine)

    assert repository.has_transaction_id("transaction-1") is False
    assert repository.record_first(persisted_transaction()) == Inserted("transaction-1")
    assert repository.has_transaction_id("transaction-1") is True


def test_concurrent_duplicate_inserts_use_the_database_unique_constraint(engine: Engine) -> None:
    repository = PostgresTransactionRepository(engine)
    record = persisted_transaction()

    with ThreadPoolExecutor(max_workers=8) as executor:
        outcomes = list(executor.map(lambda _: repository.record_first(record), range(8)))

    assert sum(isinstance(outcome, Inserted) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, AlreadyExists) for outcome in outcomes) == 7
    with engine.connect() as connection:
        assert connection.execute(select(transactions_table.c.transaction_id)).all() == [
            ("transaction-1",)
        ]


def test_failed_database_write_rolls_back_and_is_not_reported_as_a_duplicate(
    engine: Engine,
) -> None:
    repository = PostgresTransactionRepository(engine)
    invalid = persisted_transaction()
    object.__setattr__(invalid.event, "user_id", None)

    with pytest.raises(IntegrityError):
        repository.record_first(invalid)
    assert repository.record_first(persisted_transaction()) == Inserted("transaction-1")


def test_missing_table_failure_propagates_to_the_caller(engine: Engine) -> None:
    repository = PostgresTransactionRepository(engine)
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE transactions.transactions"))

    with pytest.raises(ProgrammingError):
        repository.record_first(persisted_transaction())


def test_summary_and_history_are_user_isolated_and_exact(engine: Engine) -> None:
    repository = PostgresTransactionRepository(engine)
    start = datetime(2026, 10, 22, 10, tzinfo=UTC)
    records = [
        persisted_transaction("user-a-a", event_timestamp=start, converted_amount="1.10000000"),
        persisted_transaction("user-a-b", event_timestamp=start, converted_amount="2.20000000"),
        persisted_transaction(
            "user-a-end", event_timestamp=start.replace(hour=11), converted_amount="9.90000000"
        ),
        persisted_transaction(
            "user-b-a", user_id="user-b", event_timestamp=start, converted_amount="8.80000000"
        ),
    ]
    for record in records:
        assert isinstance(repository.record_first(record), Inserted)

    summary = repository.get_summary("missing")
    assert summary.total_usd == Decimal("0")
    assert summary.transaction_count == 0
    assert repository.get_summary("user-1").total_usd == Decimal("13.20000000")
    assert repository.get_summary("user-1").transaction_count == 3

    query = TransactionHistoryQuery("user-1", start, start.replace(hour=11), limit=100, offset=0)
    history = repository.list_history(query)
    assert [record.event.transaction_id for record in history] == ["user-a-a", "user-a-b"]
    assert all(record.event.user_id == "user-1" for record in history)


def test_history_pagination_has_no_overlap_for_a_static_result_set(engine: Engine) -> None:
    repository = PostgresTransactionRepository(engine)
    start = datetime(2026, 10, 22, 10, tzinfo=UTC)
    for transaction_id in ("id-c", "id-a", "id-b"):
        assert isinstance(
            repository.record_first(persisted_transaction(transaction_id, event_timestamp=start)),
            Inserted,
        )

    first_page = repository.list_history(
        TransactionHistoryQuery("user-1", start, start + timedelta(hours=1), limit=2)
    )
    second_page = repository.list_history(
        TransactionHistoryQuery("user-1", start, start + timedelta(hours=1), limit=2, offset=2)
    )
    assert [record.event.transaction_id for record in first_page] == ["id-a", "id-b"]
    assert [record.event.transaction_id for record in second_page] == ["id-c"]
