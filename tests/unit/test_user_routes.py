from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from api.users import summary_response, transaction_response
from domain.queries import UserSummary
from domain.transaction import PersistedTransaction, TransactionEvent


def test_query_serializers_preserve_decimal_strings_and_normalize_timestamp_to_utc() -> None:
    event = TransactionEvent(
        transaction_id="transaction-1",
        user_id="user-1",
        original_amount=Decimal("12.34000000"),
        currency="EUR",
        event_timestamp=datetime(2026, 10, 22, 12, 30, tzinfo=UTC),
        currency_allowlist_version="currencies-v1",
    )
    transaction = PersistedTransaction(
        event=event,
        rate_provider="ecb",
        rate_requested_date=date(2026, 10, 22),
        rate_date=date(2026, 10, 21),
        applied_rate=Decimal("1.234567890123"),
        converted_usd_amount=Decimal("15.23456780"),
        created_at=datetime(2026, 10, 22, 13, tzinfo=UTC),
    )

    assert summary_response(UserSummary("user-1", Decimal("0"), 0)) == {
        "user_id": "user-1",
        "total_usd": "0",
        "transaction_count": 0,
    }
    assert transaction_response(transaction) == {
        "id": "transaction-1",
        "user_id": "user-1",
        "original_amount": "12.34000000",
        "currency": "EUR",
        "timestamp": "2026-10-22T12:30:00+00:00",
        "rate": "1.234567890123",
        "rate_requested_date": "2026-10-22",
        "rate_date": "2026-10-21",
        "converted_usd_amount": "15.23456780",
    }
