from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from domain.queries import TransactionHistoryQuery, UserSummary


def test_history_query_normalizes_boundaries_and_applies_documented_defaults() -> None:
    query = TransactionHistoryQuery(
        user_id="user-1",
        from_timestamp=datetime(2026, 10, 20, 3, tzinfo=UTC),
        to_timestamp=datetime(2026, 10, 20, 4, tzinfo=UTC),
    )

    assert query.limit == 50
    assert query.offset == 0
    assert query.from_timestamp == datetime(2026, 10, 20, 3, tzinfo=UTC)


@pytest.mark.parametrize(
    "changes",
    [
        {"limit": 0},
        {"limit": 101},
        {"offset": -1},
        {"to_timestamp": datetime(2026, 10, 20, 3, tzinfo=UTC)},
        {"from_timestamp": datetime(2026, 10, 20, 3)},
    ],
)
def test_history_query_rejects_invalid_page_or_interval(changes: dict[str, object]) -> None:
    values: dict[str, object] = {
        "user_id": "user-1",
        "from_timestamp": datetime(2026, 10, 20, 3, tzinfo=UTC),
        "to_timestamp": datetime(2026, 10, 20, 4, tzinfo=UTC),
    }
    values.update(changes)

    with pytest.raises(ValueError):
        TransactionHistoryQuery(**values)  # type: ignore[arg-type]


def test_summary_preserves_a_decimal_zero_for_an_empty_result() -> None:
    summary = UserSummary("user-1", Decimal("0"), 0)

    assert summary.total_usd == Decimal("0")
    assert summary.transaction_count == 0
