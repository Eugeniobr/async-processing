"""Validated value objects for persisted transaction read models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal


def _nonempty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be nonempty")


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")
    return value.astimezone(UTC)


@dataclass(frozen=True)
class UserSummary:
    """Exact aggregate facts derived only from a user's persisted records."""

    user_id: str
    total_usd: Decimal
    transaction_count: int

    def __post_init__(self) -> None:
        _nonempty(self.user_id, "user_id")
        if not isinstance(self.total_usd, Decimal) or not self.total_usd.is_finite():
            raise ValueError("total_usd must be a finite Decimal")
        if not isinstance(self.transaction_count, int) or self.transaction_count < 0:
            raise ValueError("transaction_count must be a nonnegative integer")


@dataclass(frozen=True)
class TransactionHistoryQuery:
    """One bounded UTC interval and offset page for a single user's history."""

    user_id: str
    from_timestamp: datetime
    to_timestamp: datetime
    limit: int = 50
    offset: int = 0

    def __post_init__(self) -> None:
        _nonempty(self.user_id, "user_id")
        from_timestamp = _utc(self.from_timestamp, "from_timestamp")
        to_timestamp = _utc(self.to_timestamp, "to_timestamp")
        if to_timestamp <= from_timestamp:
            raise ValueError("to_timestamp must be later than from_timestamp")
        if (
            not isinstance(self.limit, int)
            or isinstance(self.limit, bool)
            or not 1 <= self.limit <= 100
        ):
            raise ValueError("limit must be between 1 and 100")
        if not isinstance(self.offset, int) or isinstance(self.offset, bool) or self.offset < 0:
            raise ValueError("offset must be a nonnegative integer")
        object.__setattr__(self, "from_timestamp", from_timestamp)
        object.__setattr__(self, "to_timestamp", to_timestamp)
