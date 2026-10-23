"""HTTP serialization for persisted user transaction read models."""

from __future__ import annotations

from datetime import UTC
from decimal import Decimal

from domain.queries import UserSummary
from domain.transaction import PersistedTransaction


def summary_response(summary: UserSummary) -> dict[str, str | int]:
    """Render the contract's exact Decimal-string summary representation."""

    return {
        "user_id": summary.user_id,
        "total_usd": _decimal_string(summary.total_usd),
        "transaction_count": summary.transaction_count,
    }


def transaction_response(transaction: PersistedTransaction) -> dict[str, str]:
    """Render persisted conversion facts without recalculation or float conversion."""

    event = transaction.event
    return {
        "id": event.transaction_id,
        "user_id": event.user_id,
        "original_amount": _decimal_string(event.original_amount),
        "currency": event.currency,
        "timestamp": event.event_timestamp.astimezone(UTC).isoformat(),
        "rate": _decimal_string(transaction.applied_rate),
        "rate_requested_date": transaction.rate_requested_date.isoformat(),
        "rate_date": transaction.rate_date.isoformat(),
        "converted_usd_amount": _decimal_string(transaction.converted_usd_amount),
    }


def _decimal_string(value: Decimal) -> str:
    return format(value, "f")
