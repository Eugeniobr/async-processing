"""Immutable exchange-rate value objects."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

RATE_SCALE = 12
RATE_QUANTUM = Decimal("0.000000000001")
MAX_RATE = Decimal("999999999999.999999999999")


def _validate_currency(value: str, field_name: str) -> None:
    if not isinstance(value, str) or len(value) != 3 or not value.isupper():
        raise ValueError(f"{field_name} must be an uppercase three-letter code")


@dataclass(frozen=True)
class RateQuote:
    """A positive USD-per-unit quote with its requested and returned dates."""

    base_currency: str
    quote_currency: str
    rate: Decimal
    requested_date: date
    rate_date: date
    provider: str
    fetched_at: datetime

    def __post_init__(self) -> None:
        _validate_currency(self.base_currency, "base_currency")
        if self.quote_currency != "USD":
            raise ValueError("quote_currency must be USD")
        if not isinstance(self.rate, Decimal) or not self.rate.is_finite() or self.rate <= 0:
            raise ValueError("rate must be positive finite")
        if not isinstance(self.requested_date, date) or not isinstance(self.rate_date, date):
            raise ValueError("requested_date and rate_date must be dates")
        if self.rate_date > self.requested_date:
            raise ValueError("rate_date must not be later than requested_date")
        if self.rate_date < self.requested_date - timedelta(days=7):
            raise ValueError("rate_date must be within seven fallback days")
        if not isinstance(self.provider, str) or not self.provider.strip():
            raise ValueError("provider must be nonempty")
        if not isinstance(self.fetched_at, datetime) or self.fetched_at.tzinfo is None:
            raise ValueError("fetched_at must include a timezone")
        object.__setattr__(self, "fetched_at", self.fetched_at.astimezone(UTC))
