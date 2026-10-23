"""Narrow application ports for persistence; adapters own their I/O details."""

from __future__ import annotations

from datetime import date
from typing import Protocol

from domain.outcomes import AlreadyExists, Inserted
from domain.queries import TransactionHistoryQuery, UserSummary
from domain.rates import RateQuote
from domain.transaction import PersistedTransaction


class TransactionRepository(Protocol):
    """Save exactly the first record for an id; database failures propagate."""

    def record_first(self, transaction: PersistedTransaction) -> Inserted | AlreadyExists: ...

    def get_summary(self, user_id: str) -> UserSummary: ...

    def list_history(self, query: TransactionHistoryQuery) -> list[PersistedTransaction]: ...


class ExchangeRateProvider(Protocol):
    """Return a validated USD-per-unit quote or raise an explicit rate error."""

    def get_usd_rate(self, currency: str, requested_date: date) -> RateQuote: ...


class RetryableRateError(Exception):
    """A rate service failure that a later consumer may retry."""


class PermanentRateError(Exception):
    """A rate request or response that cannot be completed by retrying unchanged input."""
