from __future__ import annotations

from datetime import date
from typing import cast

from application.ports import ExchangeRateProvider, TransactionRepository
from domain.outcomes import Inserted
from domain.rates import RateQuote
from domain.transaction import PersistedTransaction


class FakeRepository:
    def has_transaction_id(self, transaction_id: str) -> bool:
        return False

    def record_first(self, transaction: PersistedTransaction) -> Inserted:
        return Inserted(transaction.event.transaction_id)


class FakeRateProvider:
    def get_usd_rate(self, currency: str, requested_date: date) -> RateQuote:
        raise NotImplementedError


def test_fake_implementation_satisfies_the_repository_port() -> None:
    repository = cast(TransactionRepository, FakeRepository())
    rate_provider = cast(ExchangeRateProvider, FakeRateProvider())

    assert repository
    assert rate_provider
