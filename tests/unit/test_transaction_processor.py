from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from application.money_converter import MoneyConverter
from application.ports import PermanentRateError, RetryableRateError
from application.transaction_processor import TransactionProcessor
from domain.outcomes import (
    AlreadyExists,
    Duplicate,
    Inserted,
    PermanentFailure,
    Processed,
    RetryableFailure,
)
from domain.rates import RateQuote
from domain.transaction import PersistedTransaction, TransactionEvent


class FakeRateProvider:
    def __init__(self, quote: RateQuote | Exception, calls: list[str]) -> None:
        self._quote = quote
        self._calls = calls

    def get_usd_rate(self, currency: str, requested_date: date) -> RateQuote:
        self._calls.append(f"provider:{currency}:{requested_date.isoformat()}")
        if isinstance(self._quote, Exception):
            raise self._quote
        return self._quote


class FakeRepository:
    def __init__(self, result: Inserted | AlreadyExists | Exception, calls: list[str]) -> None:
        self._result = result
        self._calls = calls
        self.recorded: list[PersistedTransaction] = []

    def record_first(self, transaction: PersistedTransaction) -> Inserted | AlreadyExists:
        self._calls.append("repository")
        self.recorded.append(transaction)
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


def event(*, currency: str = "EUR", amount: Decimal = Decimal("10.00000000")) -> TransactionEvent:
    return TransactionEvent(
        transaction_id="tx-processor-1",
        user_id="user-1",
        original_amount=amount,
        currency=currency,
        event_timestamp=datetime(2026, 10, 20, 22, 30, tzinfo=UTC),
        currency_allowlist_version="currencies-v1",
    )


def quote(
    *,
    base_currency: str = "EUR",
    rate: Decimal = Decimal("1.234567890123"),
    rate_date: date = date(2026, 10, 20),
) -> RateQuote:
    return RateQuote(
        base_currency=base_currency,
        quote_currency="USD",
        rate=rate,
        requested_date=date(2026, 10, 20),
        rate_date=rate_date,
        provider="ecb",
        fetched_at=datetime(2026, 10, 22, tzinfo=UTC),
    )


def processor(
    rate_quote: RateQuote | Exception,
    repository_result: Inserted | AlreadyExists | Exception,
    calls: list[str],
) -> tuple[TransactionProcessor, FakeRepository]:
    repository = FakeRepository(repository_result, calls)
    return (
        TransactionProcessor(
            FakeRateProvider(rate_quote, calls),
            MoneyConverter(),
            repository,
            now=lambda: datetime(2026, 10, 22, 1, 2, 3, tzinfo=UTC),
        ),
        repository,
    )


def test_processes_and_persists_conversion_after_rate_lookup() -> None:
    calls: list[str] = []
    subject, repository = processor(quote(), Inserted("tx-processor-1"), calls)

    outcome = subject.process(event())

    assert outcome == Processed("tx-processor-1")
    assert calls == ["provider:EUR:2026-10-20", "repository"]
    assert repository.recorded == [
        PersistedTransaction(
            event=event(),
            rate_provider="ecb",
            rate_requested_date=date(2026, 10, 20),
            rate_date=date(2026, 10, 20),
            applied_rate=Decimal("1.234567890123"),
            converted_usd_amount=Decimal("12.34567890"),
            created_at=datetime(2026, 10, 22, 1, 2, 3, tzinfo=UTC),
        )
    ]


def test_reports_duplicate_after_repository_confirms_first_record_exists() -> None:
    calls: list[str] = []
    subject, repository = processor(quote(), AlreadyExists("tx-processor-1"), calls)

    outcome = subject.process(event())

    assert outcome == Duplicate("tx-processor-1")
    assert len(repository.recorded) == 1
    assert calls[-1] == "repository"


def test_uses_usd_identity_quote_and_persists_original_amount() -> None:
    calls: list[str] = []
    subject, repository = processor(
        quote(base_currency="USD", rate=Decimal("1")), Inserted("tx-processor-1"), calls
    )

    outcome = subject.process(event(currency="USD", amount=Decimal("-2.50000000")))

    assert outcome == Processed("tx-processor-1")
    assert repository.recorded[0].applied_rate == Decimal("1.000000000000")
    assert repository.recorded[0].converted_usd_amount == Decimal("-2.50000000")


def test_classifies_temporary_provider_and_repository_failures_as_retryable() -> None:
    calls: list[str] = []
    unavailable, repository = processor(
        RetryableRateError("timeout"), Inserted("tx-processor-1"), calls
    )
    database_failure, _ = processor(quote(), ConnectionError("database offline"), calls)

    assert unavailable.process(event()) == RetryableFailure("rate provider unavailable")
    assert repository.recorded == []
    assert database_failure.process(event()) == RetryableFailure("persistence unavailable")


def test_classifies_permanent_provider_and_invalid_quote_outcomes_without_persistence() -> None:
    calls: list[str] = []
    no_rate, repository = processor(
        PermanentRateError("no eligible historical rate"), Inserted("tx-processor-1"), calls
    )
    wrong_pair, wrong_pair_repository = processor(
        quote(base_currency="GBP"), Inserted("tx-processor-1"), calls
    )

    assert no_rate.process(event()) == PermanentFailure("no eligible historical rate")
    assert wrong_pair.process(event()) == PermanentFailure("rate quote does not match transaction")
    assert repository.recorded == []
    assert wrong_pair_repository.recorded == []
