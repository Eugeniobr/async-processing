"""Queue-agnostic transaction conversion and persistence coordination."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from application.money_converter import MoneyConverter
from application.ports import (
    ExchangeRateProvider,
    PermanentRateError,
    RetryableRateError,
    TransactionRepository,
)
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

ProcessingOutcome = Processed | Duplicate | RetryableFailure | PermanentFailure


class TransactionProcessor:
    """Convert one event and wait for the repository's committed outcome."""

    def __init__(
        self,
        rate_provider: ExchangeRateProvider,
        money_converter: MoneyConverter,
        repository: TransactionRepository,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._rate_provider = rate_provider
        self._money_converter = money_converter
        self._repository = repository
        self._now = now or (lambda: datetime.now(UTC))

    def process(self, event: TransactionEvent) -> ProcessingOutcome:
        """Return a classified outcome; delivery acknowledgement remains the caller's job."""

        try:
            if self._repository.has_transaction_id(event.transaction_id):
                return Duplicate(event.transaction_id)
        except Exception:
            return RetryableFailure("persistence unavailable")

        try:
            quote = self._rate_provider.get_usd_rate(event.currency, event.rate_requested_date)
        except RetryableRateError:
            return RetryableFailure("rate provider unavailable")
        except PermanentRateError as error:
            return PermanentFailure(str(error))

        if not self._matches_event(quote, event):
            return PermanentFailure("rate quote does not match transaction")

        try:
            applied_rate = self._money_converter.quantize_rate(quote.rate)
            converted_amount = self._money_converter.convert(event.original_amount, applied_rate)
            transaction = PersistedTransaction(
                event=event,
                rate_provider=quote.provider,
                rate_requested_date=quote.requested_date,
                rate_date=quote.rate_date,
                applied_rate=applied_rate,
                converted_usd_amount=converted_amount,
                created_at=self._now(),
            )
        except ValueError:
            return PermanentFailure("conversion cannot be represented")

        try:
            result = self._repository.record_first(transaction)
        except Exception:
            return RetryableFailure("persistence unavailable")
        if isinstance(result, Inserted):
            return Processed(result.transaction_id)
        if isinstance(result, AlreadyExists):
            return Duplicate(result.transaction_id)
        return RetryableFailure("persistence returned an unknown outcome")

    @staticmethod
    def _matches_event(quote: RateQuote, event: TransactionEvent) -> bool:
        return (
            quote.base_currency == event.currency
            and quote.quote_currency == "USD"
            and quote.requested_date == event.rate_requested_date
        )
