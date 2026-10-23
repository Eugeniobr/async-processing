"""Immutable transaction and durable-delivery value objects."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal

AMOUNT_SCALE = 8
AMOUNT_QUANTUM = Decimal("0.00000001")
MAX_AMOUNT = Decimal("9999999999999999.99999999")


def _require_nonempty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be nonempty")


def _normalize_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")
    return value.astimezone(UTC)


def validate_amount(value: Decimal) -> None:
    """Validate a value that must fit the approved NUMERIC(24,8) amount range."""

    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("original_amount must be finite")
    if value.as_tuple().exponent < -AMOUNT_SCALE:
        raise ValueError("original_amount must have at most eight fractional digits")
    if abs(value) > MAX_AMOUNT:
        raise ValueError("original_amount is outside the numeric(24,8) range")


@dataclass(frozen=True)
class TransactionEvent:
    """Validated transaction data, normalized to UTC before durable acceptance."""

    transaction_id: str
    user_id: str
    original_amount: Decimal
    currency: str
    event_timestamp: datetime
    currency_allowlist_version: str

    def __post_init__(self) -> None:
        _require_nonempty(self.transaction_id, "transaction_id")
        _require_nonempty(self.user_id, "user_id")
        validate_amount(self.original_amount)
        if (
            not isinstance(self.currency, str)
            or len(self.currency) != 3
            or not self.currency.isupper()
        ):
            raise ValueError("currency must be an uppercase three-letter code")
        _require_nonempty(self.currency_allowlist_version, "currency_allowlist_version")
        object.__setattr__(
            self, "event_timestamp", _normalize_utc(self.event_timestamp, "event_timestamp")
        )

    @classmethod
    def from_accepted_input(
        cls,
        *,
        supported_currencies: Collection[str],
        currency_allowlist_version: str,
        transaction_id: str,
        user_id: str,
        original_amount: Decimal,
        currency: str,
        event_timestamp: datetime,
    ) -> TransactionEvent:
        """Construct an event only when its currency belongs to the active snapshot."""

        if currency not in supported_currencies:
            raise ValueError("currency is not supported by the active allow-list")
        return cls(
            transaction_id=transaction_id,
            user_id=user_id,
            original_amount=original_amount,
            currency=currency,
            event_timestamp=event_timestamp,
            currency_allowlist_version=currency_allowlist_version,
        )

    @property
    def rate_requested_date(self) -> date:
        """Return the approved exchange-rate reference date in UTC."""

        return self.event_timestamp.date()


@dataclass(frozen=True)
class RetryEnvelope:
    """Persistent retry metadata kept immutable across duplicate deliveries."""

    schema_version: int
    event: TransactionEvent
    attempt: int
    accepted_at: datetime
    currency_allowlist_version: str
    last_failure_kind: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.schema_version, int) or self.schema_version < 1:
            raise ValueError("schema_version must be a positive integer")
        if not isinstance(self.event, TransactionEvent):
            raise ValueError("event must be a TransactionEvent")
        if not isinstance(self.attempt, int) or isinstance(self.attempt, bool) or self.attempt < 0:
            raise ValueError("attempt must be a nonnegative integer")
        _require_nonempty(self.currency_allowlist_version, "currency_allowlist_version")
        if self.currency_allowlist_version != self.event.currency_allowlist_version:
            raise ValueError("currency_allowlist_version must match the accepted event")
        if self.last_failure_kind is not None:
            _require_nonempty(self.last_failure_kind, "last_failure_kind")
        object.__setattr__(self, "accepted_at", _normalize_utc(self.accepted_at, "accepted_at"))

    def for_confirmed_retry(self, failure_kind: str) -> RetryEnvelope:
        """Return the next immutable envelope after retry forwarding is confirmed."""

        _require_nonempty(failure_kind, "failure_kind")
        return replace(self, attempt=self.attempt + 1, last_failure_kind=failure_kind)
