from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from domain.transaction import RetryEnvelope, TransactionEvent


def valid_event(**changes: object) -> TransactionEvent:
    values: dict[str, object] = {
        "transaction_id": "transaction-1",
        "user_id": "user-1",
        "original_amount": Decimal("12.34567890"),
        "currency": "EUR",
        "event_timestamp": datetime(2026, 10, 22, 10, 30, tzinfo=UTC),
        "currency_allowlist_version": "currencies-v1",
    }
    values.update(changes)
    return TransactionEvent.from_accepted_input(
        supported_currencies=frozenset({"USD", "EUR"}),
        **values,
    )


def test_transaction_normalizes_a_timezone_aware_timestamp_to_utc() -> None:
    event = valid_event(event_timestamp=datetime.fromisoformat("2026-10-22T07:30:00-03:00"))

    assert event.event_timestamp == datetime(2026, 10, 22, 10, 30, tzinfo=UTC)
    assert event.rate_requested_date.isoformat() == "2026-10-22"


@pytest.mark.parametrize("amount", [Decimal("0"), Decimal("-12.50")])
def test_transaction_accepts_zero_and_negative_amounts(amount: Decimal) -> None:
    assert valid_event(original_amount=amount).original_amount == amount


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"transaction_id": ""}, "transaction_id"),
        ({"user_id": " "}, "user_id"),
        ({"currency": "eur"}, "currency"),
        ({"currency": "GBP"}, "supported"),
        ({"original_amount": Decimal("NaN")}, "finite"),
        ({"original_amount": Decimal("1.000000001")}, "eight"),
        ({"original_amount": Decimal("10000000000000000")}, "range"),
        ({"event_timestamp": datetime(2026, 10, 22, 10, 30)}, "timezone"),
        ({"currency_allowlist_version": ""}, "currency_allowlist_version"),
    ],
)
def test_transaction_rejects_invalid_domain_values(
    changes: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        valid_event(**changes)


def test_retry_envelope_is_immutable_and_increments_attempt() -> None:
    envelope = RetryEnvelope(
        schema_version=1,
        event=valid_event(),
        attempt=0,
        accepted_at=datetime.fromisoformat("2026-10-22T07:30:00-03:00"),
        currency_allowlist_version="currencies-v1",
    )

    next_envelope = envelope.for_confirmed_retry("provider_unavailable")

    assert envelope.attempt == 0
    assert envelope.accepted_at == datetime(2026, 10, 22, 10, 30, tzinfo=UTC)
    assert next_envelope.attempt == 1
    assert next_envelope.currency_allowlist_version == "currencies-v1"
    assert next_envelope.last_failure_kind == "provider_unavailable"


@pytest.mark.parametrize("attempt", [-1, 1.5])
def test_retry_envelope_rejects_an_invalid_attempt(attempt: int | float) -> None:
    with pytest.raises(ValueError, match="attempt"):
        RetryEnvelope(
            schema_version=1,
            event=valid_event(),
            attempt=attempt,  # type: ignore[arg-type]
            accepted_at=datetime.now(UTC),
            currency_allowlist_version="currencies-v1",
        )
