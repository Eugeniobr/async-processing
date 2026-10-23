"""Focused acceptance-boundary validation tests."""

from __future__ import annotations

from datetime import UTC, date
from decimal import Decimal

import pytest

from api.transactions import parse_transaction_input
from composition.settings import CurrencyAllowlist


@pytest.fixture()
def allowlist() -> CurrencyAllowlist:
    return CurrencyAllowlist(
        version="currencies-v1",
        provider="ecb",
        source_url="https://example.invalid/currencies",
        retrieved_on=date(2026, 10, 22),
        codes=frozenset({"EUR", "USD"}),
    )


def test_parses_decimal_and_normalizes_timestamp_to_utc(allowlist: CurrencyAllowlist) -> None:
    event = parse_transaction_input(
        {
            "id": "transaction-1",
            "user_id": "user-1",
            "amount": "-0.12500000",
            "currency": "EUR",
            "timestamp": "2026-10-22T07:30:00-03:00",
        },
        allowlist,
    )

    assert event.original_amount == Decimal("-0.12500000")
    assert event.event_timestamp == event.event_timestamp.astimezone(UTC)
    assert event.event_timestamp.isoformat() == "2026-10-22T10:30:00+00:00"
    assert event.currency_allowlist_version == "currencies-v1"


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount", 1),
        ("amount", "Infinity"),
        ("amount", "1.000000001"),
        ("currency", "eur"),
        ("currency", "ZZZ"),
        ("timestamp", "2026-10-22T10:30:00"),
    ],
)
def test_rejects_invalid_input_before_publication(
    allowlist: CurrencyAllowlist, field: str, value: object
) -> None:
    payload: dict[str, object] = {
        "id": "transaction-1",
        "user_id": "user-1",
        "amount": "1.00000000",
        "currency": "EUR",
        "timestamp": "2026-10-22T10:30:00Z",
    }
    payload[field] = value

    with pytest.raises(ValueError):
        parse_transaction_input(payload, allowlist)
