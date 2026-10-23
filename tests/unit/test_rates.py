from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from domain.rates import RateQuote


def test_rate_quote_accepts_a_positive_finite_usd_pair() -> None:
    quote = RateQuote(
        base_currency="EUR",
        quote_currency="USD",
        rate=Decimal("1.0825"),
        requested_date=date(2026, 10, 22),
        rate_date=date(2026, 10, 21),
        provider="ecb",
        fetched_at=datetime.fromisoformat("2026-10-22T07:30:00-03:00"),
    )

    assert quote.fetched_at == datetime(2026, 10, 22, 10, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"rate": Decimal("NaN")}, "positive finite"),
        ({"rate": Decimal("0")}, "positive finite"),
        ({"quote_currency": "EUR"}, "USD"),
        ({"rate_date": date(2026, 10, 23)}, "later"),
        ({"rate_date": date(2026, 10, 14)}, "seven"),
        ({"fetched_at": datetime(2026, 10, 22, 10, 30)}, "timezone"),
    ],
)
def test_rate_quote_rejects_invalid_values(changes: dict[str, object], message: str) -> None:
    values: dict[str, object] = {
        "base_currency": "EUR",
        "quote_currency": "USD",
        "rate": Decimal("1.0825"),
        "requested_date": date(2026, 10, 22),
        "rate_date": date(2026, 10, 22),
        "provider": "ecb",
        "fetched_at": datetime(2026, 10, 22, 10, 30, tzinfo=UTC),
    }
    values.update(changes)

    with pytest.raises(ValueError, match=message):
        RateQuote(**values)  # type: ignore[arg-type]
