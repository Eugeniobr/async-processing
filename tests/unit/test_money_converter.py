from __future__ import annotations

from decimal import Decimal

import pytest

from application.money_converter import MoneyConverter


@pytest.mark.parametrize(
    ("amount", "rate", "expected"),
    [
        (Decimal("12.34567890"), Decimal("1"), Decimal("12.34567890")),
        (Decimal("0"), Decimal("1"), Decimal("0.00000000")),
        (Decimal("-12.50"), Decimal("1.2"), Decimal("-15.00000000")),
        (Decimal("10.12345678"), Decimal("1.2345678901234"), Decimal("12.49809468")),
    ],
)
def test_convert_uses_usd_per_unit_decimal_arithmetic(
    amount: Decimal, rate: Decimal, expected: Decimal
) -> None:
    assert MoneyConverter().convert(amount, rate) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("1.2345678901235"), Decimal("1.234567890124")),
        (Decimal("1.2345678901225"), Decimal("1.234567890122")),
    ],
)
def test_quantize_rate_uses_round_half_even(value: Decimal, expected: Decimal) -> None:
    assert MoneyConverter().quantize_rate(value) == expected


@pytest.mark.parametrize(
    ("amount", "rate", "expected"),
    [
        (Decimal("1"), Decimal("1.000000005"), Decimal("1.00000000")),
        (Decimal("1"), Decimal("1.000000015"), Decimal("1.00000002")),
    ],
)
def test_convert_uses_round_half_even_at_usd_storage_scale(
    amount: Decimal, rate: Decimal, expected: Decimal
) -> None:
    assert MoneyConverter().convert(amount, rate) == expected


@pytest.mark.parametrize(
    ("amount", "rate"),
    [
        (Decimal("NaN"), Decimal("1")),
        (Decimal("1"), Decimal("Infinity")),
        (Decimal("1"), Decimal("0")),
        (Decimal("10000000000000000"), Decimal("1")),
        (Decimal("9999999999999999.99999999"), Decimal("2")),
        (Decimal("1"), Decimal("1000000000000")),
    ],
)
def test_convert_rejects_non_finite_or_unstorable_values(amount: Decimal, rate: Decimal) -> None:
    with pytest.raises(ValueError):
        MoneyConverter().convert(amount, rate)
