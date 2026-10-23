"""Pure Decimal conversion under the approved storage rules."""

from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext

from domain.rates import MAX_RATE, RATE_QUANTUM
from domain.transaction import AMOUNT_QUANTUM, MAX_AMOUNT, validate_amount


class MoneyConverter:
    """Convert an amount using a stored USD-per-unit rate without I/O or state."""

    def quantize_rate(self, rate: Decimal) -> Decimal:
        """Round a positive rate to the approved NUMERIC(24,12) storage scale."""

        if not isinstance(rate, Decimal) or not rate.is_finite() or rate <= 0:
            raise ValueError("rate must be positive and finite")
        if rate > MAX_RATE:
            raise ValueError("rate is outside the numeric(24,12) range")
        try:
            with localcontext() as context:
                context.prec = 50
                quantized = rate.quantize(RATE_QUANTUM, rounding=ROUND_HALF_EVEN)
        except InvalidOperation as error:
            raise ValueError("rate cannot be represented as numeric(24,12)") from error
        if quantized <= 0 or quantized > MAX_RATE:
            raise ValueError("rate cannot be represented as positive numeric(24,12)")
        return quantized

    def convert(self, amount: Decimal, usd_per_unit: Decimal) -> Decimal:
        """Return ``amount × usd_per_unit`` rounded for NUMERIC(24,8) storage."""

        validate_amount(amount)
        rate = self.quantize_rate(usd_per_unit)
        try:
            with localcontext() as context:
                context.prec = 50
                converted = (amount * rate).quantize(AMOUNT_QUANTUM, rounding=ROUND_HALF_EVEN)
        except InvalidOperation as error:
            raise ValueError("converted amount cannot be represented as numeric(24,8)") from error
        if abs(converted) > MAX_AMOUNT:
            raise ValueError("converted amount is outside the numeric(24,8) range")
        return converted
