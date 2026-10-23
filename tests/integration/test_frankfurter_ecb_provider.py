"""Optional live-provider checks; deterministic policy tests stay in the unit suite."""

from __future__ import annotations

import os
from datetime import date

import httpx
import pytest

from infrastructure.frankfurter_ecb import FrankfurterEcbRateProvider

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_EXTERNAL_PROVIDER_TESTS") != "1",
    reason="set RUN_EXTERNAL_PROVIDER_TESTS=1 to call the public Frankfurter provider",
)


def test_ecb_provider_returns_a_known_business_day_and_weekend_reference_date() -> None:
    with httpx.Client() as client:
        provider = FrankfurterEcbRateProvider(client)
        business_day = provider.get_usd_rate("EUR", date(2024, 5, 15))
        weekend = provider.get_usd_rate("EUR", date(2024, 5, 18))

    assert business_day.rate_date == date(2024, 5, 15)
    assert weekend.rate_date <= weekend.requested_date
    assert weekend.rate_date >= date(2024, 5, 11)
