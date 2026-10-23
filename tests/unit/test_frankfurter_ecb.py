from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import httpx
import pytest

from application.ports import PermanentRateError, RetryableRateError
from infrastructure.frankfurter_ecb import FrankfurterEcbRateProvider


def provider(handler: httpx.MockTransport) -> FrankfurterEcbRateProvider:
    return FrankfurterEcbRateProvider(
        httpx.Client(transport=handler),
        now=lambda: datetime(2026, 10, 22, tzinfo=UTC),
    )


def test_returns_a_historical_base_to_usd_decimal_quote() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/providers/ecb/rate/eur/usd"
        assert request.url.params["date"] == "2026-10-21"
        return httpx.Response(
            200,
            json={"date": "2026-10-21", "base": "EUR", "quote": "USD", "rate": "1.234567890123"},
        )

    quote = provider(httpx.MockTransport(handler)).get_usd_rate("EUR", date(2026, 10, 21))

    assert quote.rate == Decimal("1.234567890123")
    assert quote.base_currency == "EUR"
    assert quote.quote_currency == "USD"
    assert quote.rate_date == date(2026, 10, 21)


def test_parses_a_json_number_as_decimal_without_a_float_round_trip() -> None:
    literal = "1.234567890123456789"

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=(
                '{"date":"2026-10-21","base":"EUR","quote":"USD","rate":' + literal + "}"
            ).encode(),
        )

    quote = provider(httpx.MockTransport(handler)).get_usd_rate("EUR", date(2026, 10, 21))

    assert quote.rate == Decimal(literal)


def test_uses_usd_identity_rate_without_an_http_call() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        pytest.fail("USD must not call the provider")

    quote = provider(httpx.MockTransport(handler)).get_usd_rate("USD", date(2026, 10, 21))

    assert quote.rate == Decimal("1")
    assert quote.rate_date == date(2026, 10, 21)


def test_404_consumes_one_day_and_returns_the_first_valid_previous_quote() -> None:
    dates: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        dates.append(request.url.params["date"])
        if request.url.params["date"] == "2026-10-21":
            return httpx.Response(404)
        return httpx.Response(
            200, json={"date": "2026-10-20", "base": "EUR", "quote": "USD", "rate": "1.08"}
        )

    quote = provider(httpx.MockTransport(handler)).get_usd_rate("EUR", date(2026, 10, 21))

    assert dates == ["2026-10-21", "2026-10-20"]
    assert quote.rate_date == date(2026, 10, 20)


@pytest.mark.parametrize(
    "response",
    [
        {"date": "2026-10-21", "base": "EUR", "quote": "USD", "rate": "NaN"},
        {"date": "2026-10-21", "base": "GBP", "quote": "USD", "rate": "1.08"},
        {"date": "2026-10-22", "base": "EUR", "quote": "USD", "rate": "1.08"},
        {"invalid": "payload"},
    ],
)
def test_rejects_malformed_or_mismatched_responses(response: dict[str, str]) -> None:
    adapter = provider(httpx.MockTransport(lambda _: httpx.Response(200, json=response)))

    with pytest.raises(PermanentRateError, match="invalid"):
        adapter.get_usd_rate("EUR", date(2026, 10, 21))


@pytest.mark.parametrize("status", [429, 500, 503])
def test_classifies_unavailable_provider_responses_as_retryable(status: int) -> None:
    adapter = provider(httpx.MockTransport(lambda _: httpx.Response(status)))

    with pytest.raises(RetryableRateError):
        adapter.get_usd_rate("EUR", date(2026, 10, 21))


@pytest.mark.parametrize("status", [400, 422])
def test_classifies_invalid_provider_requests_as_permanent(status: int) -> None:
    adapter = provider(httpx.MockTransport(lambda _: httpx.Response(status)))

    with pytest.raises(PermanentRateError):
        adapter.get_usd_rate("EUR", date(2026, 10, 21))


def test_classifies_connection_failure_as_retryable() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline")

    with pytest.raises(RetryableRateError, match="communication"):
        provider(httpx.MockTransport(handler)).get_usd_rate("EUR", date(2026, 10, 21))


def test_exhausted_missing_dates_are_permanent() -> None:
    adapter = provider(httpx.MockTransport(lambda _: httpx.Response(404)))

    with pytest.raises(PermanentRateError, match="seven days"):
        adapter.get_usd_rate("EUR", date(2026, 10, 21))
