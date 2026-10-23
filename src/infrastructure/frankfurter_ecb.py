"""Frankfurter v2 adapter pinned to the ECB historical-rate provider."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation

import httpx

from application.ports import PermanentRateError, RetryableRateError
from domain.rates import RateQuote
from infrastructure.tracing import NoopTransactionTracer, TransactionTracer


class FrankfurterEcbRateProvider:
    """Fetch one base-to-USD ECB quote with the approved historical-date policy."""

    def __init__(
        self,
        client: httpx.Client,
        *,
        base_url: str = "https://api.frankfurter.dev",
        timeout_seconds: float = 5.0,
        now: Callable[[], datetime] | None = None,
        tracer: TransactionTracer | None = None,
    ) -> None:
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._now = now or (lambda: datetime.now(UTC))
        self._tracer = tracer or NoopTransactionTracer()

    def get_usd_rate(self, currency: str, requested_date: date) -> RateQuote:
        """Return the first valid quote from the requested date through day seven."""

        with self._tracer.span("transaction.rate_lookup", stage="rate_lookup") as span:
            if not isinstance(currency, str) or len(currency) != 3 or not currency.isupper():
                raise PermanentRateError("currency must be an uppercase three-letter code")
            if requested_date > self._now().astimezone(UTC).date():
                raise PermanentRateError("future requested dates are not eligible for a rate")
            if currency == "USD":
                quote = RateQuote(
                    base_currency="USD",
                    quote_currency="USD",
                    rate=Decimal("1"),
                    requested_date=requested_date,
                    rate_date=requested_date,
                    provider="ecb",
                    fetched_at=self._now(),
                )
                span.set_outcome("found")
                return quote

            for offset in range(8):
                candidate_date = requested_date - timedelta(days=offset)
                response = self._request(currency, candidate_date)
                if response.status_code == 404:
                    continue
                if response.status_code in {429} or response.status_code >= 500:
                    raise RetryableRateError(f"rate provider returned HTTP {response.status_code}")
                if response.status_code >= 400:
                    raise PermanentRateError(f"rate provider returned HTTP {response.status_code}")
                if response.status_code != 200:
                    raise PermanentRateError(
                        f"unexpected rate provider HTTP {response.status_code}"
                    )
                quote = self._parse_quote(response, currency, requested_date)
                span.set_outcome("found")
                return quote
            raise PermanentRateError("no eligible historical rate exists within seven days")

    def _request(self, currency: str, candidate_date: date) -> httpx.Response:
        try:
            return self._client.get(
                f"{self._base_url}/v2/providers/ecb/rate/{currency.lower()}/usd",
                params={"date": candidate_date.isoformat()},
                timeout=self._timeout_seconds,
            )
        except httpx.HTTPError as error:
            raise RetryableRateError("rate provider communication failed") from error

    def _parse_quote(
        self, response: httpx.Response, currency: str, requested_date: date
    ) -> RateQuote:
        try:
            # httpx forwards these arguments to json.loads.  Parsing the JSON
            # number directly as Decimal avoids a lossy float round-trip.
            payload = response.json(parse_float=Decimal)
            if not isinstance(payload, dict):
                raise ValueError("response is not an object")
            if payload["base"].upper() != currency or payload["quote"].upper() != "USD":
                raise ValueError("response pair does not match the requested base-to-USD pair")
            rate = Decimal(str(payload["rate"]))
            return RateQuote(
                base_currency=payload["base"].upper(),
                quote_currency=payload["quote"].upper(),
                rate=rate,
                requested_date=requested_date,
                rate_date=date.fromisoformat(payload["date"]),
                provider="ecb",
                fetched_at=self._now(),
            )
        except (KeyError, TypeError, ValueError, InvalidOperation) as error:
            raise PermanentRateError("rate provider returned an invalid response") from error
