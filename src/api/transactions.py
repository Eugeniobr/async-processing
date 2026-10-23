"""Validation for the durable transaction-acceptance boundary."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal, InvalidOperation

from composition.settings import CurrencyAllowlist
from domain.transaction import TransactionEvent
from infrastructure.tracing import NoopTransactionTracer, TransactionTracer


def parse_transaction_input(
    payload: object,
    allowlist: CurrencyAllowlist,
    *,
    tracer: TransactionTracer | None = None,
) -> TransactionEvent:
    """Return a validated event before any broker publication is attempted."""

    active_tracer = tracer or NoopTransactionTracer()
    candidate_id = payload.get("id") if isinstance(payload, Mapping) else None
    transaction_id = candidate_id if isinstance(candidate_id, str) else None
    with active_tracer.span(
        "transaction.validation", stage="api_validation", transaction_id=transaction_id
    ) as span:
        if not isinstance(payload, Mapping):
            raise ValueError("transaction input must be a JSON object")
        expected = {"id", "user_id", "amount", "currency", "timestamp"}
        if set(payload) != expected:
            raise ValueError(
                "transaction input must contain exactly id, user_id, amount, currency, timestamp"
            )

        transaction_id = _string(payload["id"], "id")
        user_id = _string(payload["user_id"], "user_id")
        amount_text = _string(payload["amount"], "amount")
        currency = _string(payload["currency"], "currency")
        timestamp_text = _string(payload["timestamp"], "timestamp")

        try:
            amount = Decimal(amount_text)
        except InvalidOperation as error:
            raise ValueError("amount must be a finite decimal string") from error
        try:
            timestamp = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError("timestamp must be an ISO 8601 datetime") from error

        event = TransactionEvent.from_accepted_input(
            supported_currencies=allowlist.codes,
            currency_allowlist_version=allowlist.version,
            transaction_id=transaction_id,
            user_id=user_id,
            original_amount=amount,
            currency=currency,
            event_timestamp=timestamp,
        )
        span.set_outcome("validated")
        return event


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value
