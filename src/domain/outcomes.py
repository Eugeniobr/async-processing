"""Explicit immutable outcomes used by later processing coordination."""

from __future__ import annotations

from dataclasses import dataclass


def _require_nonempty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be nonempty")


@dataclass(frozen=True)
class Processed:
    transaction_id: str

    def __post_init__(self) -> None:
        _require_nonempty(self.transaction_id, "transaction_id")


@dataclass(frozen=True)
class Duplicate:
    transaction_id: str

    def __post_init__(self) -> None:
        _require_nonempty(self.transaction_id, "transaction_id")


@dataclass(frozen=True)
class RetryableFailure:
    reason: str

    def __post_init__(self) -> None:
        _require_nonempty(self.reason, "reason")


@dataclass(frozen=True)
class PermanentFailure:
    reason: str

    def __post_init__(self) -> None:
        _require_nonempty(self.reason, "reason")
