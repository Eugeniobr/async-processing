"""Validated development configuration; no service behavior is assembled here."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, HttpUrl, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True)
class CurrencyAllowlist:
    """A reviewed provider-coverage snapshot used by future ingestion validation."""

    version: str
    provider: str
    source_url: str
    retrieved_on: date
    codes: frozenset[str]


class Settings(BaseSettings):
    """Configuration shared by future API and consumer composition roots."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    database_url: str
    rabbitmq_url: str
    frankfurter_base_url: HttpUrl = "https://api.frankfurter.dev"
    currency_allowlist_path: Path = Path("config/currencies-v1.json")
    consumer_prefetch: int = Field(default=100, ge=1)
    rate_fallback_days: int = Field(default=7, ge=0, le=7)
    otlp_traces_endpoint: str | None = None
    metrics_mode: Literal["multiprocess", "instance"] = "multiprocess"
    consumer_concurrency: int = Field(default=4, ge=1, le=64)
    consumer_shutdown_seconds: float = Field(default=30, gt=0, le=300)
    consumer_metrics_port: int = Field(default=8001, ge=1, le=65535)
    database_pool_size: int = Field(default=4, ge=1, le=64)
    database_pool_timeout: float = Field(default=5, gt=0)
    database_statement_timeout_ms: int = Field(default=10000, ge=1)
    rate_timeout_seconds: float = Field(default=5, gt=0, le=60)

    @field_validator("app_env")
    @classmethod
    def validate_app_env(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("APP_ENV must not be empty")
        return value


def load_currency_allowlist(path: Path) -> CurrencyAllowlist:
    """Load a versioned, syntactically valid allow-list without contacting a provider."""

    try:
        payload: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Unable to load currency allow-list at {path}") from error

    if not isinstance(payload, dict):
        raise ValueError("Currency allow-list must be a JSON object")

    required = {"version", "provider", "source_url", "retrieved_on", "codes"}
    if set(payload) != required:
        raise ValueError("Currency allow-list must contain only the required metadata fields")

    version = payload["version"]
    provider = payload["provider"]
    source_url = payload["source_url"]
    raw_codes = payload["codes"]
    if not all(
        isinstance(value, str) and value.strip() for value in (version, provider, source_url)
    ):
        raise ValueError("Currency allow-list metadata values must be nonempty strings")
    if not isinstance(raw_codes, list) or not raw_codes:
        raise ValueError("Currency allow-list codes must be a nonempty list")
    if any(not isinstance(code, str) or len(code) != 3 or not code.isupper() for code in raw_codes):
        raise ValueError("Currency allow-list codes must be uppercase three-letter strings")
    codes = frozenset(raw_codes)
    if len(codes) != len(raw_codes):
        raise ValueError("Currency allow-list codes must be unique")
    if "USD" not in codes:
        raise ValueError("Currency allow-list must include USD")

    try:
        retrieved_on = date.fromisoformat(payload["retrieved_on"])
    except (TypeError, ValueError) as error:
        raise ValueError("Currency allow-list retrieved_on must be an ISO date") from error

    return CurrencyAllowlist(
        version=version,
        provider=provider,
        source_url=source_url,
        retrieved_on=retrieved_on,
        codes=codes,
    )


__all__ = ["CurrencyAllowlist", "Settings", "ValidationError", "load_currency_allowlist"]
