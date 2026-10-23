from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from composition.settings import Settings, load_currency_allowlist


def test_settings_require_database_and_broker_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("RABBITMQ_URL", raising=False)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_allowlist_loads_versioned_currency_snapshot() -> None:
    allowlist = load_currency_allowlist(path=Path("config/currencies-v1.json"))

    assert allowlist.version == "currencies-v1"
    assert allowlist.provider == "ecb"
    assert "USD" in allowlist.codes


@pytest.mark.parametrize(
    "payload",
    [
        {
            "version": "currencies-v1",
            "provider": "ecb",
            "source_url": "x",
            "retrieved_on": "2026-10-22",
            "codes": ["USD", "USD"],
        },
        {
            "version": "currencies-v1",
            "provider": "ecb",
            "source_url": "x",
            "retrieved_on": "2026-10-22",
            "codes": ["EUR"],
        },
    ],
)
def test_allowlist_rejects_duplicates_or_missing_usd(tmp_path, payload: dict[str, object]) -> None:
    path = tmp_path / "currencies.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError):
        load_currency_allowlist(path)
