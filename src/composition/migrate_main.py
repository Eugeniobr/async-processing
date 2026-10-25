"""Apply the single approved migration as a separate one-shot process."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from sqlalchemy import create_engine, text

from composition.settings import Settings


def run() -> None:
    path = Path(__file__).resolve().parents[2] / "migrations/versions/001_transactions.py"
    if not path.exists():  # Installed wheel: migration files live in the image workdir.
        path = Path("migrations/versions/001_transactions.py")
    spec = importlib.util.spec_from_file_location("migration_001", path)
    assert spec and spec.loader
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    engine = create_engine(Settings().database_url)
    try:
        with engine.begin() as connection:
            connection.execute(text("SELECT pg_advisory_xact_lock(20261024)"))
            migration.upgrade(connection)
    finally:
        engine.dispose()


if __name__ == "__main__":
    run()
