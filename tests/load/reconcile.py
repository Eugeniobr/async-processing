"""Reconcile accepted synthetic load IDs with their PostgreSQL records."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import psycopg


def accepted_ids(raw_path: Path) -> set[str]:
    """Read only the optional, test-only local k6 acceptance ledger."""

    values: set[str] = set()
    for line in raw_path.read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record.get("metric") != "transaction_acceptance_id_ledger":
            continue
        if record.get("type") != "Point" or record.get("data", {}).get("value") != 1:
            continue
        transaction_id = record["data"]["tags"].get("transaction_id")
        if isinstance(transaction_id, str):
            values.add(transaction_id)
    return values


def reconcile(database_url: str, ids: set[str]) -> dict[str, object]:
    """Compare distinct accepted IDs with committed rows, never inferring by counts alone."""

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT transaction_id FROM transactions.transactions "
                "WHERE transaction_id = ANY(%s)",
                (list(ids),),
            )
            persisted = {row[0] for row in cursor}
            cursor.execute(
                """
                SELECT transaction_id
                FROM transactions.transactions
                WHERE transaction_id = ANY(%s)
                GROUP BY transaction_id
                HAVING count(*) > 1
                """,
                (list(ids),),
            )
            duplicates = {row[0] for row in cursor}
    return {
        "accepted_distinct_ids": len(ids),
        "persisted_matching_ids": len(persisted),
        "missing_persisted_ids": sorted(ids - persisted),
        "unexpected_persisted_ids": sorted(persisted - ids),
        "duplicate_persisted_ids": sorted(duplicates),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    report = reconcile(arguments.database_url, accepted_ids(arguments.raw))
    arguments.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
