"""Summarize local k6 raw data without treating all HTTP requests as acceptance."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from math import ceil
from pathlib import Path


def percentile(values: list[float], percentile_value: float) -> float | None:
    """Return the nearest-rank percentile for an already local test sample."""

    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, ceil(percentile_value * len(ordered)) - 1))
    return ordered[index]


def summarize(raw_path: Path) -> dict[str, object]:
    """Extract POST acceptance throughput/status/latency from a k6 JSON stream."""

    durations: list[float] = []
    timestamps: list[datetime] = []
    post_statuses: Counter[str] = Counter()
    all_statuses: Counter[str] = Counter()
    ledger_ids: set[str] = set()

    for line in raw_path.read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record.get("type") != "Point":
            continue
        data = record["data"]
        tags = data.get("tags", {})
        metric = record.get("metric")
        if metric == "transaction_acceptance_id_ledger" and data.get("value") == 1:
            transaction_id = tags.get("transaction_id")
            if isinstance(transaction_id, str):
                ledger_ids.add(transaction_id)
        if metric != "http_reqs":
            continue
        status = str(tags.get("status", "unknown"))
        all_statuses[status] += 1
        if tags.get("method") == "POST" and tags.get("url", "").endswith("/transactions"):
            post_statuses[status] += 1
        if metric == "http_req_duration":
            continue
        # k6 emits the matching duration in a separate point, so this branch only
        # classifies statuses. Duration collection happens below in a second pass.

    for line in raw_path.read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record.get("type") != "Point" or record.get("metric") != "http_req_duration":
            continue
        data = record["data"]
        tags = data.get("tags", {})
        if tags.get("method") == "POST" and tags.get("url", "").endswith("/transactions"):
            durations.append(float(data["value"]))
            timestamps.append(datetime.fromisoformat(data["time"].replace("Z", "+00:00")))

    span_seconds = (max(timestamps) - min(timestamps)).total_seconds() if len(timestamps) > 1 else 0
    accepted = post_statuses["202"]
    return {
        "accepted_distinct_ids": len(ledger_ids),
        "acceptance_http_statuses": dict(sorted(post_statuses.items())),
        "all_http_statuses": dict(sorted(all_statuses.items())),
        "acceptance_latency_ms": {
            "p50": percentile(durations, 0.50),
            "p95": percentile(durations, 0.95),
            "p99": percentile(durations, 0.99),
            "min": min(durations) if durations else None,
            "max": max(durations) if durations else None,
        },
        "acceptance_measurement_span_seconds": span_seconds,
        "effective_accepted_per_second": accepted / span_seconds if span_seconds else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    report = summarize(arguments.raw)
    arguments.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
