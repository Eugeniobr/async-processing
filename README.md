# Async Processing

A small, specification-driven Python service for accepting transactions, processing them
asynchronously, and exposing completed USD conversions. It is designed as an interview
walkthrough: the HTTP API accepts durable work; RabbitMQ and PostgreSQL provide the
processing and idempotency boundaries.

## What an acceptance means

`POST /transactions` returns `202` only after RabbitMQ has positively confirmed a
persistent, mandatory-routed publication to `transactions.work`. It does **not** mean
that conversion or PostgreSQL persistence has completed. A client can safely resend the
same transaction ID after a `503`; duplicate broker deliveries are resolved by the
database unique key, where the first committed transaction wins.

Completed records are available through:

- `GET /users/{user_id}/summary`
- `GET /users/{user_id}/transactions?from=<UTC>&to=<UTC>`
- `GET /metrics` for Prometheus-format processing-failure observations

The API contract is in
[specs/001-async-transaction-processing/contracts/openapi.yaml](specs/001-async-transaction-processing/contracts/openapi.yaml).

## Local startup

Requirements: Docker Compose, Python 3.12, and (only for the load procedure) k6. The
commands below run the API and consumer as separate host processes so they can share a
Prometheus multiprocess directory.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
cp .env.example .env
docker compose up -d --wait

# This directory is operational metric state, not business data. Clear it only while
# both API and consumer are stopped.
export PROMETHEUS_MULTIPROC_DIR="$(pwd)/.runtime/prometheus"
mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
find "$PROMETHEUS_MULTIPROC_DIR" -maxdepth 1 -type f -delete
```

Apply the versioned schema migration once to the development database:

```bash
python - <<'PY'
import importlib.util
from pathlib import Path

from sqlalchemy import create_engine
from composition.settings import Settings

spec = importlib.util.spec_from_file_location("migration_001", Path("migrations/versions/001_transactions.py"))
assert spec and spec.loader
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)
engine = create_engine(Settings().database_url)
with engine.begin() as connection:
    migration.upgrade(connection)
engine.dispose()
PY
```

Start these in separate terminals with the virtual environment and
`PROMETHEUS_MULTIPROC_DIR` exported in each:

```bash
python -m composition.api_main
python -m composition.consumer_main
```

Use a USD event for an offline local smoke test; USD is converted at rate `1` without a
provider request:

```bash
curl -i http://localhost:8000/transactions \
  -H 'content-type: application/json' \
  -d '{"id":"demo-001","user_id":"demo-user","amount":"12.34000000","currency":"USD","timestamp":"2026-10-22T10:30:00Z"}'
curl 'http://localhost:8000/users/demo-user/summary'
curl http://localhost:8000/metrics
```

`docker compose down` stops local infrastructure. Add `-v` only when deliberately
discarding PostgreSQL and RabbitMQ development data.

## Processing and queue rationale

The broker is pinned to `rabbitmq:4.3.6-management`. Its durable direct exchanges route
to non-exclusive quorum queues:

| Queue | Purpose |
| --- | --- |
| `transactions.work` | Accepted work and messages returned from a retry bucket. |
| `transactions.retry.1s` … `.60s` | Seven fixed-delay durable retry buckets: 1, 2, 4, 8, 16, 32, then 60 seconds. |
| `transactions.failure` | Inspectable permanent failures. |

All queues use quorum replication semantics, durable messages, `overflow=reject-publish`,
`max-length=100000`, and `delivery-limit=-1`. Retry queues additionally use the
at-least-once dead-letter strategy and expire back to the work route. Publishers use
mandatory routing and confirms; consumers acknowledge only after a database commit or a
confirmed retry/failure forward. See [docker/rabbitmq/README.md](docker/rabbitmq/README.md)
for the topology details.

Fixed TTL buckets make the retry schedule visible and durable across consumer restarts.
Their trade-off is seven queues and coarse, fixed delays rather than arbitrary per-message
delays. A confirmed retry forward followed by a consumer crash before the original ACK can
produce duplicate deliveries, never an exactly-once claim; PostgreSQL deduplication makes
the final persisted transaction idempotent. The Compose topology has one RabbitMQ node, so
it demonstrates local disk/restart persistence, not multi-node quorum-member loss.

## Rates, currencies, money, and time

The provider adapter calls Frankfurter v2 pinned to the ECB provider for base-to-USD daily
rates. It normalizes an event timestamp to UTC, requests that calendar date first, and
falls back one day at a time only for a documented no-rate response, through seven prior
days. Timeout, connection, `429`, and `5xx` failures are retryable; malformed/mismatched
responses, future dates, and exhaustion of the eight-date window are permanent. The
persisted transaction records both the requested and returned rate dates.

`config/currencies-v1.json` is the reviewed, versioned ECB allow-list. Startup validates
its schema, uppercase unique codes, and USD inclusion. An unsupported input is rejected
before publication. A coverage change after acceptance becomes a durable permanent failure;
the accepted event is not silently discarded. Updates are generated from the recorded
provider source, validated against a known business date in staging, reviewed, version
bumped, and deployed atomically.

Amounts use `Decimal` end-to-end. Original and converted amounts are bounded
`NUMERIC(24,8)` values with at most eight fractional places and absolute value no greater
than `9,999,999,999,999,999.99999999`. Applied USD-per-unit rates use `NUMERIC(24,12)`.
Rates and converted amounts are quantized with `ROUND_HALF_EVEN`; overflowing input or
conversion results are rejected instead of truncated. Event timestamps are stored in UTC.

## Metrics and observability

`transaction_processing_failures_total{kind="retryable|permanent"}` increments once only
after the classified retry/failure forwarding has received RabbitMQ confirmation and before
the original delivery is acknowledged. It does not count acceptance, success, duplicates,
or unconfirmed forwarding. The API scrapes the shared Prometheus multiprocess directory;
the consumer and API must both start with the same exported directory. See
[docs/metrics-observability.md](docs/metrics-observability.md) for its cross-process and
crash-observation limits.

For the local transaction journey view, start Compose and browse Jaeger at
`http://localhost:16686`. It is development observability only: trace-export failure does
not change acceptance, acknowledgement, retry, or persistence behavior.

## Verification and load measurement

Run the regular checks with:

```bash
python -m ruff format --check .
python -m ruff check .
python -m pytest
```

The reproducible load procedure is in [tests/load/transactions.js](tests/load/transactions.js).
It exercises the real HTTP acceptance route, samples completed history, queue depth, and
the failure metric. Run the sustained and burst scenarios separately, then record actual
results (not capacity claims) in [docs/load-report-template.md](docs/load-report-template.md):

```bash
k6 run -e SCENARIO=sustained -e BASE_URL=http://localhost:8000 tests/load/transactions.js
k6 run -e SCENARIO=burst -e BASE_URL=http://localhost:8000 tests/load/transactions.js
```

The executed local measurement and its limitations are recorded in
[docs/load-report-2026-10-23.md](docs/load-report-2026-10-23.md). Its reviewed, checksum-
verified archive is committed under [docs/evidence/](docs/evidence/); the complete local
working set, including preliminary runs and diagnostics, remains in `.load-evidence/`.

At ten times the target load, benchmark first and expect to tune consumer count/prefetch,
database connection and index capacity, and RabbitMQ cluster/disk throughput. Replace the
single-node Compose broker with a tested multi-node quorum deployment, place the shared
Prometheus directory on appropriate shared storage, and reassess queue length and back
pressure. Do not increase queue limits or claim capacity without new measurements.

## Project documents

- [Historical specification v0.1](docs/specification-v0.1.md)
- [Constitution](.specify/memory/constitution.md)
- [Feature specification](specs/001-async-transaction-processing/spec.md)
- [Technical plan](specs/001-async-transaction-processing/plan.md)
- [Research decisions](specs/001-async-transaction-processing/research.md)
- [Verification report](docs/verification-report.md)
- [Latest local load report](docs/load-report-2026-10-23.md)
