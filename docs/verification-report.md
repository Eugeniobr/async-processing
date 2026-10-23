# Verification Report

**Date:** 2026-10-23
**Scope:** `001-async-transaction-processing`
**Infrastructure:** local Docker Compose PostgreSQL and RabbitMQ 4.3.6; Jaeger 2.22 for
the tracing check. This report distinguishes executed evidence from procedures that remain
unexecuted.

## Executed matrix

| Area | Evidence | Result |
| --- | --- | --- |
| Decimal conversion, validation, rate policy, ports, processing, HTTP contracts, and metrics contracts | `python -m ruff format --check src tests && python -m ruff check src tests && pytest -q tests/unit tests/contract` | 145 passed; one upstream `TestClient` deprecation warning. |
| Confirmed acceptance and producer re-submission | `tests/integration/test_transaction_acceptance.py` in the real-infrastructure matrix | Passed. A `202` followed an actual confirm; two submissions with one ID remained two work deliveries for downstream idempotency. |
| Concurrent idempotency | `tests/integration/test_concurrent_deduplication.py` | Passed. Real independent RabbitMQ/PostgreSQL attempts left one first-write-wins row. |
| Temporary outages, retry forwarding, permanent classification | `tests/integration/test_retry_and_failure_routing.py` | Passed. It covers retry buckets, provider/database outage recovery, malformed deliveries, exhausted historical lookup, and coverage change. |
| Commit/ACK and confirmed-retry/ACK crash windows | `tests/recovery` | Passed. The controller uses IPC checkpoints and SIGKILL, then verifies redelivery, idempotent persistence, and empty relevant queues. |
| Query visibility and exact read semantics | `tests/integration/test_user_queries.py` | Passed. PostgreSQL-backed query routes show only committed records with documented range/order/pagination semantics. |
| Failure metric and durable failures | `tests/integration/test_metrics_and_failures.py` | Passed. Separate consumer/API processes exposed both confirmed-forwarding labels through the multiprocess scraper; the failure queue survived consumer restart. |
| Live transaction tracing | `tests/integration/test_live_tracing.py` | Passed. Jaeger returned one correlated trace across acceptance, publish, consumption, conversion, rate lookup, and persistence; exporter outage did not change delivery behavior. |
| Combined real-infrastructure matrix | `pytest -q tests/integration/test_transaction_acceptance.py tests/integration/test_concurrent_deduplication.py tests/integration/test_retry_and_failure_routing.py tests/integration/test_user_queries.py tests/integration/test_metrics_and_failures.py tests/integration/test_live_tracing.py tests/recovery` | 15 passed; one `TestClient` deprecation warning. |
| Broker restart at acceptance boundary | Controlled isolated-vhost check in `.t016-restart3.log` | Passed. A confirmed persistent work message remained ready after a normal RabbitMQ container restart. |
| Final repository suite | `python -m ruff format --check . && python -m ruff check . && pytest -q` | 153 passed, 17 infrastructure/external-provider skips, and one upstream `TestClient` deprecation warning. The real-infrastructure matrix above supplies evidence for the skipped categories. |

## Requirement-to-evidence map

| Requirement | Evidence |
| --- | --- |
| FR-001–FR-004 | `tests/contract/test_submit_transaction.py`, `tests/unit/test_transaction_input.py`, and `tests/integration/test_transaction_acceptance.py` |
| FR-005–FR-007 | `tests/unit/test_money_converter.py`, `tests/unit/test_frankfurter_ecb.py`, `tests/unit/test_transaction_processor.py`, and retry/failure integration coverage |
| FR-008 | `tests/integration/test_concurrent_deduplication.py` and `tests/integration/test_postgres_repository.py` |
| FR-009–FR-011 | `tests/integration/test_retry_and_failure_routing.py`, `tests/recovery/`, and consumer unit tests |
| FR-012–FR-014 | `tests/contract/test_user_queries.py`, `tests/unit/test_transaction_queries.py`, and `tests/integration/test_user_queries.py` |
| FR-015 | `tests/contract/test_metrics.py` and `tests/integration/test_metrics_and_failures.py` |
| FR-016 | README startup and operational walkthrough review; the documented procedures are present |
| FR-017 | Unit, contract, integration, recovery, and topology suites named above; actual infrastructure claims are confined to the tests executed here |
| FR-018 | Real k6 sustained and burst measurements in [load-report-2026-10-23.md](load-report-2026-10-23.md), with raw output and exact-ID reconciliation preserved in the reviewed [evidence package](evidence/load-validation-20261023.md) (archive SHA-256 `44ff30478e7080933ae5f4b0c03fcfb52a4f609d0dca8c85ad8383d57914ed9d`) |

## Previously skipped tests audit

The prior final `pytest -q` skipped 17 tests because external infrastructure variables were
intentionally absent. Fourteen had already been executed in the earlier Compose-backed matrix;
that fact is listed as historical coverage only, not treated as a substitute for the individual
tests. The command below reran all 17 with their required environment, including the public
provider opt-in:

```bash
RUN_EXTERNAL_PROVIDER_TESTS=1 \
RABBITMQ_URL=amqp://async_processing:async_processing@localhost:5672/ \
RABBITMQ_MANAGEMENT_URL=http://localhost:15672 \
DATABASE_URL=postgresql+psycopg://async_processing:async_processing@localhost:5433/async_processing \
pytest -q tests/integration/test_frankfurter_ecb_provider.py \
  tests/integration/test_live_tracing.py tests/integration/test_metrics_and_failures.py \
  tests/integration/test_rabbitmq_consumer.py tests/integration/test_rabbitmq_topology.py \
  tests/integration/test_retry_and_failure_routing.py \
  tests/integration/test_transaction_acceptance.py tests/integration/test_user_queries.py tests/recovery
```

Result: **17 passed** in 154.98 seconds (plus one upstream `TestClient` deprecation warning).
The log and exit code are `.t035-previous-skips.log` and `.t035-previous-skips-exit.log`.

| Former skip count | Test group / requirements | Prior real-matrix coverage | Final audit result |
| --- | --- | --- | --- |
| 1 | Frankfurter ECB business-day and weekend fallback; FR-005–FR-007 | No; external call was intentionally omitted | Passed with `RUN_EXTERNAL_PROVIDER_TESTS=1` |
| 2 | Local Jaeger journey and exporter outage; tracing proposal / Constitution V | Yes, `test_live_tracing.py` | Passed again |
| 1 | Multiprocess metrics and durable failure retention; FR-015, SC-003 | Yes | Passed again |
| 1 | Manual consumer ACK against real RabbitMQ; FR-009–FR-011 | No | Passed |
| 1 | RabbitMQ version/topology/confirm checks; FR-003, FR-009–FR-011 | No | Passed |
| 6 | Retry buckets, outages, permanent classifications; FR-007, FR-009–FR-011, SC-003 | Yes | Passed again |
| 2 | Confirmed HTTP acceptance and duplicate producer submission; FR-001–FR-003, SC-001 | Yes | Passed again |
| 1 | Query visibility and exact read semantics; FR-012–FR-014, SC-004 | Yes | Passed again |
| 2 | SIGKILL recovery boundaries; FR-009–FR-010, FR-017 | Yes | Passed again |

## Unverified or bounded claims

- k6 load measurements were run locally; their host-specific results are not a production
  capacity claim. See [load-report-2026-10-23.md](load-report-2026-10-23.md).
- The optional live Frankfurter provider test was run and passed. Its result validates the
  documented known-date/weekend behavior at that time, not future provider availability.
- This local single-node quorum setup verifies normal container-restart persistence only.
  It does not demonstrate survival of loss of a quorum member or a multi-node outage.
- RabbitMQ publisher confirms prove broker acceptance/routing, not consumer processing or
  exactly-once delivery. Database uniqueness proves idempotent first-write-wins persistence.
- The failure counter measures confirmed forwarding attempts, not unique transactions. A
  crash between forwarding confirmation, metric increment, and ACK can lose or repeat an
  observation while the message remains recoverable.
