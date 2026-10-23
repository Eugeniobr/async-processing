# Implementation Plan: Async Transaction Processing

**Branch**: `001-async-transaction-processing` | **Date**: 2026-10-22 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification for reliable asynchronous transaction processing.

## Summary

Accept valid transactions only after RabbitMQ confirms durable publication; process them at least once; atomically preserve the first record for each transaction ID in PostgreSQL; and expose USD summaries, history, and a failure metric. The design uses small, constructor-injected classes, durable RabbitMQ quorum queues, confirmed retry forwarding, PostgreSQL uniqueness, and a pluggable exchange-rate provider. Detailed decisions and alternatives are in [research.md](research.md).

## Technical Context

**Language/Version**: Python 3.12 (confirmed project language; version is a new proposal)

**Primary Dependencies**: FastAPI for HTTP validation and documentation, Pydantic for request validation, aio-pika for AMQP, SQLAlchemy with psycopg for PostgreSQL, HTTPX for rate retrieval, Prometheus client for metrics (new proposals; all behind small application interfaces where appropriate)

**Storage**: PostgreSQL (confirmed); RabbitMQ durable quorum queues retain work, retry, and permanent-failure messages (confirmed broker; queue type is a new proposal)

**Testing**: pytest unit tests; Docker Compose-backed integration and recovery tests; k6 load measurement (new proposals)

**Target Platform**: Linux containers started locally with Docker Compose (containers confirmed; runtime target is a new proposal)

**Project Type**: Single asynchronous web service with a separately started consumer process

**Performance Goals**: Measure sustained acceptance/processing near 100 events/s and bursts of 1,000 events/s; do not claim unmeasured capacity (confirmed target)

**Constraints**: At-least-once delivery; first persisted record wins; Decimal arithmetic; UTC timestamps; confirmed publication/forwarding before acknowledgement; retry delays of 1, 2, 4, 8, 16, 32, then 60 seconds; no process-local retry timer (confirmed)

**Scale/Scope**: Small interview-scale service; ingestion, conversion, persistence, user queries, and one operational metric only. Accounts, authorization, notifications, and non-USD reporting are excluded.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Pre-research gate — PASS**

| Constitution requirement | Plan response |
| --- | --- |
| Specification before implementation | This plan derives from [spec.md](spec.md); no application code or implementation tasks are created. |
| Simple OOP, Repository, constructor injection | The workflow has six focused classes and two small ports; endpoint functions only validate and delegate. |
| Durable acceptance and idempotency | Confirmed persistent publication, manual acknowledgements, quorum queues, confirmed retry forwarding, and a unique database key provide at-least-once, first-write-wins behavior. |
| Explicit money and time rules | The plan fixes Decimal storage limits, `ROUND_HALF_EVEN`, UTC event dates, rate fallback, interval boundaries, and ordering. |
| Verification | Unit, integration, recovery, and measured-load strategies trace to feature requirements. |

**Post-design gate — PASS**: [research.md](research.md), [data-model.md](data-model.md), [contracts/openapi.yaml](contracts/openapi.yaml), and [quickstart.md](quickstart.md) resolve the choices needed before implementation. The only operational limitation is intentional: accepted work depends on a functioning RabbitMQ quorum and configured durable queues; the README and load report must state the tested topology and limits.

## Confirmed Decisions and New Proposals

### Confirmed by constitution or feature specification

- Python service; RabbitMQ broker; PostgreSQL persistence; local Docker Compose.
- At-least-once delivery, idempotent persistence, and database-enforced transaction-ID uniqueness; first persisted record wins.
- Decimal money, UTC timestamps, event-date rate reference, stored applied rate and date, inclusive `from`/exclusive `to`, deterministic `(timestamp, id)` history ordering.
- Retry delays of 1, 2, 4, 8, 16, 32, then 60 seconds, with persistent retry state.
- Approximately 100 events/s with bursts to 1,000 events/s must be measured.

### New proposals adopted by this plan

- Use RabbitMQ durable **quorum queues** for work, seven fixed-delay retry buckets, and permanent failures. Use publisher confirms, `mandatory` routing, persistent messages, manual acknowledgements, and bounded consumer prefetch (initial benchmark value: 100).
- Pin local RabbitMQ to `rabbitmq:4.3.6-management`. Work, retry, and failure queues use `x-queue-type=quorum`, durable/non-exclusive declaration, `overflow=reject-publish`, `max-length=100000`, and `delivery-limit=-1`; retry queues additionally use `dead-letter-strategy=at-least-once`, `dead-letter-exchange=transactions`, `dead-letter-routing-key=work`, and their fixed `message-ttl`; `stream_queue` MUST be enabled. Application publishes are persistent, mandatory, and publisher-confirmed, while consumers use manual acknowledgement. These are new operational proposals that preserve the approved at-least-once behavior.
- Use Frankfurter v2 pinned to the ECB provider for daily historical rates; retain an `ExchangeRateProvider` port so a direct ECB adapter can replace it.
- Query the UTC event date deterministically: request `requested_date`, then request each preceding calendar date through day seven only when the prior request returns a documented no-rate response. A 200 response is accepted only when its positive finite quote reports `rate_date <= requested_date` and no older than seven days; persist `requested_date` and returned `rate_date`. Never use a future rate. Transport, timeout, 429, and 5xx responses are retryable; malformed data, a future request, and exhaustion of the eight-date search are permanent failures.
- Support USD plus codes in committed `config/currencies-v1.json`, a reviewed snapshot of Frankfurter's ECB coverage. The snapshot records provider, source URL, retrieval date, and codes; its version travels in the durable envelope and persisted transaction. Deployment rejects a malformed or duplicate-code snapshot. A candidate update is validated against the provider for a known business date in staging, reviewed, and version-bumped before deployment. An input outside the accepted snapshot is rejected at ingestion; a provider coverage change after acceptance produces a durable `provider_coverage_changed` failure record, never silent loss.
- Store input and USD amounts as `NUMERIC(24,8)`, rates as `NUMERIC(24,12)`, and round the rate and converted amount to their stored scales with `ROUND_HALF_EVEN`. Reject a validly shaped input that cannot fit the amount range instead of truncating it.
- Use one service process for HTTP ingestion/query and one consumer process; both compose the same application classes.

## Simple Processing Workflow

1. The ingestion endpoint validates the transaction, normalizes its timestamp to UTC, and asks `TransactionPublisher` to publish a persistent envelope to the durable work queue.
2. The publisher treats publication as accepted only after a positive publisher confirm and no mandatory-routing return. A negative or missing confirm returns temporary unavailability; a producer may retry, creating a harmless duplicate.
3. `TransactionConsumer` receives with manual acknowledgement and bounded prefetch, then calls `TransactionProcessor`.
4. The processor obtains a USD rate (or uses 1 for USD), applies the documented Decimal rules, and calls `TransactionRepository.record_first`.
5. The repository performs one atomic insert under the transaction-ID unique constraint. Inserted means success; conflict means an already-persisted transaction and preserves the first record. In either case the consumer acknowledges after the database result commits.
6. On a temporary provider or database failure, the consumer publishes the unchanged envelope plus incremented attempt metadata to the selected fixed-delay retry bucket, waits for that publish confirm, then acknowledges the original delivery. The bucket expires it back to work after its fixed delay.
7. On a permanent validation/rate failure, the consumer confirms durable forwarding to the failure queue before acknowledging the original. Operators inspect or replay that durable record.

## Classes, Responsibilities, and Interfaces

| Component | Responsibility | Constructor dependencies / public interface |
| --- | --- | --- |
| `TransactionPublisher` | Build a versioned envelope and safely publish it to the work or retry route. | AMQP connection/channel factory, topology settings. `publish_work(event)`, `publish_retry(envelope, delay_seconds)`, `publish_failure(envelope, reason)`. Each returns only after confirm and routing success. |
| `TransactionConsumer` | Receive manually acknowledged deliveries and make the acknowledgement decision. | `TransactionProcessor`, `TransactionPublisher`, consumer channel. `consume_forever()`, `handle(delivery)`. |
| `TransactionProcessor` | Coordinate rate retrieval, Decimal conversion, persistence, and failure classification. No queue protocol operations. | `ExchangeRateProvider`, `TransactionRepository`, `MoneyConverter`. `process(event) -> Processed | Duplicate | RetryableFailure | PermanentFailure`. |
| `MoneyConverter` | Apply the USD-per-unit convention, scale limits, and `ROUND_HALF_EVEN`. | No dependencies. `convert(amount, rate) -> converted_amount`. |
| `FrankfurterEcbRateProvider` | Retrieve and validate a daily base-to-USD rate and resolve the documented date fallback. | HTTP client, clock/configuration. Implements `ExchangeRateProvider.get_usd_rate(currency, requested_date) -> RateQuote`. |
| `PostgresTransactionRepository` | Persist the first transaction atomically and supply summary/history reads. | Database session factory. Implements `record_first(record)`, `get_summary(user_id)`, `list_history(query)`. |
| `MetricsRecorder` | Increase counters at well-defined outcomes without owning business logic. | Metrics registry. `record_processing_failure(kind)`, `record_retry(delay)`, `record_permanent_failure(reason)`. |

Ports remain narrow:

- `ExchangeRateProvider`: return either a validated `RateQuote`, a retryable availability error, or a permanent unsupported/missing-rate error.
- `TransactionRepository`: return `Inserted` or `AlreadyExists`; it never overwrites the existing row.

No generic base classes, service locator, event bus, or repository-per-entity abstraction is proposed. FastAPI route functions validate/translate HTTP and delegate to these classes.

## Messaging and Persistence Design

### RabbitMQ topology

- Durable direct exchange `transactions`; durable quorum queue `transactions.work` bound with routing key `work`.
- Durable direct exchange `transactions.retry`; seven durable quorum queues `transactions.retry.1s`, `.2s`, `.4s`, `.8s`, `.16s`, `.32s`, and `.60s`, each with its fixed queue TTL and dead-letter route back to `transactions:work`.
- Durable quorum queue `transactions.failure` receives permanent failures. Each envelope contains `schema_version`, immutable event payload, `attempt`, and original acceptance metadata. This is the persistent retry state.
- RabbitMQ is pinned to `rabbitmq:4.3.6-management`. Work, retry, and failure queues are durable, non-exclusive, quorum (`x-queue-type=quorum`) and use `overflow=reject-publish`, `max-length=100000`, and `delivery-limit=-1`. Retry queues additionally use `dead-letter-strategy=at-least-once`, `dead-letter-exchange=transactions`, `dead-letter-routing-key=work`, and their fixed `message-ttl`; the `stream_queue` feature flag is enabled. `delivery-limit=-1` on both work and retry queues prevents RabbitMQ's default delivery limit from silently ending the approved indefinite retry schedule. The max length causes a new work/retry/failure publish to be negatively confirmed; the source delivery is therefore left unacknowledged and recoverable.
- Work, retry, and failure messages are persistent. Publisher-confirm mode and mandatory routing are enabled for application publishing. Quorum queues are selected because they offer safer at-least-once dead lettering than ordinary DLX forwarding. Compose is a one-node local environment, so its confirms protect a single durable node only; the documented replicated-quorum claim applies only to a multi-node deployment with a live majority.
- The initial prefetch proposal is 100 unacknowledged deliveries per consumer; tune only after the required load measurement. The service does not use `nack(requeue=true)`, because it can cause a hot redelivery loop.

### PostgreSQL and concurrency

- `transactions.transaction_id` is a unique constraint. `record_first` issues a single `INSERT ... ON CONFLICT DO NOTHING RETURNING` statement inside the database transaction—never a prior lookup followed by an insert.
- The inserted transaction contains all conversion facts. A conflict returns `AlreadyExists`; conflicting duplicate payloads are neither compared for replacement nor updated. This implements first-write-wins under concurrent consumers.
- The consumer acknowledges only after the transaction commits. A crash before acknowledgement causes redelivery, which resolves as `AlreadyExists` and is then acknowledged.
- A crash after confirmed retry publication but before acknowledgement can create a duplicate retry delivery, never a loss: the original is redelivered and may be forwarded again. Both envelopes retain the same transaction ID and resolve through the unique insert; recovery testing proves this boundary.

## HTTP and Observability Contracts

The normative external contract is [contracts/openapi.yaml](contracts/openapi.yaml).

- `POST /transactions` returns `202` only after confirmed durable routing; `422` for invalid input; `503` when durable acceptance cannot be confirmed.
- `GET /users/{user_id}/summary` returns the USD total and processed count, including zero values for no records.
- `GET /users/{user_id}/transactions` accepts UTC `from` (inclusive), `to` (exclusive), `limit` (default 50, maximum 100), and nonnegative `offset`; results use ascending `(timestamp, transaction_id)` order.
- `GET /metrics` exposes Prometheus text. Required metric: `transaction_processing_failures_total`, labelled by `kind` (`retryable` or `permanent`). It may also expose retry and successful-processing counters.

## Verification Strategy

| Level | Evidence | Requirements covered |
| --- | --- | --- |
| Unit | Decimal conversion/rounding, UTC-date selection, missing-date policy, input validation, and duplicate result handling using fakes. | FR-001, FR-002, FR-004–FR-008 |
| Integration | Docker Compose RabbitMQ/PostgreSQL tests for confirmed accept, two concurrent same-ID deliveries, summary/history queries, retry routing, and permanent failure forwarding. | FR-003, FR-008–FR-015, FR-017 |
| Recovery | Stop a consumer after database commit but before acknowledgement; restart it and prove one record remains. Stop provider/database dependencies, recover them, and prove accepted messages are retried without loss. | FR-009–FR-011, FR-017 |
| Load measurement | Drive valid requests at ~100 events/s sustained and 1,000-event bursts. Record duration, accepted/rejected outcomes, processing completion, duplicate count, queue depth, and failure metric; publish limits rather than extrapolating. | FR-018, SC-005 |

## Project Structure

### Documentation (this feature)

```text
specs/001-async-transaction-processing/
├── plan.md              # This file ($speckit-plan command output)
├── research.md          # Phase 0 output ($speckit-plan command)
├── data-model.md        # Phase 1 output ($speckit-plan command)
├── quickstart.md        # Phase 1 output ($speckit-plan command)
├── contracts/           # Phase 1 output ($speckit-plan command)
└── tasks.md             # Phase 2 output ($speckit-tasks command - NOT created by $speckit-plan)
```

### Source Code (repository root)
```text
src/
├── api/                # HTTP routes and request/response translation
├── application/        # Processor and money-conversion use cases
├── domain/             # Immutable transaction and rate value objects
├── infrastructure/     # RabbitMQ, PostgreSQL, rate-provider adapters
└── composition/        # Explicit constructor wiring for API and consumer

tests/
├── contract/
├── integration/
├── recovery/
└── unit/
```

**Structure Decision**: A single service repository keeps the interview walkthrough small. The dependency direction is API/infrastructure → application → domain. `infrastructure` implements the two ports; `composition` is the only assembly location. These paths are planned, not yet created.

## Complexity Tracking

No constitution violations require complexity tracking.
