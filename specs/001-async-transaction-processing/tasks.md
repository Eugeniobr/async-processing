# Tasks: Async Transaction Processing

**Input**: [spec.md](spec.md), [plan.md](plan.md), [research.md](research.md), [data-model.md](data-model.md), [contracts/openapi.yaml](contracts/openapi.yaml), and [quickstart.md](quickstart.md)

**Tests**: Unit tests for conversion and deduplication, plus Docker Compose-backed integration, recovery, and load checks are required by FR-017 and FR-018.

**Organization**: Tasks are ordered in small, demonstrable milestones. Story tasks carry their user-story label; shared foundations intentionally do not.

## Format: `[ID] [P?] [Story] Description`

- `[P]` means the task can proceed in parallel after its stated dependencies are complete.
- Every task identifies its deliverable, affected files, requirements, dependencies, and verification in its description.

## Phase 1: Setup — Reproducible Local Workspace

**Purpose**: Establish an explainable Python service skeleton and pinned local infrastructure without implementing business behavior.

- [X] T001 Create the Python 3.12 project skeleton and dependency groups in `pyproject.toml`, `src/{api,application,domain,infrastructure,composition}/__init__.py`, and `tests/{unit,contract,integration,recovery}/__init__.py`; deliverable: importable empty packages; refs: plan Technical Context; depends on: none; verify: dependency installation and package imports succeed.
- [X] T002 [P] Define local service configuration and `config/currencies-v1.json` in `src/composition/settings.py`, `.env.example`, `config/currencies-v1.json`, and `README.md`; include provider/source/retrieval date, unique uppercase codes including USD, allow-list version, provider URL, database URL, broker URL, prefetch, and rate-fallback window; deliverable: versioned configuration surface; refs: FR-001, FR-007, FR-016; depends on: T001; verify: malformed/duplicate-code snapshots or absent USD fail startup, and an input outside the snapshot is rejected before publisher invocation.
- [X] T003 [P] Create pinned local dependencies in `compose.yaml`, `docker/rabbitmq/definitions.json`, and `docker/rabbitmq/rabbitmq.conf`; pin `rabbitmq:4.3.6-management`, enable `stream_queue`, configure durable non-exclusive quorum work/retry/failure queues with `overflow=reject-publish`, `max-length=100000`, and `delivery-limit=-1`, and add retry-only `dead-letter-strategy=at-least-once`, `dead-letter-exchange=transactions`, `dead-letter-routing-key=work`, and exact 1/2/4/8/16/32/60-second TTLs; deliverable: versioned durable-retry topology; refs: FR-003, FR-009–FR-011; depends on: T001; verify: Compose starts PostgreSQL/RabbitMQ 4.3.6, policy inspection shows every required setting on the correct queues, and retry buckets expose the exact TTLs.
- [X] T004 [P] Add development quality and test commands in `pyproject.toml`, `README.md`, and `.github/workflows/ci.yml`; deliverable: formatting, lint, unit, integration, recovery, and contract-test command entry points; refs: FR-017; depends on: T001; verify: CI discovers the test directories without application tests being skipped.

**Checkpoint**: A reviewer can start only the local dependencies, inspect a pinned broker topology, and see the planned code/test layout.

---

## Phase 2: Foundational — Contracts, Domain Rules, and Durable Boundaries

**Purpose**: Build the shared rules and infrastructure that every user story needs. No user-story endpoint or consumer work begins until this phase is complete.

- [X] T005 Define immutable transaction, rate quote, retry-envelope, and processing-outcome value objects in `src/domain/transaction.py`, `src/domain/rates.py`, and `src/domain/outcomes.py`; quote these constraints verbatim: `transaction_id` is “Nonempty global idempotency key”, `original_amount` is “numeric(24,8)”, `currency` is “Uppercase configured provider-supported code”, `attempt` “Starts at zero and increases on each confirmed retry forwarding”, and `currency_allowlist_version` is immutable through retries; deliverable: shared validated vocabulary; refs: FR-001, FR-004–FR-009; depends on: T001, T002; verify: unit construction tests accept zero/negative amounts and reject empty IDs, unsupported currencies, and malformed timezone-less timestamps.
- [X] T006 [P] Define constructor-injected ports in `src/application/ports.py` for `ExchangeRateProvider` and `TransactionRepository`, including explicit `Inserted`, `AlreadyExists`, retryable, and permanent outcomes; deliverable: narrow interfaces with no generic base layer; refs: FR-008–FR-011 and Constitution II; depends on: T005; verify: fake implementations can satisfy every port in a unit test.
- [X] T007 [P] Implement Decimal conversion and range validation in `src/application/money_converter.py` and `tests/unit/test_money_converter.py`; apply `USD amount = original amount × USD per unit`, `ROUND_HALF_EVEN`, input/USD `NUMERIC(24,8)`, rate `NUMERIC(24,12)`, and reject values outside absolute `9,999,999,999,999,999.99999999`; deliverable: conversion unit suite; refs: FR-005–FR-006, SC-001; depends on: T005; verify: test USD rate 1, negative/zero values, half-even boundaries, 8/12 decimal quantization, and input/result overflow rejection.
- [X] T008 [P] Create the PostgreSQL schema and migration in `migrations/versions/001_transactions.py` and `src/infrastructure/postgres_schema.py`; include unique `transaction_id`, `numeric(24,8)` amounts, `numeric(24,12)` rate, UTC timestamp fields, requested/returned rate dates, `currency_allowlist_version`, and index `(user_id, event_timestamp, transaction_id)`; deliverable: inspectable schema; refs: FR-004, FR-006, FR-008, FR-012, FR-013, FR-014; depends on: T001; verify: migration applies to Compose PostgreSQL and rejects a duplicate transaction ID.
- [X] T009 Implement `PostgresTransactionRepository` in `src/infrastructure/postgres_repository.py` and its focused unit tests in `tests/unit/test_postgres_repository.py`; use one `INSERT ... ON CONFLICT DO NOTHING RETURNING` transaction and never update a conflicting row; deliverable: atomic first-write-wins repository; refs: FR-008 and Constitution III; depends on: T006, T008; verify: concurrent calls with the same ID yield exactly one `Inserted`, one or more `AlreadyExists`, and preserve the first payload.
- [X] T010 [P] Implement `FrankfurterEcbRateProvider` in `src/infrastructure/frankfurter_ecb.py`, deterministic tests in `tests/unit/test_frankfurter_ecb.py`, and external-provider checks in `tests/integration/test_frankfurter_ecb_provider.py`; request the UTC date then only decrement on 404/no-rate through day seven, accept only a correct positive finite quote with `rate_date <= requested_date`, classify timeout/connection/429/5xx as retryable, and classify malformed/mismatched/future/exhausted responses as permanent; deliverable: deterministic retrieval policy plus live-provider evidence; refs: FR-005, FR-006, FR-007; depends on: T005, T006, T002; verify: fakes cover all eight date candidates and errors, while the external test verifies one known ECB business date and weekend fallback against the actual provider.
- [X] T011 Implement RabbitMQ topology verification and confirmed publisher adapter in `src/infrastructure/rabbitmq_topology.py`, `src/infrastructure/rabbitmq_publisher.py`, and `tests/integration/test_rabbitmq_topology.py`; require RabbitMQ 4.3.6, enabled `stream_queue`, persistent messages, durable non-exclusive quorum queues, `mandatory` routing, publisher confirms, manual consumer acknowledgements, work/retry/failure `reject-publish`/`max-length=100000`/`delivery-limit=-1`, and retry-only `at-least-once` DLX settings; deliverable: fail-fast topology/publish boundary; refs: FR-003, FR-009–FR-011; depends on: T003, T005; verify: policy/feature-flag inspection fails on any missing setting, returned/unconfirmed publishes are not accepted, and each retry queue returns to `transactions.work` after its configured delay.
- [X] T012 [P] Add shared FastAPI application assembly, error translation, structured logging, and `transaction_processing_failures_total{kind}` metric registration in `src/composition/container.py`, `src/api/app.py`, `src/api/errors.py`, and `src/infrastructure/metrics.py`; deliverable: constructor-only wiring and observable failures; refs: FR-015 and Constitution II; depends on: T002, T005, T006; verify: an app smoke test exposes `/metrics` and maps domain validation/temporary errors consistently.

**Checkpoint**: Decimal, rate-date, deduplication, broker topology, and port boundaries are independently verified before any user-facing workflow is added.

---

## Phase 3: User Story 1 — Submit a Transaction for Reliable Processing (Priority: P1) 🎯 MVP

**Goal**: A producer can submit a valid transaction and receives `202` only after confirmed durable routing; invalid input never enters work.

**Independent Test**: Use `POST /transactions` to submit a valid event and inspect the work queue; submit invalid input and an intentionally unavailable/unroutable broker path, expecting `422` and `503` respectively with no false acceptance.

- [X] T013 [P] [US1] Write the API contract tests in `tests/contract/test_submit_transaction.py` for `POST /transactions`; cover `202` only after confirm, `422` before publication for invalid fields, and `503` when a positive confirm/routing result is absent; deliverable: executable FR-001–FR-003 contract checks; refs: US1 scenarios 1–3, FR-001–FR-003; depends on: T011, T012; verify: tests fail against route stubs before route behavior is added.
- [X] T014 [US1] Implement request parsing and UTC normalization in `src/api/transactions.py`; enforce nonempty IDs, decimal-string amount with at most eight fractional places and bounded `NUMERIC(24,8)` range, configured uppercase currency, and timezone-aware ISO 8601 timestamp; deliverable: validated `TransactionEvent`; refs: US1 scenarios 1–2, FR-001, FR-002, FR-004; depends on: T005, T012; verify: contract tests accept valid zero/negative transactions and reject each invalid field before publisher invocation.
- [X] T015 [US1] Wire `POST /transactions` through `src/api/routes.py` and `src/composition/container.py` to `TransactionPublisher.publish_work`; return `202` only after positive confirm and no mandatory-return, otherwise `503`; deliverable: durable acceptance endpoint; refs: US1 scenarios 1–3, FR-003, SC-001; depends on: T013, T014, T011; verify: Compose integration test confirms a `202` event exists in the durable work queue and a publisher failure never yields `202`.
- [X] T016 [US1] Add acceptance-path integration checks in `tests/integration/test_transaction_acceptance.py`; include confirm loss/retry safety and broker restart behavior without claiming exactly-once publication; deliverable: demonstrated at-least-once acceptance boundary; refs: FR-003, FR-010, SC-001; depends on: T015; verify: retried producer submissions may duplicate broker deliveries but retain the same transaction ID for downstream idempotency.

**Checkpoint**: US1 is independently demoable: a valid request is safely accepted, while validation or broker failure is truthfully reported.

---

## Phase 4: User Story 2 — Process Each Transaction Once Despite Failures (Priority: P1)

**Goal**: The consumer converts and persists each accepted transaction once, handles duplicates, and safely forwards temporary/permanent failures.

**Independent Test**: Deliver a valid message, concurrent duplicate messages, a temporary dependency failure, and a permanent rate/envelope failure; verify one persisted transaction or durable failure retention as appropriate.

- [X] T017 [P] [US2] Write processor unit tests in `tests/unit/test_transaction_processor.py` using port fakes; cover USD rate 1, stored requested/returned dates, first-write-wins conflict, retryable provider/database failure, and permanent no-eligible-rate outcome; deliverable: processor behavior specification; refs: US2 scenarios 1–4, FR-005–FR-010; depends on: T007, T009, T010; verify: tests fail before the processor is implemented.
- [X] T018 [US2] Implement `TransactionProcessor` in `src/application/transaction_processor.py`; call the rate provider, `MoneyConverter`, and `TransactionRepository.record_first` through constructor injection, returning only explicit processing outcomes; deliverable: queue-agnostic processing coordination; refs: FR-005–FR-009 and Constitution II–IV; depends on: T017; verify: processor unit suite proves Decimal conversion and duplicate results do not overwrite the first record.
- [X] T019 [US2] Implement manual-ack consumer and confirmed retry/failure forwarding in `src/infrastructure/rabbitmq_consumer.py` and `src/composition/consumer_main.py`; on temporary failure increment durable `attempt`, publish to the selected retry bucket, wait for confirm, then acknowledge the original; on permanent failure confirm failure-queue forwarding before acknowledgement; deliverable: no acknowledged loss window; refs: US2 scenarios 4–5, FR-009–FR-011; depends on: T011, T018, T012; verify: fault injection proves the original remains unacknowledged if forwarding is returned/nacked/connection-lost before confirmation, and a crash after confirmation before acknowledgement produces only idempotent duplicate delivery, never loss.
- [X] T020 [US2] Add concurrent-deduplication integration coverage in `tests/integration/test_concurrent_deduplication.py`; publish concurrent same-ID deliveries with conflicting payloads; deliverable: real PostgreSQL/RabbitMQ proof of first-write-wins; refs: US2 scenario 3, FR-008, FR-017, SC-002; depends on: T019; verify: exactly one row survives and it matches the first successful database commit.
- [X] T021 [US2] Add retry and permanent-failure integration coverage in `tests/integration/test_retry_and_failure_routing.py`; simulate provider/database outages, recovery, malformed envelopes, missing historical rates beyond seven days, unsupported pairs, and a post-acceptance `provider_coverage_changed` response; deliverable: durable failure classification proof; refs: US2 scenario 4, US4 scenario 1, FR-007, FR-009–FR-011, FR-017, SC-003; depends on: T019; verify: temporary outages lead through 1/2/4/8/16/32/60-second buckets without loss, permanent inputs land in the failure queue with their allow-list version, and no hot requeue occurs.
- [X] T022 [US2] Add recovery seams/tests in `src/infrastructure/rabbitmq_consumer.py`, `tests/recovery/test_commit_before_ack.py`, and `tests/recovery/test_confirmed_retry_before_ack.py`; stop the consumer after database commit before acknowledgement and separately after retry publication confirmation before acknowledgement, then restart it; deliverable: both acknowledgement-boundary proofs; refs: US2 scenario 5, FR-009, FR-010, FR-017; depends on: T019; verify: commit redelivery resolves as `AlreadyExists`, confirmed-retry redelivery may create duplicate retry envelopes, and both paths leave exactly one persisted transaction with no event loss.

**Checkpoint**: US2 is independently demoable with real infrastructure: duplicates, outages, forwarding-confirm failures, and consumer interruption cannot silently lose an accepted event.

---

## Phase 5: User Story 3 — Review a User's Processed Transactions (Priority: P2)

**Goal**: A viewer can retrieve a processed user's summary and deterministically paginated history.

**Independent Test**: Seed processed records for two users and query one user's summary/history across interval boundaries and pagination values.

- [X] T023 [P] [US3] Write summary and history contract tests in `tests/contract/test_user_queries.py`; cover zero results, user isolation, `from <= timestamp < to`, `(timestamp, transaction_id)` order, default 50, maximum 100, nonnegative offset, and invalid parameters; deliverable: executable query contract; refs: US3 scenarios 1–5, FR-012, FR-013, FR-014; depends on: T008, T012; verify: tests fail against placeholder routes before query behavior is added.
- [X] T024 [US3] Implement repository summary/history reads in `src/infrastructure/postgres_repository.py` and `tests/unit/test_transaction_queries.py`; calculate summary only from persisted rows and use the documented range/order/pagination; deliverable: deterministic query repository behavior; refs: FR-012, FR-013, FR-014; depends on: T009, T023; verify: two-user fixture returns zero/count-zero for no rows and excludes the exclusive `to` boundary.
- [X] T025 [US3] Implement query request parsing and routes in `src/api/users.py` and `src/api/routes.py`; return decimal strings and translate invalid bounds/limits/offsets to `422`; deliverable: `/users/{user_id}/summary` and `/users/{user_id}/transactions` contract; refs: US3 scenarios 1–5, FR-012, FR-013, FR-014, SC-004; depends on: T024, T012; verify: contract suite passes against Compose-backed persisted data.
- [X] T026 [US3] Add end-to-end query integration checks in `tests/integration/test_user_queries.py`; deliverable: verified query visibility after successful processing only; refs: FR-012, FR-013, FR-014, SC-004; depends on: T025, T019; verify: a newly accepted but unprocessed event is absent, then appears exactly once after processing.

**Checkpoint**: US3 is independently demoable using persisted records, without relying on a separate counter or exposing pending work as completed data.

---

## Phase 6: User Story 4 — Investigate Processing Health and Failures (Priority: P3)

**Goal**: An operator can inspect processing failure counts and durable permanent failures.

**Independent Test**: Trigger one retryable and one permanent failure, then inspect the failure queue and `/metrics` output.

- [X] T027 [P] [US4] Write metrics and failure-observability contract tests in `tests/contract/test_metrics.py`; require `transaction_processing_failures_total` with `retryable` and `permanent` kinds; deliverable: observable-failure contract; refs: US4 scenarios 1–2, FR-015; depends on: T012; verify: tests fail before consumer outcome instrumentation is added.
- [X] T028 [US4] Instrument processor/consumer outcomes in `src/application/transaction_processor.py`, `src/infrastructure/rabbitmq_consumer.py`, and `src/infrastructure/metrics.py`; increment retry/permanent counters only when the corresponding failure outcome has been classified and durable forwarding is confirmed; deliverable: no misleading completed-failure metric; refs: US4 scenarios 1–2, FR-011, FR-015; depends on: T019, T027; verify: the same forced failures from T021 create matching metric labels and a durable failure record.
- [X] T029 [US4] Expose the metrics endpoint through `src/api/routes.py` and add Compose integration coverage in `tests/integration/test_metrics_and_failures.py`; deliverable: operator-readable metric endpoint and failure-queue evidence; refs: FR-015, SC-003; depends on: T028, T021; verify: `/metrics` includes both labels after induced failures, and permanent failures remain inspectable after consumer restart.
- [X] T034 [P] Add local live transaction tracing in `compose.yaml`, `pyproject.toml`, `src/infrastructure/tracing.py`, `src/api/transactions.py`, `src/infrastructure/rabbitmq_publisher.py`, `src/infrastructure/rabbitmq_consumer.py`, `src/infrastructure/frankfurter_ecb.py`, `src/infrastructure/postgres_repository.py`, and `tests/integration/test_live_tracing.py`; export OpenTelemetry spans to a pinned Jaeger UI with `transaction_id`, stage, attempt, and outcome only; deliverable: development-only transaction journey view; refs: user-requested observability proposal and Constitution V; depends on: T015, T019; verify: one accepted transaction produces one searchable trace across API, broker, conversion, and persistence, while an exporter outage does not change acceptance, acknowledgement, or retry behavior.

**Checkpoint**: US4 is independently demoable: permanent failures are retained, and operators can distinguish retryable from permanent processing failures.

---

## Phase 7: Polish and Cross-Cutting Verification

**Purpose**: Complete delivery evidence without expanding product scope.

- [X] T030 [P] Document startup, RabbitMQ queue rationale, the fixed-TTL retry trade-off, rate-provider/date policy, currency allow-list, Decimal limits/rounding, and ten-times-load changes in `README.md`; deliverable: English interview walkthrough; refs: FR-016 and Constitution constraints; depends on: T003, T010, T011, T029; verify: an independent reviewer can follow the README and identify the required rationale and trade-off.
- [X] T031 [P] Create reproducible load scenarios and report template in `tests/load/transactions.js` and `docs/load-report-template.md`; exercise sustained ~100 events/s and a 1,000-event burst, recording duration, outcomes, completion, duplicate count, queue depth, failure metric, topology/version, and limitations; deliverable: measured-capacity procedure; refs: FR-018, SC-005; depends on: T015, T019, T029; verify: the scenario validates its target rate inputs and emits the listed measurements.
- [X] T032 Run the quickstart acceptance matrix and record executed checks/limitations in `docs/verification-report.md`; cover conversion, concurrent duplicates, temporary downstream outages, retry-forwarding confirm loss, consumer recovery, queries, metrics, and load results; deliverable: traceable evidence report; refs: FR-017–FR-018, SC-001–SC-006; depends on: T016, T020–T022, T026, T029, T030, T031; verify: every reported claim links to a test result or is explicitly listed as unverified.
- [X] T033 Preserve the historical baseline and perform a final artifact traceability review in `docs/specification-v0.1.md`, `specs/001-async-transaction-processing/spec.md`, and `docs/verification-report.md`; deliverable: untouched v0.1 baseline plus requirement-to-evidence map; refs: Constitution I and V; depends on: T032; verify: `git diff -- docs/specification-v0.1.md` is empty and all FR-001–FR-018 have recorded evidence.

## Dependencies and Execution Order

```text
Phase 1 Setup
  → Phase 2 Foundational
    → US1 durable acceptance (MVP)
      → US2 processing, retries, deduplication, and recovery
        → US3 processed-data queries
        → US4 metrics and failure observability
          → Phase 7 documentation, load measurement, final evidence
```

- **US1** depends on the confirmed-publish topology and shared domain validation (T005, T011, T012). It is the minimum demoable increment.
- **US2** depends on US1's accepted message envelope and the foundational processor/repository/rate rules.
- **US3** depends on persisted processed data from US2 for its end-to-end visibility check, though its repository/query contract work may begin after Phase 2.
- **US4** depends on US2 failure outcomes and may run in parallel with US3 after T019.
- Phase 7 only reports behavior proven by completed prior tasks.

## Parallel Opportunities

- After T001, T002–T004 can run in parallel.
- After T005, T006–T008 and T010 can run in parallel; T009 follows the schema and T011 follows broker configuration.
- After Phase 2, T013/T014, T017, T023, and T027 can be prepared in parallel, but implementation follows the dependencies recorded in each task.
- After T019, US3 query work and US4 observability work can progress in parallel.

## Incremental Interview Milestones

1. **Foundation**: Explain the two ports, exact money/date rules, unique insert, and RabbitMQ topology.
2. **MVP acceptance**: Demonstrate truthful `202`/`422`/`503` behavior and confirmed publication.
3. **Reliability increment**: Demonstrate first-write-wins, retry-forwarding confirmation before acknowledgement, and restart recovery.
4. **Read increment**: Demonstrate deterministic processed-data queries.
5. **Operations increment**: Demonstrate durable permanent failures, metrics, and measured load rather than assumed capacity.

## Implementation Strategy

Implement and verify phases sequentially through the US2 reliability checkpoint before presenting any durability claim. US3 and US4 may then progress in parallel. Do not mark a load, recovery, or outage guarantee complete until its actual Compose-backed check is recorded in `docs/verification-report.md`.
