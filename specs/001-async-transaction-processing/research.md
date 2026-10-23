# Phase 0 Research: Async Transaction Processing

All choices below are resolved for planning. “Confirmed” means the constitution or feature specification already requires it; “proposal” is adopted by this plan and can be revisited through a documented specification change.

## Message Reliability and Retries

**Decision (confirmed + proposal)**: Pin local execution to `rabbitmq:4.3.6-management`, with persistent messages, publisher confirms, mandatory routing, manual consumer acknowledgements, bounded prefetch, and seven durable quorum retry queues (1, 2, 4, 8, 16, 32, 60 seconds). Work, retry, and failure queues are non-exclusive and use `x-queue-type=quorum`, `overflow=reject-publish`, `max-length=100000`, and `delivery-limit=-1`; retry queues additionally set `dead-letter-strategy=at-least-once`, `dead-letter-exchange=transactions`, `dead-letter-routing-key=work`, and their fixed TTL; `stream_queue` is enabled. Retry metadata travels in the durable message envelope. On a temporary failure, publish to the selected retry queue and wait for its confirm before acknowledging the original. Permanent failures are confirmed to a durable failure queue.

**Rationale**: RabbitMQ 4.3 documents publisher confirms as quorum replication acknowledgement and manual acknowledgements as the at-least-once safety mechanism. Its quorum-DLX documentation requires `dead-letter-strategy=at-least-once`, `overflow=reject-publish`, a dead-letter exchange, and `stream_queue`; it also explains that the source retains messages until target confirmation and duplicates remain possible. `delivery-limit=-1` on work and retry queues is an explicit preservation of the approved unbounded 60-second retry schedule, rather than RabbitMQ's default redelivery cutoff. [RabbitMQ 4.3 quorum queues](https://www.rabbitmq.com/docs/4.3/quorum-queues), [RabbitMQ reliability guide](https://www.rabbitmq.com/docs/reliability), and the [official 4.3.6 image tags](https://github.com/docker-library/docs/blob/master/rabbitmq/README.md) support this design.

**Operational limit**: The Compose environment is a single RabbitMQ node, so a confirmed local message survives process restart and disk persistence but does not demonstrate tolerance of losing a quorum member. Multi-node high-availability claims require a separately tested deployment with a live majority.

**Alternatives considered**:

- Delayed-message plugin: rejected because RabbitMQ lists it as no longer maintained.
- One queue with per-message TTL: rejected because expired messages can wait behind the head of the queue, making later retry timing less predictable. [RabbitMQ TTL documentation](https://www.rabbitmq.com/docs/ttl)
- `nack(requeue=true)`: rejected because it has no controlled delay and can loop immediately.
- Process-local timer: rejected by the constitution and not durable across restarts.

## Atomic Deduplication

**Decision (confirmed + proposal)**: Make `transaction_id` a PostgreSQL unique key and have `record_first` issue `INSERT ... ON CONFLICT DO NOTHING RETURNING` as the single concurrency boundary. A conflict returns `AlreadyExists` and never updates the persisted record.

**Rationale**: A separate “does it exist?” read races with concurrent consumers. PostgreSQL documents that unique-index conflicts coordinate concurrent inserts and that `ON CONFLICT` provides the alternative to a uniqueness error. This implements the required first-write-wins rule. [PostgreSQL INSERT](https://www.postgresql.org/docs/current/sql-insert.html) and [unique-index checks](https://www.postgresql.org/docs/current/index-unique-checks.html).

**Alternatives considered**:

- Application-side lookup then insert: rejected because it is not atomic.
- Updating a conflict: rejected because the constitution requires the first persisted record to win.
- A separately maintained summary counter: rejected because it can diverge from transaction records; summaries are derived from persisted transactions.

## Exchange Rates and Date Policy

**Decision (proposal)**: Use Frankfurter v2 with the ECB provider pinned for daily base-to-USD rates, through `GET /v2/providers/ecb/rate/{base}/usd?date=YYYY-MM-DD`. Keep the `ExchangeRateProvider` port so the adapter may later be replaced by the direct ECB SDMX API without affecting application logic.

**Rationale**: Frankfurter documents 223 currencies, daily historical coverage back to 1948, direct provider routes, a date parameter, no API key, and local caching permission. Pinning `ecb` avoids its default blended-provider result and gives a compact, interview-explainable adapter. [Frankfurter documentation](https://frankfurter.dev/) and its [OpenAPI definition](https://api.frankfurter.dev/v2/openapi.json).

**Reference-date policy (proposal)**: Normalize the event timestamp to UTC and use its calendar date as `requested_date`. For USD, use rate 1 and `rate_date=requested_date` without a provider call. For another currency, request the direct ECB pair endpoint for `requested_date`. On a documented no-rate response, decrement the candidate date by one calendar day and retry only through `requested_date - 7 days`; stop on the first valid 200 response. A 200 is accepted only if the response pair is correct, the rate is positive and finite, and returned `rate_date` is no later than `requested_date` and within that inclusive eight-date search. Persist `requested_date`, returned `rate_date`, provider (`ecb`), pair, quantized rate, and fetch timestamp. Do not automatically revise a completed conversion if historical data later changes.

**Missing-date and error policy (proposal)**: A provider timeout, connection failure, 429, or 5xx response is retryable and does not consume a fallback day. A 404/no-rate response consumes exactly one candidate day. A 400/422 configuration error, malformed or mismatched response, future requested date, or exhaustion of all eight candidate dates is permanent and goes to the failure queue. The stored requested and returned dates make fallback visible rather than implying a rate existed on a non-publishing day. Deterministic fake tests cover the full decision table; a separate external-provider integration test uses one known ECB business date and one known weekend date.

## Currency Allow-list Lifecycle

**Decision (proposal)**: Commit `config/currencies-v1.json` as the versioned deployment allow-list. It contains `{version, provider: "ecb", source_url, retrieved_on, codes}` and is a reviewed snapshot of Frankfurter's ECB coverage. USD is included. The accepted envelope and persisted record carry `currency_allowlist_version` for auditability.

**Deployment validation and update procedure**: Startup validates schema, unique uppercase three-letter codes, inclusion of USD, and the configured provider/source. A proposed update is generated from the documented provider coverage source, tested in staging against one known ECB business date for every candidate code, code-reviewed, version-bumped, and deployed atomically with its config. No background refresh mutates the active list.

**Failure classification**: A request outside the active snapshot is an invalid input and returns `422` before publication. Timeout/connection/429/5xx errors are temporary provider failures and retry. If a transaction was valid at acceptance but a later provider response reports coverage absent, preserve the envelope and forward it to the durable failure queue with `provider_coverage_changed`; do not reclassify it as invalid input or discard it.

**Alternatives considered**:

- Direct ECB SDMX API: more direct provenance but its series keys and formats add domain complexity for this small service. It remains a compatible replacement adapter. [ECB data API guide](https://data.ecb.europa.eu/help/api/data)
- Frankfurter’s default blended rates: rejected because the provider mix may vary; pinning ECB is more explicit.
- Current-date conversion: rejected because queue delays would change the financial outcome.

## Money Representation

**Decision (confirmed + proposal)**: Use Python `Decimal` (confirmed) and PostgreSQL `NUMERIC(24,8)` for original and converted amounts, `NUMERIC(24,12)` for applied rates, and `ROUND_HALF_EVEN` when quantizing to the stored scale (proposal). Accept decimal strings with no more than eight fractional digits and absolute value no greater than `9,999,999,999,999,999.99999999`; reject an input or result that exceeds those limits.

**Rationale**: The policy eliminates binary floating-point ambiguity, fixes reproducible storage, and provides a large but bounded range for this assignment. Storing the quantized applied rate makes the recorded conversion reproducible.

**Alternatives considered**:

- Binary floating point: rejected by the constitution.
- Currency-specific minor-unit scales: more realistic but exceeds the assignment’s deliberately small scope and complicates a single USD result rule.
- Arbitrary database precision: rejected because it leaves storage limits and rounding unspecified.

## HTTP, Metrics, and Test Tooling

**Decision (proposal)**: FastAPI/Pydantic expose the three application endpoints plus Prometheus-compatible `GET /metrics`; pytest provides unit and Compose-backed integration/recovery tests; k6 measures load.

**Rationale**: These are small, widely understood tools that make contract validation and the interview walkthrough direct. They do not change the required behavior; the contract remains in [contracts/openapi.yaml](contracts/openapi.yaml).

**Alternatives considered**:

- A custom HTTP parser/metrics format: rejected as unnecessary complexity.
- In-memory broker or database tests only: rejected because the constitution requires recovery and outage claims to be verified against actual infrastructure.

## Live Transaction Journey

**Decision (new proposal, user-requested)**: Use OpenTelemetry spans exported to a local Jaeger UI to show a single transaction's journey in development. API acceptance, confirmed broker publication, consumer delivery, rate lookup, conversion, repository result, retry forwarding, and terminal failure/success each create or enrich a span correlated by `transaction_id`. Span attributes include stage, attempt, and outcome; they exclude monetary amounts, full message payloads, credentials, and connection URLs.

**Rationale**: Jaeger accepts the OpenTelemetry Protocol and renders correlated spans as a trace timeline and graph, providing the requested live “factory” view without building a separate dashboard. [Jaeger features](https://www.jaegertracing.io/docs/2.22/features/) and [Jaeger UI configuration](https://www.jaegertracing.io/docs/2.22/deployment/frontend-ui/).

**Boundary**: Export failures are ignored after recording local diagnostics; they never turn a confirmed publish into a failure, trigger an acknowledgement, or prove end-to-end recovery. RabbitMQ's optional firehose tracing is retained as a development debugging alternative only because it adds broker work and its state is not persistent across restart. [RabbitMQ firehose tracing](https://www.rabbitmq.com/docs/4.2/firehose).
