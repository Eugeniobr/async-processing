# Validation Quickstart

This guide is for validating the planned service after implementation. It intentionally contains no application code, migrations, or task list.

## Prerequisites

- Docker and Docker Compose.
- A configured Frankfurter/ECB network path for rate retrieval, `config/currencies-v1.json`, or deterministic test doubles that return documented rate quotes.
- The eventual project README for exact startup commands; it must explain queue selection, one trade-off, and ten-times-load changes.

## Start the Local Dependencies and Service

After implementation, use the repository’s documented Compose command to start PostgreSQL, RabbitMQ 4.3.6, the HTTP service, and a consumer. Confirm that work/retry/failure queues are durable quorum queues with `overflow=reject-publish`, `max-length=100000`, and `delivery-limit=-1`; retry queues additionally have `dead-letter-strategy=at-least-once`, a DLX back to work, and fixed TTLs; and the service’s `/metrics` endpoint is reachable. The local single-node broker does not prove multi-node quorum survival.

## Core Acceptance Checks

1. Submit a valid transaction to `POST /transactions` using the shape in [contracts/openapi.yaml](contracts/openapi.yaml). Expect `202` and `status: accepted` only after durable publish confirmation.
2. Submit malformed data, an unsupported currency, or a timestamp without a timezone. Expect `422` and no work message.
3. Submit the same ID twice, including simultaneous submissions or deliveries. After processing, query the user summary and history; expect exactly one persisted transaction, retaining the first payload.
4. Submit a USD transaction and a non-USD transaction. Verify the records against [data-model.md](data-model.md): decimal strings, UTC timestamp, requested and returned rate dates, stored rate, and converted amount.
5. Verify a known ECB business date and known weekend date against the live provider; the latter must retain the requested date and the returned prior rate date. Verify that an input outside `currencies-v1` returns `422` before publication.
6. Request a summary for a user with no records. Expect decimal zero and count zero. Request history for a bounded UTC interval and verify its inclusive/exclusive boundaries, `(timestamp, id)` order, default 50 limit, maximum 100 limit, and offset behavior.

## Failure and Recovery Checks

1. Make rate retrieval or PostgreSQL temporarily unavailable after a work delivery. Verify the message enters the fixed retry sequence and is not acknowledged until confirmed retry forwarding succeeds. Restore the dependency and verify one eventual persisted record.
2. Stop the consumer after the persistence commit but before its acknowledgement (using an implementation test seam). Restart it. Verify redelivery results in no second transaction record.
3. Stop the consumer after retry publication is confirmed but before acknowledgement of the original delivery. Restart it. Verify one or more retry deliveries may occur, but the transaction is never lost and persistence remains idempotent.
4. Deliver a permanently invalid envelope, a transaction with no eligible historical rate, or an event whose accepted allow-list code later loses provider coverage. Verify it reaches the durable failure queue and increments `transaction_processing_failures_total{kind="permanent"}`.
5. Cause a retryable failure. Verify `transaction_processing_failures_total{kind="retryable"}` and a retry metric increase, without a hot requeue loop.

## Load Measurement

Use the chosen load tool to run a documented sustained scenario near 100 accepted events/s and a separate 1,000-event burst. Record command, environment, duration, request outcomes, end-to-end completion, duplicate count, queue depth, processing-failure count, and known limitations. Do not turn a passing local run into an unmeasured capacity claim.

## Traceability

- API outcomes: [spec.md](spec.md) FR-001–FR-003 and FR-012–FR-015.
- Durable processing: FR-005–FR-011 and SC-002–SC-003.
- Required verification: FR-017–FR-018 and SC-005.
