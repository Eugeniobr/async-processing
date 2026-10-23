# Load Measurement Report — 2026-10-23

## Scope and reproducibility

This is a local measurement, not a capacity claim. It used the production API and consumer
composition, RabbitMQ quorum queues, PostgreSQL, and the actual `POST /transactions` path.

- Generator: official `grafana/k6:0.55.0` image.
- Host: Linux 5.19, 12 logical CPUs, 15 GiB RAM (6.2 GiB available at collection time).
- Services: PostgreSQL `postgres:16.9-alpine`; RabbitMQ
  `rabbitmq:4.3.6-management`; one API process and one consumer process.
- Consumer configuration: `CONSUMER_PREFETCH=100`.
- Isolation: database `load_20261023_load_185755` and RabbitMQ vhost
  `/20261023-load-185755`, both removed after the run. The vhost declared nine durable quorum
  queues and the approved overflow, delivery-limit, and retry DLX policies.
- Rate condition: all events used USD. The approved USD branch creates a deterministic rate
  of `1` and makes no external provider request.
- Durable reviewed evidence: [evidence/load-validation-20261023.md](evidence/load-validation-20261023.md)
  and its 2.1 MiB archive. It contains the corrected k6 raw JSON, summaries, reconciliation
  reports, topology snapshots, drain/cleanup state, and host environment details. Its archive
  SHA-256 is `44ff30478e7080933ae5f4b0c03fcfb52a4f609d0dca8c85ad8383d57914ed9d`.
  The full original working set remains locally at `.load-evidence/20261023-load-185755/`
  (ignored by Git), including preliminary runs and local diagnostics deliberately excluded
  from the reviewed package. The reviewed subset contains synthetic IDs only.

The corrected commands use the isolated vhost and persist the exact accepted ID set only in
the local k6 raw file:

```bash
docker run --rm -u "$(id -u):$(id -g)" \
  --network container:async-processing-py312-runner \
  -v "$PWD:/app" -w /app grafana/k6:0.55.0 run \
  --summary-export=/app/.load-evidence/20261023-load-185755/<scenario>-summary.json \
  --out json=/app/.load-evidence/20261023-load-185755/<scenario>.raw.json \
  -e BASE_URL=http://127.0.0.1:8000 \
  -e RABBITMQ_MANAGEMENT_URL=http://127.0.0.1:15672 \
  -e RABBITMQ_VHOST_ENCODED=%2F20261023-load-185755 \
  -e RECORD_ACCEPTED_IDS=1 -e LOAD_RUN_ID=<run-id> \
  tests/load/transactions.js
```

`RECORD_ACCEPTED_IDS=1` is test-only and emits ID tags solely to the local k6 raw JSON. It is
not an application metric and is never exposed through `/metrics`.

## Sustained scenario

Configuration: `SCENARIO=sustained`, `RATE=100`, `DURATION=60s`.

| Observation | Result |
| --- | --- |
| Scheduled / completed iterations | 6,001 / 6,001 |
| Dropped or interrupted iterations | 0 / 0 |
| `POST /transactions` responses | 6,001 × `202`; no `422`, `503`, or other POST status |
| Effective accepted throughput | 100.02/s across a 59.999 s acceptance span |
| Acceptance latency p50 / p95 / p99 | 3.09 ms / 5.01 ms / 11.63 ms |
| Completion observation | 100%; k6 p50 8 ms, p95 209 ms, max 251 ms from submission to query visibility |
| Reconciliation | 6,001 distinct accepted IDs; 6,001 matching persisted IDs; zero missing, unexpected, or duplicate rows |
| Retry/failure queues and metric | All queues empty after drain check; no retry/failure forwarding; no failure counter series was emitted because no classified failure occurred |

At k6 teardown, the work-queue snapshot observed one pending message. The controller then
performed a separate conditional drain check with a 90-second deadline; the first poll found
every queue at zero ready and zero unacknowledged messages. This is an observation after the
run returns, not a claim that draining took zero time.

## Burst scenario

Configuration: `SCENARIO=burst`, `BURST_COUNT=1000`, `BURST_VUS=100`,
`BURST_MAX_DURATION=2m`.

| Observation | Result |
| --- | --- |
| Scheduled / completed iterations | 1,000 / 1,000 |
| Dropped or interrupted iterations | 0 / 0 |
| `POST /transactions` responses | 1,000 × `202`; no `422`, `503`, or other POST status |
| Acceptance span / effective throughput | 3.241 s / 308.55 accepted/s |
| Acceptance latency p50 / p95 / p99 | 306.47 ms / 361.07 ms / 379.16 ms |
| Completion observation | 100%; k6 p50 324 ms, p95 378 ms, max 524 ms from submission to query visibility |
| Reconciliation | 1,000 distinct accepted IDs; 1,000 matching persisted IDs; zero missing, unexpected, or duplicate rows |
| Queue and failure state | Work, retry, and failure queues were empty on the first post-run drain poll within the 90-second deadline; no failure forwarding occurred |

## Generator correction and limitations

The first executions exposed a generator defect: teardown queried the root vhost and did not
send a usable Basic authorization header, creating one `401` monitoring request. It did not
affect any `POST` acceptance result, but those preliminary runs are excluded from this report.
`transactions.js` now accepts `RABBITMQ_VHOST_ENCODED` and sets the Basic header explicitly;
the corrected reruns had zero HTTP failures.

The generator performs one or more query polls per accepted transaction to establish
completion, so its total HTTP request rate includes those reads and must not be described as
acceptance throughput. The acceptance-only measurements above are calculated from POST points
in the raw data. No latency pass/fail threshold exists in the specification; these are reported
as measurements only.

This single-host, single-consumer run does not establish cluster-member-loss survival,
production capacity, or exactly-once delivery. Confirmed publication proves durable broker
acceptance; unique persistence provides first-write-wins idempotency.
