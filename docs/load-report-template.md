# Load Measurement Report Template

Use [transactions.js](../tests/load/transactions.js) against the real API, consumer,
RabbitMQ, and PostgreSQL processes. This template records an observation; it is not a
capacity claim. Run sustained and burst scenarios separately, retain the k6 output, and
fill every field below.

## Environment

| Field | Value |
| --- | --- |
| Date and UTC time | |
| Git revision | |
| Host CPU, memory, and OS | |
| Python version | |
| RabbitMQ image/version | `rabbitmq:4.3.6-management` unless changed and justified |
| PostgreSQL image/version | |
| Consumer process count and prefetch | |
| `PROMETHEUS_MULTIPROC_DIR` arrangement | |
| Rate provider and currency used | `USD` avoids an external rate request; record another choice explicitly |
| Database and broker persistence volumes | |

## Preparation

1. Start Compose, apply the migration, and start the API and consumer as documented in
   the README.
2. Export a new `LOAD_RUN_ID` for each run. Do not reuse it, because transaction IDs are
   idempotency keys.
3. Ensure `/metrics` responds before beginning. Optionally set
   `RABBITMQ_MANAGEMENT_URL`, `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD`, and
   `RABBITMQ_VHOST_ENCODED` to sample the final work-queue depth. For an isolated vhost,
   use its URL-encoded name (for example, `%2Fload-run`).
4. Save stdout/stderr and `--summary-export` for each command.

Example commands:

```bash
export LOAD_RUN_ID="sustained-$(date +%s)"
k6 run --summary-export="${LOAD_RUN_ID}.json" \
  -e SCENARIO=sustained -e BASE_URL=http://localhost:8000 \
  -e RABBITMQ_MANAGEMENT_URL=http://localhost:15672 \
  tests/load/transactions.js | tee "${LOAD_RUN_ID}.log"

export LOAD_RUN_ID="burst-$(date +%s)"
k6 run --summary-export="${LOAD_RUN_ID}.json" \
  -e SCENARIO=burst -e BASE_URL=http://localhost:8000 \
  -e RABBITMQ_MANAGEMENT_URL=http://localhost:15672 \
  tests/load/transactions.js | tee "${LOAD_RUN_ID}.log"
```

The defaults are a 60-second constant arrival rate of 100 submissions/second and a
1,000-iteration concurrent burst. The script rejects invalid `SCENARIO`, `RATE`,
`BURST_COUNT`, `BURST_VUS`, completion timeout, and poll interval inputs before traffic
is sent. It records accepted/rejected/unavailable outcomes, per-event completion
observation and latency, duplicate records seen in query results, the failure counter,
and (when management credentials are provided) final work-queue depth.

For an exact accepted-ID-to-row reconciliation, add `-e RECORD_ACCEPTED_IDS=1` and
`--out json="${LOAD_RUN_ID}.raw.json"`. This emits the test-only
`transaction_acceptance_id_ledger` samples with an ID tag to the local raw file. It is
disabled by default, is not a service metric, and must never be scraped or exported as
Prometheus observability data.

After the queues drain, run the repository helper against the isolated load database:

```bash
python tests/load/reconcile.py \
  --raw "${LOAD_RUN_ID}.raw.json" \
  --database-url "$DATABASE_URL" \
  --output "${LOAD_RUN_ID}.reconciliation.json"
```

The report compares the exact accepted-ID set with committed rows and lists missing,
unexpected, or duplicate IDs; matching counts alone are not acceptance evidence.

Use the matching raw file with `tests/load/analyze.py` to produce the acceptance-only
status, throughput, and p50/p95/p99 latency report. It excludes the polling GET requests
from the acceptance latency calculation.

## Sustained scenario (~100 events/s)

| Measurement | Result |
| --- | --- |
| Command and k6 version | |
| Configured rate and duration | |
| Start/end UTC and duration | |
| Submitted / `202` accepted / `422` rejected / `503` unavailable | |
| Completion observed / not observed before deadline | |
| Completion latency (min, median, p95, max) | |
| Duplicate records observed | |
| Work queue depth at end | |
| `transaction_processing_failures_total{kind="retryable"}` | |
| `transaction_processing_failures_total{kind="permanent"}` | |
| HTTP errors, dropped iterations, and check failures | |
| Log and summary-export paths | |

## Burst scenario (1,000 events)

| Measurement | Result |
| --- | --- |
| Command and k6 version | |
| Configured iterations and VUs | |
| Start/end UTC and duration | |
| Submitted / `202` accepted / `422` rejected / `503` unavailable | |
| Completion observed / not observed before deadline | |
| Completion latency (min, median, p95, max) | |
| Duplicate records observed | |
| Work queue depth at end | |
| `transaction_processing_failures_total{kind="retryable"}` | |
| `transaction_processing_failures_total{kind="permanent"}` | |
| HTTP errors, dropped iterations, and check failures | |
| Log and summary-export paths | |

## Interpretation and limitations

- A `202` proves only confirmed RabbitMQ acceptance. Completion measurements are based on
  subsequent query visibility after PostgreSQL persistence.
- Database uniqueness is idempotent persistence, not exactly-once broker delivery.
- Failure counters count confirmed forwarding attempts, not unique transactions; a crash
  between forwarding confirmation, metric update, and acknowledgement may lose or repeat
  an observation while preserving the message.
- The single-node Compose broker does not establish multi-node quorum availability.
- State any provider outage, rate-limit response, external workload, stale metric shard,
  queue backlog, or test-environment constraint that affected the result.
