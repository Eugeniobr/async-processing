# Processing-Failure Metrics

`transaction_processing_failures_total` is a Prometheus counter with exactly one label:
`kind`, whose values are `retryable` and `permanent`.

The consumer increments it once after RabbitMQ confirms forwarding a classified failure
to a retry or failure queue and before it acknowledges the original delivery. It does
not count accepted publications, successful processing, duplicate persistence outcomes,
or failed/uncertain forwarding. It has no transaction, user, payload, amount, or error
reason labels.

## Process layout and scraping

The HTTP and consumer processes do not share Python memory. Both must receive the same
writable `PROMETHEUS_MULTIPROC_DIR` environment variable before Python starts; the
directory must be a shared volume in a multi-container deployment. The consumer writes
its counter shards there. The HTTP process is assembled with `build_api_container()` and
`GET /metrics` aggregates those shards through the Prometheus multiprocess collector.
Prometheus scrapes that HTTP process's `/metrics` endpoint, not an in-memory API registry
unrelated to the consumer.

The multiprocess directory is operational state. Clear stale shard files only while both
processes are stopped, then create the shared directory before starting them. It is not a
durable business record and is not used for retry or acknowledgement decisions.

## Observation boundary

The counter reports confirmed failure-forwarding attempts, not unique transactions.
Between the forwarding confirm, counter increment, and original-delivery acknowledgement,
a crash can lose or repeat an observation while the message itself remains recoverable.
This service deliberately does not add transactional metric persistence or exactly-once
metric semantics.
