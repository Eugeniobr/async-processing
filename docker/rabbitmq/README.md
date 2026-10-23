# Local RabbitMQ topology

The Compose broker is pinned to RabbitMQ 4.3.6 with the management plugin. A new
4.3 broker enables stable feature flags at first startup; the live verification
for this task confirms that `stream_queue` is enabled, which is required by
quorum queues that use at-least-once dead-lettering.

| Queue | Purpose |
| --- | --- |
| `transactions.work` | Durable queue for newly accepted and retried transaction envelopes. |
| `transactions.retry.1s` … `transactions.retry.60s` | Fixed-delay buckets. Each expires messages into `transactions` with routing key `work`. |
| `transactions.failure` | Durable inspection queue for permanently unprocessable envelopes. |

All queues are durable, non-exclusive quorum queues. The safety policies reject
publishes at the 100,000-message limit and disable the quorum delivery limit.
Only retry queues use at-least-once dead-lettering, so duplicate retry deliveries
remain possible and must be handled idempotently by the later consumer.

This is a one-node local broker. Its volume and restart checks show local
persistence only; they do not demonstrate multi-node quorum availability or
end-to-end consumer recovery.
