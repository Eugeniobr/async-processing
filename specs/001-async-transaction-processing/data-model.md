# Data Model: Async Transaction Processing

This model defines planned persistent data and durable-message fields. It is not a migration or implementation.

## Persisted Transaction

| Field | Type / limit | Rules |
| --- | --- | --- |
| `transaction_id` | text, unique | Nonempty global idempotency key. The first committed row is authoritative. |
| `user_id` | text | Nonempty; used for query isolation. |
| `original_amount` | numeric(24,8) | Parsed from a decimal string; zero and negative values are allowed. |
| `currency` | char(3) | Uppercase configured provider-supported code. |
| `currency_allowlist_version` | text | Version from the accepted durable envelope, initially `currencies-v1`. |
| `event_timestamp` | timestamp with time zone | Normalized to UTC at ingestion; history filter and first sort key. |
| `rate_provider` | text | Initially `ecb` through Frankfurter. |
| `rate_requested_date` | date | UTC calendar date of `event_timestamp`. |
| `rate_date` | date | Date actually returned by the provider; never later than `rate_requested_date`. |
| `applied_rate` | numeric(24,12) | USD per unit of original currency after `ROUND_HALF_EVEN` quantization. USD is exactly 1. |
| `converted_usd_amount` | numeric(24,8) | `original_amount × applied_rate`, quantized with `ROUND_HALF_EVEN`. |
| `created_at` | timestamp with time zone | UTC persistence audit time. |

**Constraints and indexes**:

- Unique constraint on `transaction_id` is the deduplication mechanism.
- Query index on `(user_id, event_timestamp, transaction_id)` supports the documented history order and interval filter.
- `record_first` must insert all fields in one transaction using conflict-do-nothing semantics; it must not update a conflicting row.

## Durable Delivery Envelope

| Field | Purpose |
| --- | --- |
| `schema_version` | Lets the consumer reject unknown envelope formats permanently. |
| `event` | Immutable original transaction event. |
| `attempt` | Starts at zero and increases on each confirmed retry forwarding. |
| `accepted_at` | UTC audit timestamp established at ingestion. |
| `currency_allowlist_version` | The active allow-list version that accepted the event; it is immutable through retries. |
| `last_failure_kind` | Diagnostic value for the most recent retry route. |

The envelope is persistent in work/retry/failure queues. It is not a second source of truth once a transaction is persisted.

## Rate Quote

| Field | Purpose |
| --- | --- |
| `base_currency` / `quote_currency` | The requested pair; quote is USD. |
| `rate` | Provider decimal parsed without binary floating point. |
| `requested_date` | Event UTC date. |
| `rate_date` | Published date for the returned rate. |
| `provider` | `ecb`. |
| `fetched_at` | UTC retrieval audit timestamp. |

The processor tries the requested UTC date and, only after a no-rate response, each preceding calendar date through seven days earlier. It accepts a quote only when its pair is correct, its rate is positive and finite, and its `rate_date` lies in that inclusive eight-date window ending on `requested_date`.

## Permanent Failure Record

The failure queue carries the original durable envelope plus a reason code and diagnostic text. Planned codes are `unsupported_currency`, `provider_coverage_changed`, `no_eligible_rate`, `invalid_envelope`, and `invalid_provider_response`. Operators may repair and replay a record manually; automatic replay is out of scope.

## Derived Query Views

- **User summary**: `SUM(converted_usd_amount)` and `COUNT(*)` over persisted transactions for one `user_id`. An empty set returns decimal zero and count zero.
- **User history**: persisted transactions for one user where `from <= event_timestamp < to`, ordered by `(event_timestamp ASC, transaction_id ASC)`, then limited and offset.

## State Transitions

```text
validated → published-to-work → processing
processing → persisted → acknowledged
processing → duplicate-found → acknowledged
processing → confirmed-retry-bucket → acknowledged → published-to-work
processing → confirmed-failure-queue → acknowledged
```

If the consumer fails before acknowledgement, RabbitMQ redelivers the unacknowledged state. If it fails after a database commit, the redelivery becomes `duplicate-found`.
