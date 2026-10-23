# Feature Specification: Async Transaction Processing

**Feature Branch**: `001-async-transaction-processing`  
**Created**: 2026-10-22  
**Status**: Draft  
**Input**: Adapted from `docs/specification-v0.1.md` under the Async Processing Constitution.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Submit a Transaction for Reliable Processing (Priority: P1)

As a transaction producer, I submit a valid transaction and receive confirmation that it has been accepted for asynchronous processing, so temporary processing delays do not require me to wait for conversion to finish.

**Why this priority**: Reliable acceptance is the entry point for every other capability and protects producers from transient downstream failures.

**Independent Test**: Submit a valid transaction and verify that acceptance is returned only after durable handoff is confirmed; submit malformed data and verify that it is rejected without being accepted for processing.

**Acceptance Scenarios**:

1. **Given** a transaction with a nonempty ID and user ID, a finite decimal amount, a supported three-letter uppercase currency, and a timezone-aware timestamp, **When** the producer submits it, **Then** the producer receives an acceptance response only after the transaction has been durably accepted for later processing.
2. **Given** a transaction that omits or violates a required field, **When** the producer submits it, **Then** the producer receives a validation error and the transaction is not accepted for processing.
3. **Given** durable handoff cannot be confirmed, **When** the producer submits a valid transaction, **Then** the producer receives a temporary-unavailability response and may safely resend the same transaction ID.

---

### User Story 2 - Process Each Transaction Once Despite Failures (Priority: P1)

As an operations stakeholder, I need every accepted transaction to be converted and recorded without duplicate financial impact, even when processing is retried or restarted.

**Why this priority**: Accurate, durable records are the core business outcome and must withstand normal downstream outages and redelivery.

**Independent Test**: Deliver the same transaction repeatedly, including concurrently; interrupt processing during a downstream outage; then restore dependencies and verify that one durable record results and no accepted transaction is silently lost.

**Acceptance Scenarios**:

1. **Given** a newly accepted transaction in a non-USD supported currency, **When** processing succeeds, **Then** one transaction record stores its original monetary value, its currency, the applied USD rate, that rate's reference date, and the resulting USD value.
2. **Given** a USD transaction, **When** processing succeeds, **Then** its applied conversion rate is 1 and its resulting USD value equals its original value under the documented monetary rules.
3. **Given** repeated or concurrent deliveries with the same transaction ID, **When** they are processed, **Then** at most one transaction record exists and the first persisted record remains authoritative if payloads conflict.
4. **Given** an accepted transaction cannot be processed because an exchange-rate source or persistence is temporarily unavailable, **When** the failure occurs, **Then** the transaction remains available for retry using delays of 1, 2, 4, 8, 16, 32, and then 60 seconds for all later retries.
5. **Given** processing stops after a record is committed but before the delivery is acknowledged, **When** processing recovers, **Then** reprocessing does not create a second record.

---

### User Story 3 - Review a User's Processed Transactions (Priority: P2)

As a transaction viewer, I can see a user's processed USD total and browse that user's transaction history, so I can understand their recorded activity.

**Why this priority**: Queries deliver the visible value of processed data after the reliable processing path is in place.

**Independent Test**: Seed processed records for multiple users, request a summary and a bounded history for one user, and verify totals, isolation, interval handling, ordering, and pagination.

**Acceptance Scenarios**:

1. **Given** a user with processed transactions, **When** a summary is requested, **Then** the response contains that user's USD total and transaction count calculated from persisted transactions.
2. **Given** a user with no processed transactions, **When** a summary is requested, **Then** the response contains a USD total of zero and a transaction count of zero.
3. **Given** processed transactions for more than one user, **When** a history is requested for one user with a time interval, **Then** only that user's records whose event timestamps fall from the inclusive start through the exclusive end are returned.
4. **Given** matching history records with identical timestamps, **When** history is requested, **Then** records are ordered by ascending timestamp and then ascending transaction ID, with a default page size of 50, a maximum page size of 100, and a nonnegative offset.
5. **Given** malformed query parameters, **When** a summary or history request is made, **Then** the request is rejected with a validation error.

---

### User Story 4 - Investigate Processing Health and Failures (Priority: P3)

As an operator, I can observe processing failures and inspect transactions that cannot be processed permanently, so I can detect and remediate operational problems.

**Why this priority**: This supports safe operation and recovery without changing the primary acceptance and processing flows.

**Independent Test**: Cause a permanent processing failure and a temporary failure, then verify durable retention or forwarding, observable failure information, and eventual recovery for the temporary case.

**Acceptance Scenarios**:

1. **Given** a message that is permanently invalid for processing, **When** it is encountered, **Then** it is durably retained for inspection and later reprocessing before the original delivery is considered complete.
2. **Given** one or more processing failures, **When** an operator views operational metrics, **Then** at least the processing-failure count is available.

### Edge Cases

- Accepted transactions can be redelivered after a restart; redelivery must not duplicate persisted data.
- Transactions sharing an ID but containing different payloads preserve the first persisted record.
- A recently accepted transaction may be absent from queries until processing completes.
- Zero and negative monetary amounts are valid unless later requirements explicitly restrict them.
- Timestamps are normalized to UTC before interval filtering and ordering.
- Dates without a published exchange rate and exchange-rate source failures must not silently discard accepted transactions.
- Offset pagination does not promise a frozen history while new matching transactions are recorded.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST accept a transaction only when it contains a nonempty transaction ID, a nonempty user ID, a finite decimal monetary amount, a supported three-letter uppercase currency code, and an ISO 8601 timestamp with a timezone.
- **FR-002**: The system MUST reject invalid transactions before they enter asynchronous processing and communicate that rejection as a validation error.
- **FR-003**: The system MUST confirm acceptance only after durable handoff for asynchronous processing is confirmed; if that confirmation cannot be obtained, it MUST communicate temporary unavailability and MUST NOT claim acceptance.
- **FR-004**: The system MUST normalize accepted event timestamps to UTC and retain the normalized event timestamp with the transaction.
- **FR-005**: The system MUST convert each successfully processed transaction into USD using the convention `USD amount = original amount × USD per unit of original currency`; USD transactions MUST use a rate of 1.
- **FR-006**: The system MUST use decimal monetary arithmetic and documented precision and rounding rules; it MUST store the original amount, original currency, applied rate, rate reference date, and converted USD amount for every persisted transaction.
- **FR-007**: The system MUST use the transaction's UTC event date as the intended exchange-rate reference date, so processing delay does not alter the applicable date.
- **FR-008**: The system MUST persist at most one transaction per transaction ID, including during concurrent delivery; if duplicate IDs contain conflicting payloads, the first persisted record MUST remain the record of authority.
- **FR-009**: The system MUST preserve accepted transactions through temporary exchange-rate or persistence failures, without silently discarding them, and MUST retry them with persistent retry state using delays of 1, 2, 4, 8, 16, 32, and then 60 seconds for subsequent attempts.
- **FR-010**: The system MUST make an uncompleted delivery available again after a processing interruption and MUST consider a delivery complete only after its processing result or durable forwarding has been confirmed.
- **FR-011**: The system MUST durably retain or forward permanently invalid processing messages for inspection and possible reprocessing.
- **FR-012**: The system MUST provide a user summary containing the total converted USD value and count of that user's persisted transactions; users without persisted transactions MUST receive zero for both values.
- **FR-013**: The system MUST provide a user's transaction history filtered by UTC event time using an inclusive start boundary and exclusive end boundary, ordered by ascending timestamp and transaction ID, and restricted to that user.
- **FR-014**: The system MUST support transaction-history pagination with a default limit of 50, a maximum limit of 100, and a nonnegative offset; invalid query parameters MUST be rejected as validation errors.
- **FR-015**: The system MUST expose at least one operational measure of processing failures.
- **FR-016**: The delivered project documentation MUST be in English and explain local startup, the queue selection rationale, one trade-off, and anticipated changes at ten times the expected load.
- **FR-017**: The project MUST include verification of conversion and deduplication, as well as verification against actual dependent services for concurrent duplicates, temporary downstream unavailability, and processing recovery.
- **FR-018**: The project MUST measure behavior at approximately 100 events per second with bursts to 1,000 events per second and report the measurement rather than asserting capacity from design alone.

### Key Entities

- **Transaction Event**: The submitted transaction, identified by a globally unique transaction ID and containing a user ID, decimal amount, currency, and timezone-aware event timestamp.
- **Persisted Transaction**: The authoritative, deduplicated record of a successfully processed transaction, including the original values, normalized timestamp, applied rate, rate reference date, and USD value.
- **Processing Delivery**: A durable unit of asynchronous work for one transaction event, with completion status and persistent retry state when processing cannot yet succeed.
- **Exchange Rate Reference**: The USD-per-unit rate and reference date applied to a transaction; its source and missing-date policy are documented before implementation.
- **User Summary**: A derived view of one user's persisted transactions, containing the USD total and transaction count.
- **Permanent Failure Record**: A durably retained processing message that cannot be completed without review or correction.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In acceptance testing, 100% of valid submitted transactions receive an acceptance outcome only after durable handoff is confirmed, and 100% of invalid submissions are rejected before asynchronous processing.
- **SC-002**: In concurrent duplicate-delivery testing, 100% of tested transaction IDs result in no more than one persisted transaction, including when duplicate payloads conflict.
- **SC-003**: In simulated temporary dependency outages and recovery tests, 100% of accepted test transactions remain recoverable and are either eventually persisted once or durably retained for operator action; none are silently lost.
- **SC-004**: In query acceptance testing, 100% of tested summaries and histories return only persisted records for the requested user and meet the documented totals, boundaries, ordering, and pagination rules.
- **SC-005**: A documented measurement demonstrates observed behavior at an expected sustained load of approximately 100 events per second and burst load of 1,000 events per second, including the test duration, outcomes, and limitations.
- **SC-006**: An independent reviewer can follow the English project documentation to start the service locally, identify the queue rationale and stated trade-off, and understand the documented scaling changes at ten times the expected load.

## Assumptions

- The scope is a small transaction-processing service; unrelated product functions such as user accounts, authorization management, notifications, and multi-currency reporting beyond USD are out of scope.
- Clients represent monetary inputs as decimal strings so submitted values can be interpreted without binary floating-point ambiguity.
- An acceptance response means durable acceptance for later processing, not completed conversion or immediate query visibility.
- Queries intentionally reflect only transactions that have completed processing; they do not promise read-after-acceptance consistency.
- Supported currencies, exact decimal storage precision, rounding rules, exchange-rate provider, and the policy for dates without a published rate will be selected and recorded during planning before corresponding behavior is implemented.
- The project follows the constitution's required durable at-least-once delivery and idempotent persistence model; its operational limits will be documented with measurement results.
- Public-repository publication is a delivery activity outside this feature specification; the repository contents must nevertheless remain suitable for the required walkthrough.

