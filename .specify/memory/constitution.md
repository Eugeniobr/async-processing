# Async Processing Constitution

## Core Principles

### I. Specification Before Implementation

Changes MUST start from documented behavior and acceptance criteria. Use GitHub Spec Kit
to carry the specification through clarification, planning, task breakdown, implementation,
and verification. Resolve decisions that affect a task before implementing that task.
Keep the original specification v0.1 as a historical baseline.

### II. Simple and Explainable Object-Oriented Design

Application behavior MUST use classes with clear responsibilities, the Repository pattern
for persistence, and constructor-based dependency injection. Endpoints MAY remain small
functions that validate input and delegate work. Introduce an abstraction only when it
serves a concrete implementation or testing need. Avoid generic base classes, unnecessary
layers, and extra design patterns without a documented benefit. Each component MUST be
small enough for the candidate to explain its responsibilities and control flow during
the interview; do not impose an arbitrary line-count limit.

### III. Durable Acceptance and Idempotent Persistence

Accepted events MUST survive the documented temporary downstream failures and consumer
restarts. Use at-least-once delivery and idempotent persistence. Enforce transaction ID
uniqueness in the database, including concurrent deliveries. Acknowledge a message only
after its processing result or durable forwarding is confirmed. The first persisted
record wins when duplicate IDs carry different payloads. Temporary failures MUST NOT
silently discard accepted events. Document the operational limits of the durability claim.

### IV. Explicit Monetary and Time Semantics

Monetary calculations MUST use decimal arithmetic rather than binary floating point.
The conversion convention, reference date, supported currencies, precision, and rounding
rules MUST be explicit before the corresponding behavior is implemented. Store the
applied rate and its reference date with the converted transaction. Normalize event
timestamps to UTC and specify query interval boundaries and deterministic ordering.

### V. Verification Against Acceptance Criteria

Conversion and deduplication MUST have unit tests. Verify concurrency, downstream outages,
and consumer recovery against actual infrastructure before claiming those guarantees.
Trace verification to specification requirements. Measure the expected event load rather
than claiming capacity from architectural choices. Report checks actually run and any
remaining limitations; documentation-only changes do not require application tests.

## Project Constraints and Recorded Decisions

- Implement the service in Python and provide Docker Compose for local execution.
- Use RabbitMQ for the message broker and PostgreSQL for persistence.
- Use exponential retry delays of 1, 2, 4, 8, 16, 32, and then 60 seconds, capped at
  60 seconds on subsequent attempts. Persist retry state rather than relying exclusively
  on process-local timers. Retry topology and failure classification belong in the plan.
- Select an exchange-rate API with daily historical coverage during planning. The provider
  has not yet been selected. The v0.1 proposal to use the event's UTC date still needs an
  explicit recorded resolution, including dates without a published rate.
- Use Decimal for money. Exact storage precision and rounding remain planning decisions;
  approval of decimal arithmetic does not select those numeric parameters.
- Keep repository documentation and project artifacts in English.
- The assignment's scope is a small service handling approximately 100 events per second
  with bursts to 1,000 per second. Do not introduce unrelated product features.
- The README MUST cover startup, queue choice, one trade-off, and changes at ten times
  the load. The public repository and implementation MUST support the interview walkthrough.

## Development Workflow and Quality Gates

1. Establish this constitution using the Spec Kit constitution workflow.
2. Adapt the v0.1 baseline into a feature specification with prioritized user scenarios,
   testable requirements, measurable outcomes, and a separate quality checklist.
3. Clarify behavior and write the technical plan, including researched decisions,
   data model, component responsibilities, and external contracts.
4. Derive small implementation tasks and analyze consistency across the artifacts.
5. Implement and verify one bounded increment at a time, keeping changes explainable.
6. Run convergence checks and address remaining gaps before declaring completion.

Each stage MUST leave its results reviewable before the next stage is undertaken. The
current adoption increment includes tooling and governance only; application code is
deferred. Future implementation changes MUST reference the relevant requirement or task.

## Governance

User instructions and the original assignment take precedence over this document. This
constitution governs project design and workflow; it does not override the assignment.
Proposed amendments MUST state their reason and identify affected specifications, plans,
and tasks. Update those artifacts when an adopted change makes them inconsistent.

Use semantic versioning: major for incompatible changes to principles, minor for added
principles or materially expanded guidance, and patch for non-semantic clarifications.
Review future changes for compliance and document justified departures. Preserve the
baseline at docs/specification-v0.1.md; subsequent decisions belong in the active Spec Kit
artifacts rather than silent edits to that historical document.

**Version**: 1.0.0 | **Ratified**: 2026-10-22 | **Last Amended**: 2026-10-22
