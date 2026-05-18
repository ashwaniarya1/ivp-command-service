# IVP Command Service

Kafka-based command processing service for Investment Plans (IVP), implemented with Java 21, Spring Boot, Spring Kafka, Spring Data JPA, PostgreSQL, Maven, and Testcontainers.

The service owns the command side of the IVP domain. It consumes plan commands, persists plan and execution state, emits IVP domain events, sends order creation commands to an external Order Domain, and reconciles order result events back into execution outcomes.

## Contents

- [What This Service Does](#what-this-service-does)
- [Architecture](#architecture)
- [Kafka Topics](#kafka-topics)
- [Message Contracts](#message-contracts)
- [Domain Model](#domain-model)
- [Business Rules](#business-rules)
- [Idempotency](#idempotency)
- [Concurrency and Completion](#concurrency-and-completion)
- [Persistence Decisions](#persistence-decisions)
- [Invalid and Edge Case Handling](#invalid-and-edge-case-handling)
- [Build, Run, and Test](#build-run-and-test)
- [Test Coverage](#test-coverage)
- [Trade-offs](#trade-offs)
- [What I Would Add With More Time](#what-i-would-add-with-more-time)
- [Use of AI Tools](#use-of-ai-tools)

## What This Service Does

The IVP Command Service handles the lifecycle of investment plans:

1. Creates plans from `CREATE_PLAN` commands.
2. Soft-deletes plans from `DELETE_PLAN` commands.
3. Executes active plans from `EXECUTE_PLAN` commands.
4. Creates one order per investment during execution.
5. Tracks `ORDER_EXECUTED` and `ORDER_FAILED` events from the Order Domain.
6. Emits IVP events for a read model.
7. Completes a plan execution only after every generated order is terminal.

The service exposes no REST API. Kafka is the only integration surface, matching the CQRS/event-driven assignment context.

## Architecture

```text
API Service
  | CREATE_PLAN / DELETE_PLAN
  v
ivp-commands  ----+
                  |
Scheduler         |
  | EXECUTE_PLAN  |
  v               v
ivp-commands --> IVP Command Service --> order-commands --> Order Domain
                       ^                                      |
                       |                                      |
                       +------------- order-events <----------+
                       |
                       +------------- ivp-events -----------> Read Model
```

External systems are intentionally out of scope:

- API Service: sends create/delete commands.
- Scheduler: sends execution commands.
- Order Domain: consumes order commands and emits order results.
- Read Model: consumes IVP events.

## Kafka Topics

| Topic | Direction | Messages | Purpose |
| --- | --- | --- | --- |
| `ivp-commands` | Inbound | `CREATE_PLAN`, `DELETE_PLAN`, `EXECUTE_PLAN` | Commands from API Service and Scheduler |
| `order-commands` | Outbound | `CREATE_ORDER` | Commands sent to the Order Domain |
| `order-events` | Inbound | `ORDER_EXECUTED`, `ORDER_FAILED` | Order result events from the Order Domain |
| `ivp-events` | Outbound | `PLAN_CREATED`, `PLAN_DELETED`, `PLAN_ORDER_FILLED`, `PLAN_ORDER_REJECTED`, `PLAN_EXECUTION_COMPLETED` | Domain events for the read model |

### Partition Keys

The implementation explicitly provides Kafka keys for outbound messages:

| Message | Key |
| --- | --- |
| `CREATE_ORDER` | `orderId` |
| IVP domain events | `planId` |

Inbound command keys are owned by upstream producers. A production setup should key plan-scoped commands by `planId` when known; `CREATE_PLAN` can be keyed by `commandId` because no `planId` exists yet.

## Message Contracts

All inbound messages include a `type` field in the JSON body. The consumer reads `type` and routes to the correct handler. The Java records do not store `type`; Jackson ignores it after routing.

### Create Plan

```json
{
  "type": "CREATE_PLAN",
  "commandId": "11111111-1111-1111-1111-111111111111",
  "userId": "22222222-2222-2222-2222-222222222222",
  "name": "Monthly Tech Portfolio",
  "investments": [
    { "instrument": "AAPL", "amount": 100.00 },
    { "instrument": "GOOGL", "amount": 50.00 }
  ],
  "executionDay": 1
}
```

**Sender contract:** `commandId` must be stable across retries. If a retry sends a different `commandId`, IVP will treat it as a new command and create a duplicate plan.

**Important:** `commandId` is recorded in the idempotency table before validation runs. If a command is invalid (e.g. `executionDay: 32`), the `commandId` is permanently consumed. A re-sent message with the same `commandId` but corrected fields will be silently dropped. Upstream producers must fix the payload and use a new `commandId` for a corrected command.

Validation:

- `userId` is required.
- `name` must not be blank.
- `executionDay` must be between 1 and 31.
- `investments` must not be empty.
- every investment requires a non-blank `instrument`.
- every investment amount must be greater than zero.

Emits:

```json
{
  "type": "PLAN_CREATED",
  "eventId": "<uuid>",
  "planId": "<plan-id>",
  "userId": "<user-id>",
  "name": "Monthly Tech Portfolio",
  "executionDay": 1,
  "investments": [
    { "instrument": "AAPL", "amount": 100.00 },
    { "instrument": "GOOGL", "amount": 50.00 }
  ],
  "occurredAt": "2026-03-01T10:00:00Z"
}
```

### Delete Plan

```json
{
  "type": "DELETE_PLAN",
  "commandId": "33333333-3333-3333-3333-333333333333",
  "planId": "<plan-id>"
}
```

Deletion is soft: the plan remains stored with status `DELETED`. This preserves history and does not attempt to cancel orders that may already have been sent to the Order Domain.

Emits:

```json
{
  "type": "PLAN_DELETED",
  "eventId": "<uuid>",
  "planId": "<plan-id>",
  "userId": "<user-id>",
  "occurredAt": "2026-03-01T10:00:00Z"
}
```

### Execute Plan

```json
{
  "type": "EXECUTE_PLAN",
  "commandId": "44444444-4444-4444-4444-444444444444",
  "planId": "<plan-id>",
  "executionDate": "2026-03-01"
}
```

**Sender contract:** `commandId` must be stable across retries. Ideally the Scheduler generates a deterministic `commandId` per `(planId, executionDate)` — e.g. `UUID.nameUUIDFromBytes(planId + executionDate)` — so that a Scheduler restart does not re-execute the same plan on the same date.

The service checks that:

- `commandId`, `planId`, and `executionDate` are present.
- the plan exists.
- the plan is active.
- the same plan has not already been executed for the same `executionDate`.

Trading-day decisions are intentionally not validated here. The Scheduler owns date selection.

For each investment, the service creates a pending `PlanOrder` and sends a `CREATE_ORDER` command:

```json
{
  "type": "CREATE_ORDER",
  "commandId": "<uuid>",
  "orderId": "<order-id>",
  "userId": "<user-id>",
  "planId": "<plan-id>",
  "executionId": "<execution-id>",
  "executionDate": "2026-03-01",
  "instrument": "AAPL",
  "amount": 100.00,
  "orderDirection": "BUY"
}
```

### Order Executed

```json
{
  "type": "ORDER_EXECUTED",
  "eventId": "55555555-5555-5555-5555-555555555555",
  "orderId": "<order-id>",
  "planId": "<plan-id>",
  "executionId": "<execution-id>",
  "instrument": "AAPL",
  "filledAmount": 100.00,
  "executedAt": "2026-03-01T10:00:00Z"
}
```

Emits:

```json
{
  "type": "PLAN_ORDER_FILLED",
  "eventId": "<uuid>",
  "planId": "<plan-id>",
  "executionId": "<execution-id>",
  "orderId": "<order-id>",
  "instrument": "AAPL",
  "occurredAt": "2026-03-01T10:00:01Z"
}
```

### Order Failed

```json
{
  "type": "ORDER_FAILED",
  "eventId": "66666666-6666-6666-6666-666666666666",
  "orderId": "<order-id>",
  "planId": "<plan-id>",
  "executionId": "<execution-id>",
  "instrument": "GOOGL",
  "reason": "insufficient liquidity",
  "failedAt": "2026-03-01T10:00:00Z"
}
```

Emits:

```json
{
  "type": "PLAN_ORDER_REJECTED",
  "eventId": "<uuid>",
  "planId": "<plan-id>",
  "executionId": "<execution-id>",
  "orderId": "<order-id>",
  "instrument": "GOOGL",
  "reason": "insufficient liquidity",
  "occurredAt": "2026-03-01T10:00:01Z"
}
```

### Execution Completed

When every order for an execution is either `FILLED` or `REJECTED`, the service emits:

```json
{
  "type": "PLAN_EXECUTION_COMPLETED",
  "eventId": "<uuid>",
  "planId": "<plan-id>",
  "executionId": "<execution-id>",
  "result": "PARTIALLY_FILLED",
  "occurredAt": "2026-03-01T10:00:02Z"
}
```

Possible results:

| Result | Meaning |
| --- | --- |
| `FULLY_FILLED` | all orders filled |
| `PARTIALLY_FILLED` | at least one order filled and at least one rejected |
| `FULLY_REJECTED` | all orders rejected |

## Order Domain Contract Extensions

The assignment-provided Order Domain contracts were intentionally minimal. This implementation extends them to make correlation and idempotency reliable.

### Fields Added to `CREATE_ORDER`

| Field | Why it is needed |
| --- | --- |
| `type` | Allows a shared topic to route by message type. |
| `commandId` | IVP-generated sender-owned key for this outbound message. Enables message-level tracing on the Order Domain side. |
| `orderId` | Generated by IVP and used as the cross-domain correlation key. The Order Domain should deduplicate by this value. |
| `planId` | Enables tracing back to the plan. |
| `executionId` | Ties the order to one specific execution. |
| `executionDate` | Preserves the scheduling context for audit and reconciliation. |

The assignment already required `userId`, `instrument`, `amount`, and `orderDirection`. This implementation sets `orderDirection` to `BUY`, because executing an investment plan buys the configured instruments.

### Fields Added to `ORDER_EXECUTED`

| Field | Why it is needed |
| --- | --- |
| `type` | Allows routing from the shared `order-events` topic. |
| `eventId` | Message-level idempotency key. |
| `planId` | Traceability and debugging. |
| `executionId` | Traceability and debugging. |
| `instrument` | Audit/readability. |
| `filledAmount` | Captures the executed amount returned by the Order Domain. |
| `executedAt` | Captures the Order Domain timestamp. |

### Fields Added to `ORDER_FAILED`

| Field | Why it is needed |
| --- | --- |
| `type` | Allows routing from the shared `order-events` topic. |
| `eventId` | Message-level idempotency key. |
| `planId` | Traceability and debugging. |
| `executionId` | Traceability and debugging. |
| `instrument` | Audit/readability. |
| `failedAt` | Captures the Order Domain timestamp. |

The assignment already included `orderId` and `reason`.

### How I Would Request These Changes

I would open an Order Domain contract change request with:

- the proposed JSON schemas;
- the correctness requirement for echoing `orderId`;
- examples of duplicate delivery and replay scenarios;
- the deploy order needed to keep changes backward compatible;
- a note that `planId` and `executionId` are useful for observability, while `orderId` is mandatory for correctness.

### Alternative Approaches if the Order Domain Cannot Add Fields

**If `orderId` cannot be echoed back:**
`orderId` is the only field that is strictly required for correctness. Without it, IVP has no reliable way to correlate an order result to a `plan_order` row. Alternatives:

- Agree on a deterministic ID generation scheme (e.g. `UUID.nameUUIDFromBytes(planId + executionId + instrument)`) so IVP can recompute the key without it being echoed. A deterministic scheme could also be applied to the `commandId` on outbound `CREATE_ORDER` messages if a future retry/outbox mechanism reloads and resends orders; the current implementation keeps `commandId` random because there is no resend path without an outbox.
- Ask the Order Domain to return a `correlationId` field that IVP populates on the command; this is functionally equivalent to echoing `orderId`.

**If `planId` and `executionId` cannot be echoed back:**
These are optional for correctness. The `plan_order` table already maps `orderId → planId + executionId` internally. IVP only needs `orderId` to look up the rest. The `planId` and `executionId` fields in `OrderExecuted`/`OrderFailed` exist solely for traceability and can be omitted without affecting business logic.

**If `eventId` cannot be added:**
Without a stable event identifier, message-level deduplication must use a composite key. Options:

- Use `(orderId, event_type)` as the deduplication key. This works if each order produces at most one terminal result (which is the contract — an order cannot be both executed and failed).
- Track Kafka partition and offset in the `processed_message` table instead of a business-level key. This is less portable but does not require schema changes from the Order Domain.
- Accept that duplicate events may produce extra `PLAN_ORDER_FILLED`/`PLAN_ORDER_REJECTED` events on `ivp-events`, and require downstream read-model consumers to be idempotent on those events.

## Domain Model

| Entity/Table | Purpose |
| --- | --- |
| `investment_plan` | Plan aggregate: user, name, execution day, status, timestamps. |
| `plan_investment` | Instruments and amounts configured inside a plan. |
| `plan_execution` | One execution instance for a plan on a date. |
| `plan_order` | One generated order for one investment in one execution. |
| `processed_message` | Idempotency table for processed commands/events. |

Main states and transitions:

| Type | Values | Transitions |
| --- | --- | --- |
| `PlanStatus` | `ACTIVE`, `DELETED` | `ACTIVE → DELETED` on `DELETE_PLAN` (irreversible) |
| `ExecutionStatus` | `STARTED`, `COMPLETED` | `STARTED → COMPLETED` when all orders are terminal (irreversible) |
| `OrderStatus` | `PENDING`, `FILLED`, `REJECTED` | `PENDING → FILLED` on `ORDER_EXECUTED`; `PENDING → REJECTED` on `ORDER_FAILED`; terminal states cannot be overwritten |

Important constraints:

| Constraint | Where | Why |
| --- | --- | --- |
| Unique execution per plan/date | `plan_execution(plan_id, execution_date)` | Prevents duplicate execution for the same date. |
| Unique order id | `plan_order.order_id` | Makes order-result correlation reliable. |
| Composite idempotency key | `processed_message(message_type, message_id)` | Allows the same UUID in different message families without collision. |

## Business Rules

- A plan can be executed more than once, but not twice for the same `executionDate`.
- Each investment produces an independent order.
- One rejected order does not cancel or block other orders.
- `PLAN_EXECUTION_COMPLETED` is emitted only after every generated order has a result.
- Duplicate commands and duplicate order events are handled gracefully.
- Order terminal state is first-write-wins: once an order is `FILLED` or `REJECTED`, later conflicting results are ignored.
- Deleting a plan prevents future executions but does not cancel in-flight executions.

## Idempotency

Kafka is at-least-once. The service assumes duplicates can happen and handles them at two levels.

### Message-Level Idempotency

The `processed_message` table stores `(message_type, message_id)` and uses PostgreSQL `ON CONFLICT DO NOTHING`:

```sql
INSERT INTO processed_message(message_type, message_id, processed_at)
VALUES (:type, :id, now())
ON CONFLICT (message_type, message_id) DO NOTHING
```

If the insert returns zero rows, the command/event has already been processed and the handler exits.

This avoids a check-then-insert race. It also treats duplicates as normal data rather than relying on exception handling.

### Business-Level Idempotency

The database model adds extra guards:

- `plan_execution` is unique by `(plan_id, execution_date)`.
- `plan_order.order_id` is unique.
- `PlanOrder` terminal states cannot be overwritten.
- duplicate order-result events with the same `eventId` are ignored.
- duplicate or conflicting order-result events for a terminal order are ignored.

### Idempotency Policy Asymmetry

Two different policies are applied depending on whether the correlated entity is expected to exist:

| Case | Policy | Reason |
| --- | --- | --- |
| `EXECUTE_PLAN` — plan not found | `commandId` **is** recorded | The plan is assumed to have been processed and the execute is a late/stale command. Permanently consuming the `commandId` prevents indefinite retries against a plan that will never exist. |
| `ORDER_EXECUTED` / `ORDER_FAILED` — order not found | `eventId` **is not** recorded | The `plan_order` row is always committed before `CREATE_ORDER` is published (the publish is in `afterCommit`), so a missing order is unexpected and likely a transient race. Not recording the `eventId` allows a genuine retry to succeed once the order row is visible. |

### Sender Contract

`commandId` is sender-owned. Upstream producers must reuse the same `commandId` when retrying the same logical command. If they generate a new UUID for every retry, IVP cannot distinguish a retry from a new request.

For Scheduler retries, a deterministic `commandId` per `(planId, executionDate)` is recommended (noted in the Execute Plan contract section above).

Note: IVP's own `CREATE_ORDER` `commandId` is currently random. Making it deterministic (e.g. `UUID.nameUUIDFromBytes(orderId)`) is only useful when a retry mechanism exists — specifically the transactional outbox. Without the outbox, there is no path that reloads `plan_order` rows and resends, so determinism provides no benefit today.

## Concurrency and Completion

The risky case is concurrent order results for the same execution. Two consumer threads may process the final two orders at almost the same time. Without coordination, both could observe all orders as terminal and both could emit `PLAN_EXECUTION_COMPLETED`.

The service handles this by locking the `plan_execution` row during the completion check:

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT e FROM PlanExecution e WHERE e.id = :id")
Optional<PlanExecution> findByIdWithLock(UUID id);
```

Only one transaction can complete the execution at a time. The second transaction sees `COMPLETED` and exits.

There is an integration test that invokes the final order updates concurrently and verifies exactly one `PLAN_EXECUTION_COMPLETED` event is emitted.

The duplicate execution guard uses a separate test that bypasses the service's plan-row lock entirely, so both threads race to call `insertIfAbsent` at the same time inside their own transactions. This exercises `ON CONFLICT DO NOTHING` under genuine DB-level concurrency rather than relying on the lock to serialize the threads before they reach the insert.

## Persistence Decisions

PostgreSQL was chosen instead of in-memory storage because the important correctness properties are database-backed:

- unique execution per plan/date;
- unique order correlation id;
- idempotency through `ON CONFLICT DO NOTHING`;
- row-level lock for execution completion.

The application uses Hibernate `ddl-auto: update` for local development and `create-drop` in tests. In production, this should move to Flyway or Liquibase migrations.

## Transaction Boundaries and Kafka Publishing

Service handlers are transactional. Database state changes happen inside `@Transactional` methods. Kafka messages are published in `afterCommit` callbacks so events are not emitted for database transactions that later roll back.

Send futures are observed with `whenComplete` callbacks so publish failures are logged with the exception. IVP event sends (PLAN_CREATED, PLAN_DELETED, order results, execution completion) also log on successful broker acknowledgement. CREATE_ORDER sends log a dispatch summary immediately after initiating the batch; per-command failures are logged via the same callback.

This is intentionally simple for the assignment, but it leaves a known reliability gap: the database transaction can commit and the application can crash before the Kafka publish completes. The production fix is the transactional outbox pattern.

## Invalid and Edge Case Handling

Known invalid and malformed messages are logged and swallowed. Unexpected infrastructure failures (database errors, lock timeouts) are rethrown so Spring Kafka's error handler can retry them.

| Case | Behavior |
| --- | --- |
| invalid `CREATE_PLAN` | commandId recorded, then swallowed; no plan/event created (missing commandId swallowed before insert) |
| invalid `EXECUTE_PLAN` | commandId recorded, then swallowed (missing commandId swallowed before insert) |
| invalid `DELETE_PLAN` | commandId recorded, then swallowed (missing commandId swallowed before insert) |
| malformed JSON on any topic | swallowed, no retry |
| unknown command/event type | logged and swallowed |
| unknown plan on delete | logged, no event emitted |
| unknown plan on execute | logged, no orders created |
| execute deleted plan | logged, no orders created |
| duplicate execution date | skipped by idempotent insert (`ON CONFLICT DO NOTHING`) or pre-check |
| unknown order result | logged, no state change |
| duplicate order result | skipped by `processed_message` or terminal order state |
| conflicting order result | first terminal state wins |

## Build, Run, and Test

### Prerequisites

- Java 21+
- Maven 3.8+
- Docker Desktop or Docker Engine

### Run Tests

The integration tests use Testcontainers for Kafka and PostgreSQL:

```bash
mvn test
```

### Start Local Infrastructure

```bash
docker compose up -d
```

This starts:

- PostgreSQL 16 on `localhost:5432`
- Kafka `confluentinc/cp-kafka:7.6.0` on `localhost:9092`

### Run the Application

```bash
mvn spring-boot:run
```

Default local configuration:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/ivp
    username: ivp
    password: ivp
  kafka:
    bootstrap-servers: localhost:9092
```

### Stop Local Infrastructure

```bash
docker compose down
```

## Test Coverage

The test suite uses real Kafka and PostgreSQL through Testcontainers.

Covered flows include:

- plan creation happy path;
- plan creation validation;
- plan deletion;
- duplicate delete command;
- deleting an already deleted plan;
- delete unknown plan;
- invalid delete command;
- plan execution;
- `CREATE_ORDER` payload and key verification;
- duplicate execution command/date;
- execute deleted plan;
- execute unknown plan;
- execute before create;
- null execution date;
- all orders filled;
- all orders rejected;
- mixed filled/rejected;
- duplicate order result;
- terminal order result cannot be overwritten;
- unknown order result;
- genuine concurrent insert for the same plan and date (repository-level, no plan lock in the path, exercises `ON CONFLICT DO NOTHING` under real DB-level concurrency);
- exactly one completion event under concurrent final order results;
- exactly one order-result event when ORDER_EXECUTED and ORDER_FAILED race for the same orderId.

## Trade-offs

| Trade-off | Decision |
| --- | --- |
| Kafka-only service | Matches assignment and CQRS context; no REST API added. |
| `type` in JSON body | Self-describing and simple for tests; headers would be cleaner in a controlled production ecosystem. |
| PostgreSQL over in-memory | Required for realistic idempotency, uniqueness, and locking behavior. |
| Direct after-commit publish | Good enough for assignment scope; `whenComplete` logs failures; outbox is the production upgrade. |
| Soft delete | Preserves audit history and avoids unsafe cancellation semantics for already-sent orders. |
| No trading-day validation | Scheduler owns calendar logic. IVP validates presence only. |
| No Lombok | Explicit getters avoid annotation processor issues and keep Java 21+ builds straightforward. |

## What I Would Add With More Time

1. Transactional outbox for all outbound Kafka messages.
2. Flyway or Liquibase migrations instead of Hibernate schema generation.
3. Dead-letter topics for permanently invalid messages.
4. Explicit retry/backoff policy for transient infrastructure failures (Spring Kafka's default handler retries 10 times with no delay; a tuned `DefaultErrorHandler` with `FixedBackOff` and a DLT would be more production-appropriate).
5. Schema version fields and a schema registry for message contracts.
6. `correlationId` propagation across commands, events, and logs for production tracing.
7. Contract tests against API Service, Scheduler, and Order Domain schemas.
8. Metrics and tracing for command processing, duplicate counts, and event publish latency.
9. More precise validation failure handling, possibly with invalid-command events or DLQ records.
10. Optimistic or conditional database updates for completion if lock contention becomes an issue.
11. A deterministic Scheduler `commandId` strategy for execution retries.

## Use of AI Tools

Claude Code was used throughout as a sounding board and accelerator, not as the author of the design.

| Area | How AI was used |
| --- | --- |
| Design validation | Discussed trade-offs I had already identified — soft delete vs hard delete, PostgreSQL vs H2, direct publish vs outbox |
| Edge case pressure-testing | Asked AI to challenge my idempotency approach; it surfaced the concurrent final order result race and the asymmetric `eventId` policy |
| Contract decisions | Used AI to think through which fields `CreateOrder` needed and why — particularly whether `orderId` correlation was strictly required vs nice-to-have |
| Test scaffolding | Wrote the `CountDownLatch` boilerplate for concurrency tests once I knew what scenario I wanted to cover |
| README review | Wrote a draft and used AI to identify gaps and improve structure |

Sample prompts:

- "I'm building a Kafka consumer that handles `CreatePlan`, `DeletePlan`, and `ExecutePlan` on a single topic using a `type` field in the JSON body for routing. Is there a better approach, and what are the trade-offs?"
- "For idempotency I'm planning to use an `ON CONFLICT DO NOTHING` insert into a `processed_message` table. Is there a race condition I'm missing compared to a check-then-insert approach?"
- "I need to ensure `PLAN_EXECUTION_COMPLETED` is emitted exactly once even if two order results arrive concurrently. I'm thinking pessimistic write lock on `plan_execution` — does this hold under concurrent transactions?"
- "For the `CreateOrder` command, what fields do I need to add beyond the minimum schema so IVP can correlate order results back correctly?"
- "Review this README section on idempotency — does it accurately explain the asymmetric policy for order events?"

What I decided independently: the domain model, the two-layer idempotency approach, the pessimistic locking strategy, soft delete, and the transaction/`afterCommit` boundary.

What worked well:

- Faster discovery of concurrency and idempotency edge cases through back-and-forth discussion.
- Pressure-testing `ON CONFLICT DO NOTHING` versus exception-based duplicate handling surfaced a cleaner design.
- Validating the `orderDirection` decision and cross-domain contract extensions against real correctness requirements.

Limitations:

- AI suggestions needed verification against the actual code and assignment constraints.
- Some production-grade ideas such as the transactional outbox and schema registry were correctly identified as out of scope and kept as future improvements.
