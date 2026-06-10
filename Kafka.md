# FinLedgerX — Kafka Architecture

> **Principal Architect View** | Apache Kafka 3.x · Spring Boot 3 · Confluent Schema Registry
>
> This document is the single source of truth for all Kafka topics, message schemas, consumer group configuration, retry topology, and DLQ strategy. Reference when writing producers, consumers, and KafkaConfig classes.

---

## Table of Contents

1. [Topic Inventory](#1-topic-inventory)
2. [Producer Configuration](#2-producer-configuration)
3. [Consumer Configuration](#3-consumer-configuration)
4. [Message Schemas (All 6 Events)](#4-message-schemas)
5. [Retry Topology](#5-retry-topology)
6. [Dead Letter Queue Strategy](#6-dead-letter-queue-strategy)
7. [Event Flow — Step by Step](#7-event-flow--step-by-step)
8. [Kafka Spring Boot Configuration](#8-kafka-spring-boot-configuration)
9. [Topic Creation (Docker Compose / AdminClient)](#9-topic-creation)

---

## 1. Topic Inventory

### Core Topics

| Topic | Partitions | Key | Retention | Publisher | Consumers |
|---|---|---|---|---|---|
| `payment.events` | 3 | `fromAccountId` | 7 days | Payment Service (via Outbox) | Ledger, Notification |
| `ledger.entries` | 3 | `accountId` | 30 days | Ledger Service | Recon, Notification |
| `notification.events` | 3 | `userId` | 3 days | Notification Service | Audit consumer |
| `reconciliation.events` | 1 | `runDate` | 30 days | Recon Service | Ops Alert consumer |

### Retry Topics

| Topic | Partitions | Delay | Parent Topic |
|---|---|---|---|
| `payment.events.retry-1` | 1 | 5 seconds | `payment.events` |
| `payment.events.retry-2` | 1 | 30 seconds | `payment.events.retry-1` |
| `payment.events.retry-3` | 1 | 2 minutes | `payment.events.retry-2` |
| `ledger.entries.retry-1` | 1 | 5 seconds | `ledger.entries` |
| `ledger.entries.retry-2` | 1 | 30 seconds | `ledger.entries.retry-1` |
| `notification.events.retry-1` | 1 | 10 seconds | `notification.events` |
| `reconciliation.events.retry-1` | 1 | 1 minute | `reconciliation.events` |

### Dead Letter Queue Topics

| Topic | Partitions | Retention | Alert Severity |
|---|---|---|---|
| `payment.events.dlq` | 1 | 90 days | P1 — PagerDuty |
| `ledger.entries.dlq` | 1 | 90 days | **P0 — Immediate** 🚨 |
| `notification.events.dlq` | 1 | 90 days | P2 — Slack |
| `reconciliation.events.dlq` | 1 | 90 days | P1 — PagerDuty |

### Why these partition counts?

- **3 partitions** = matches service concurrency (3 consumer threads = 3 partitions = maximum parallelism). Each thread owns one partition — no cross-thread coordination needed.
- **Key = accountId / fromAccountId** = messages for the same account always land in the same partition, guaranteeing ordering per account. Alice's INITIATED and SETTLED events are never processed out of order.
- **1 partition for reconciliation** = recon is a single batch job. No parallel consumption needed. Order matters for the run_date sequence.
- **Retry topics: 1 partition** = retries don't need parallelism. Order within retries is irrelevant — what matters is eventual success.

---

## 2. Producer Configuration

```java
// KafkaProducerConfig.java
@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerProps());
    }

    private Map<String, Object> producerProps() {
        return Map.of(
            // Connection
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,            bootstrapServers,

            // Serialization
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,         StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,       JsonSerializer.class,

            // Durability — strongest guarantees
            ProducerConfig.ACKS_CONFIG,                         "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,           true,
            ProducerConfig.RETRIES_CONFIG,                      3,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1,  // ordering with retries

            // Throughput tuning
            ProducerConfig.LINGER_MS_CONFIG,                    5,    // batch up to 5ms
            ProducerConfig.BATCH_SIZE_CONFIG,                   16384,
            ProducerConfig.COMPRESSION_TYPE_CONFIG,             "snappy",

            // Timeouts
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,           30000,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,          120000
        );
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        KafkaTemplate<String, Object> template =
            new KafkaTemplate<>(producerFactory());
        template.setObservationEnabled(true);  // Micrometer tracing
        return template;
    }
}
```

**Why `acks=all`?** The leader broker waits for all in-sync replicas to acknowledge the write before confirming to the producer. Combined with `min.insync.replicas=2`, this means at least 2 of 3 brokers must persist the message. A single broker failure cannot cause message loss.

**Why `enable.idempotence=true`?** The broker assigns each producer a PID and sequence number. Duplicate publishes (from producer retries) are deduplicated at the broker. Combined with `max.in.flight=1`, this gives exactly-once semantics at the producer level.

**Why `max.in.flight=1`?** Without this, a retry of batch N could arrive after batch N+1, reordering messages for the same partition key. Setting to 1 sacrifices some throughput for strict ordering per key.

---

## 3. Consumer Configuration

```java
// KafkaConsumerConfig.java
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ── Core consumer factory ──────────────
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());

        // Offset management — manual commit is critical for financial data
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");

        // Isolation level: only read committed messages (from transactions)
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,          "read_committed");

        // Poll tuning
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         50);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,     300000);  // 5 min for slow DB writes
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,    10000);

        // Rebalance strategy — cooperative avoids stop-the-world rebalance
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            CooperativeStickyAssignor.class.getName());

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ── Listener container factory (for financial services) ──
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    kafkaListenerContainerFactory() {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);                                 // 1 thread per partition
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(retryErrorHandler());
        factory.getContainerProperties().setObservationEnabled(true);  // Micrometer
        return factory;
    }

    // ── Error handler with retry chain ────────────────────────
    @Bean
    public DefaultErrorHandler retryErrorHandler() {
        // Exponential backoff: 1s, 5s, 30s then DLQ
        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 3.0);
        backOff.setMaxAttempts(3);
        backOff.setMaxInterval(30000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(
            deadLetterPublishingRecoverer(), backOff
        );

        // Non-retryable exceptions go straight to DLQ — no backoff
        handler.addNotRetryableExceptions(
            LedgerImbalanceException.class,    // math is wrong — retrying won't help
            DeserializationException.class,    // corrupt message — retrying won't help
            InvalidStateTransitionException.class
        );
        return handler;
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer() {
        return new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            // Route to <topic>.dlq partition 0
            (record, exception) -> new TopicPartition(
                record.topic() + ".dlq", 0
            )
        );
    }
}
```

**Why `isolation.level=read_committed`?** When a Kafka transaction is aborted (producer failure mid-transaction), read_committed consumers will never see the partial messages. Without this, a consumer could read half a payment event and write half a journal entry.

**Why `MANUAL_IMMEDIATE` ack?** The offset is committed only after the service successfully persists to PostgreSQL. If the DB write fails, the offset is NOT committed — the message is reprocessed on the next poll. This is the critical guarantee: no ack without a successful DB write.

**Why `CooperativeStickyAssignor`?** The old eager rebalancer stops ALL consumers during a rebalance (even those whose partitions didn't change). Cooperative rebalancer only pauses the partitions being reassigned. In a financial system where a pause means delayed journal writes, this matters.

---

## 4. Message Schemas

All events implement a common `BaseEvent` contract. Headers carry tracing metadata so the full lifecycle of one payment can be reconstructed across four services from logs.

```java
// BaseEvent.java — all events extend this
public abstract class BaseEvent {
    private String eventId;          // UUID — globally unique per event
    private String eventType;        // "PAYMENT_INITIATED", "LEDGER_ENTRY_CREATED", etc.
    private String correlationId;    // same UUID for all events triggered by one payment
    private String causationId;      // eventId of the event that caused this one
    private Instant occurredAt;      // when the event happened (not when it was published)
    private int schemaVersion;       // for backward compat; increment when schema changes
}
```

---

### Event 1: PaymentInitiated

**Published by:** Payment Service  
**Topic:** `payment.events`  
**Partition key:** `fromAccountId`  
**Trigger:** When `POST /api/v1/payments` is persisted with status `INITIATED`

```json
{
  "eventId":       "evt-uuid-001",
  "eventType":     "PAYMENT_INITIATED",
  "correlationId": "corr-uuid-abc",
  "causationId":   null,
  "occurredAt":    "2026-06-03T10:00:00.000Z",
  "schemaVersion": 1,

  "paymentId":      "pay-uuid-001",
  "fromAccountId":  "acc-uuid-alice",
  "toAccountId":    "acc-uuid-bob",
  "amount":         "1000.00",
  "currency":       "INR",
  "description":    "Invoice INV-2026-001",
  "idempotencyKey": "idem-key-uuid-123",
  "initiatedBy":    "user-uuid-alice",
  "metadata": {
    "invoiceId": "INV-2026-001",
    "channel":   "MOBILE_APP"
  }
}
```

**Consumed by:**
- `ledger-service-cg` → creates HOLD journal entry for `fromAccountId`
- `notification-service-cg` → sends "Payment initiated" email to Alice

---

### Event 2: PaymentCompleted

**Published by:** Payment Service  
**Topic:** `payment.events`  
**Partition key:** `fromAccountId`  
**Trigger:** External rail confirms settlement; payment transitions to `SETTLED`

```json
{
  "eventId":       "evt-uuid-007",
  "eventType":     "PAYMENT_COMPLETED",
  "correlationId": "corr-uuid-abc",
  "causationId":   "evt-uuid-001",
  "occurredAt":    "2026-06-03T10:00:45.000Z",
  "schemaVersion": 1,

  "paymentId":       "pay-uuid-001",
  "fromAccountId":   "acc-uuid-alice",
  "toAccountId":     "acc-uuid-bob",
  "amount":          "1000.00",
  "netAmount":       "997.00",
  "feeAmount":       "3.00",
  "currency":        "INR",
  "settlementRef":   "HDFC-TXN-00001",
  "settledAt":       "2026-06-03T10:00:45.000Z"
}
```

**Consumed by:**
- `ledger-service-cg` → releases HOLD, posts DEBIT(Alice) + CREDIT(Bob) + CREDIT(Fee)
- `notification-service-cg` → sends "Payment successful" to Alice + Bob

---

### Event 3: PaymentFailed

**Published by:** Payment Service  
**Topic:** `payment.events`  
**Partition key:** `fromAccountId`  
**Trigger:** Validation failure, fraud rejection, rail timeout, or insufficient funds

```json
{
  "eventId":       "evt-uuid-099",
  "eventType":     "PAYMENT_FAILED",
  "correlationId": "corr-uuid-xyz",
  "causationId":   "evt-uuid-098",
  "occurredAt":    "2026-06-03T11:30:00.000Z",
  "schemaVersion": 1,

  "paymentId":      "pay-uuid-009",
  "fromAccountId":  "acc-uuid-charlie",
  "toAccountId":    "acc-uuid-diana",
  "amount":         "5000.00",
  "currency":       "INR",
  "failureCode":    "INSUFFICIENT_FUNDS",
  "failureReason":  "Available balance ₹2500 is less than requested ₹5000",
  "failedAt":       "2026-06-03T11:30:00.000Z",
  "retryable":      false
}
```

**Consumed by:**
- `ledger-service-cg` → releases HOLD (if one existed), no debit posted
- `notification-service-cg` → sends "Payment failed" email with reason

**`retryable` field:** If `true`, the Payment Service will automatically retry via the payment rail. If `false` (e.g., insufficient funds), no retry is attempted.

---

### Event 4: LedgerEntryCreated

**Published by:** Ledger Service  
**Topic:** `ledger.entries`  
**Partition key:** `accountId`  
**Trigger:** After a double-entry transaction is successfully written to `journal_entries`

```json
{
  "eventId":       "evt-uuid-010",
  "eventType":     "LEDGER_ENTRY_CREATED",
  "correlationId": "corr-uuid-abc",
  "causationId":   "evt-uuid-007",
  "occurredAt":    "2026-06-03T10:00:46.000Z",
  "schemaVersion": 1,

  "transactionId": "txn-uuid-001",
  "entries": [
    {
      "entryId":       "entry-uuid-001",
      "accountId":     "acc-uuid-alice",
      "entryType":     "DEBIT",
      "amount":        "1000.00",
      "currency":      "INR",
      "runningBalance": "4000.00"
    },
    {
      "entryId":       "entry-uuid-002",
      "accountId":     "acc-uuid-bob",
      "entryType":     "CREDIT",
      "amount":        "997.00",
      "currency":      "INR",
      "runningBalance": "1997.00"
    },
    {
      "entryId":       "entry-uuid-003",
      "accountId":     "sys-acc-fees",
      "entryType":     "CREDIT",
      "amount":        "3.00",
      "currency":      "INR",
      "runningBalance": "153.00"
    }
  ],
  "netBalance":    "0.00",
  "referenceId":   "pay-uuid-001",
  "referenceType": "PAYMENT"
}
```

**Consumed by:**
- `recon-service-cg` → registers entries for T+1 reconciliation batch
- `notification-service-cg` (conditional) → fires alert if any single entry > ₹50,000

---

### Event 5: ReconciliationCompleted

**Published by:** Reconciliation Service  
**Topic:** `reconciliation.events`  
**Partition key:** `runDate`  
**Trigger:** Daily batch job completes (02:00 AM)

```json
{
  "eventId":       "evt-uuid-500",
  "eventType":     "RECONCILIATION_COMPLETED",
  "correlationId": "recon-run-uuid-001",
  "causationId":   null,
  "occurredAt":    "2026-06-03T02:15:00.000Z",
  "schemaVersion": 1,

  "reconRunId":      "run-uuid-001",
  "runDate":         "2026-06-02",
  "status":          "COMPLETED",
  "totalLedger":     1250,
  "totalBank":       1250,
  "matchedCount":    1247,
  "mismatchCount":   3,
  "durationMs":      900000,
  "triggeredBy":     "SCHEDULER",
  "mismatches": [
    {
      "mismatchId":   "mm-uuid-001",
      "type":         "MISSING_IN_LEDGER",
      "bankRef":      "HDFC-TXN-GHOST",
      "bankAmount":   "999.00",
      "currency":     "INR"
    }
  ]
}
```

**Consumed by:**
- `ops-alert-cg` → if `mismatchCount > 0`, fires Grafana P1 alert + Slack notification
- `notification-service-cg` → sends reconciliation summary email to Finance team

---

### Event 6: NotificationTriggered

**Published by:** Notification Service  
**Topic:** `notification.events`  
**Partition key:** `userId`  
**Trigger:** After Notification Service dispatches a message to any channel

```json
{
  "eventId":       "evt-uuid-200",
  "eventType":     "NOTIFICATION_TRIGGERED",
  "correlationId": "corr-uuid-abc",
  "causationId":   "evt-uuid-007",
  "occurredAt":    "2026-06-03T10:00:47.000Z",
  "schemaVersion": 1,

  "notificationId": "notif-uuid-001",
  "userId":         "user-uuid-alice",
  "referenceId":    "pay-uuid-001",
  "referenceType":  "PAYMENT",
  "channel":        "EMAIL",
  "eventType":      "PAYMENT_COMPLETED",
  "recipient":      "alice@example.com",
  "status":         "SENT",
  "deliveryMs":     340
}
```

**Consumed by:**
- Audit consumer → updates `delivery_log.status` and `sent_at`
- Analytics consumer (future) → notification delivery metrics

---

## 5. Retry Topology

### The Three-Tier Retry Pattern

```
Consumer receives event from payment.events
           │
           ▼
    Process (DB write, etc.)
           │
     ┌─────┴──────┐
   success      exception?
     │               │
   ACK          Is retryable?
                     │
            ┌────────┴────────┐
          NO (e.g.          YES (transient
       LedgerImbalance)      DB error)
            │                    │
         → DLQ             Attempt 1 failed
         directly               │
                          →  retry-1 (5s delay)
                               Attempt 2 failed
                          →  retry-2 (30s delay)
                               Attempt 3 failed
                          →  retry-3 (2min delay)
                               Attempt 4 failed
                          →  DLQ (give up)
```

```java
// RetryTopicConfig.java — using Spring Kafka's @RetryableTopic
@Component
public class LedgerPaymentConsumer {

    @RetryableTopic(
        attempts = "4",                                      // 1 original + 3 retries
        backoff = @Backoff(delay = 5000, multiplier = 6.0, maxDelay = 120000),
        // delay: 5s → 30s → 120s
        dltStrategy = DltStrategy.FAIL_ON_ERROR,
        kafkaTemplate = "kafkaTemplate",
        include = {TransientDataAccessException.class,       // DB connection issue
                   KafkaException.class},                    // transient Kafka issue
        exclude = {LedgerImbalanceException.class,           // non-retryable
                   DeserializationException.class}
    )
    @KafkaListener(topics = "payment.events", groupId = "ledger-service-cg")
    public void consume(@Payload PaymentEvent event, Acknowledgment ack) {
        ledgerService.postDoubleEntry(buildTransaction(event));
        ack.acknowledge();
    }

    @DltHandler
    public void handleDlt(PaymentEvent event,
                          @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {
        log.error("CRITICAL: Event reached DLQ. topic={} paymentId={} error={}",
            topic, event.getPaymentId(), exceptionMessage);

        // Fire P0 alert for ledger DLQ
        alertService.fireAlert(AlertSeverity.P0,
            "Ledger DLQ: paymentId=" + event.getPaymentId());

        // Persist to DLQ audit table for manual review
        dlqRepository.save(new DlqRecord(event, topic, exceptionMessage));
    }
}
```

### Header-Based Retry Metadata

Spring Kafka automatically adds these headers on each retry attempt:

| Header | Value |
|---|---|
| `kafka_dlt-original-topic` | `payment.events` |
| `kafka_dlt-original-partition` | `0` |
| `kafka_dlt-original-offset` | `12345` |
| `kafka_dlt-exception-message` | `Connection refused to PostgreSQL` |
| `kafka_dlt-exception-stacktrace` | Full stacktrace (truncated at 4KB) |
| `x-correlation-id` | Original correlationId — preserved across all retries |

---

## 6. Dead Letter Queue Strategy

### DLQ Consumer

```java
// DlqMonitorConsumer.java
@Component
@Slf4j
public class DlqMonitorConsumer {

    @KafkaListener(
        topics = {"payment.events.dlq", "ledger.entries.dlq",
                  "notification.events.dlq", "reconciliation.events.dlq"},
        groupId = "dlq-monitor-cg"
    )
    public void consumeDlq(
            @Payload String rawPayload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = "kafka_dlt-original-topic", required = false) String originalTopic,
            @Header(value = "kafka_dlt-exception-message", required = false) String error,
            @Header(value = "x-correlation-id", required = false) String correlationId,
            Acknowledgment ack) {

        AlertSeverity severity = resolveSeverity(topic);

        log.error("DLQ event received. topic={} originalTopic={} correlationId={} error={}",
            topic, originalTopic, correlationId, error);

        // 1. Persist to dlq_records table for Ops dashboard
        dlqRecordRepository.save(DlqRecord.builder()
            .topic(topic)
            .originalTopic(originalTopic)
            .rawPayload(rawPayload)
            .errorMessage(error)
            .correlationId(correlationId)
            .receivedAt(Instant.now())
            .build());

        // 2. Fire alert
        alertService.fire(AlertSeverity.builder()
            .severity(severity)
            .title("Kafka DLQ: " + topic)
            .body("correlationId=" + correlationId + " error=" + error)
            .build());

        ack.acknowledge();
    }

    private AlertSeverity resolveSeverity(String topic) {
        if (topic.contains("ledger"))          return AlertSeverity.P0;  // financial data
        if (topic.contains("payment"))         return AlertSeverity.P1;
        if (topic.contains("reconciliation"))  return AlertSeverity.P1;
        return AlertSeverity.P2;                                          // notification
    }
}
```

### DLQ Replay

When an Ops engineer fixes the root cause (e.g., a DB schema migration), messages in the DLQ can be replayed:

```java
// DlqReplayService.java
@Service
public class DlqReplayService {

    public void replayFromDlq(String dlqTopic, String targetTopic, int maxMessages) {
        String consumerGroupId = "dlq-replay-" + UUID.randomUUID();

        // Create a temporary consumer, read from DLQ
        // Re-publish each message to the original topic with cleaned headers
        // This is a manual, admin-triggered operation — never automated
    }
}
```

---

## 7. Event Flow — Step by Step

### Happy Path: Alice pays Bob ₹1000

```
Step 1  Client → POST /api/v1/payments (X-Idempotency-Key: uuid-123)

Step 2  Payment Service:
        a. Check Redis for idempotency key uuid-123 → cache miss
        b. Validate: amount OK, accounts differ, currency supported
        c. Check velocity (Redis): Alice under 10 payments/min limit
        d. Begin DB transaction:
              INSERT INTO payments (status='AUTHORIZED', ...)
              INSERT INTO outbox_events (event_type='PAYMENT_INITIATED', published=false, ...)
           COMMIT

Step 3  Return 202 Accepted { paymentId: "pay-uuid-001" }

Step 4  OutboxPoller (every 500ms):
        SELECT * FROM outbox_events WHERE published=false
        → Publish to Kafka: payment.events
          key=acc-uuid-alice, value=PaymentInitiated{...}
        → UPDATE outbox_events SET published=true

Step 5  Ledger Service consumes PaymentInitiated:
        → ledger-service-cg reads from payment.events partition 0
        → postDoubleEntry: INSERT journal_entry (HOLD, alice, 1000)
        → UPDATE account_holds (status=ACTIVE)
        → ack.acknowledge() ← offset committed ONLY after DB write

Step 6  Notification Service consumes PaymentInitiated:
        → notification-service-cg reads from payment.events
        → render template PAYMENT_INITIATED
        → send email to alice@example.com
        → INSERT delivery_log (status=SENT)

Step 7  Payment Service calls external payment rail (NEFT/UPI):
        → rail returns SUCCESS, settlementRef=HDFC-TXN-00001
        → UPDATE payments SET status=SETTLED
        → INSERT outbox_events (PAYMENT_COMPLETED, published=false)
        COMMIT
        → OutboxPoller publishes PaymentCompleted to payment.events

Step 8  Ledger Service consumes PaymentCompleted:
        → Release HOLD: UPDATE account_holds SET status=RELEASED
        → postDoubleEntry:
              DEBIT  alice  1000.00  (payment)
              CREDIT bob     997.00  (settlement)
              CREDIT fees      3.00  (platform fee)
        → Verify net = 0 ✓
        → Publish LedgerEntryCreated to ledger.entries

Step 9  Notification Service consumes PaymentCompleted:
        → send "Payment successful" to alice@example.com
        → send "You received ₹997" to bob@example.com
        → Publish NotificationTriggered ×2 to notification.events

Step 10 Recon Service (02:00 AM next day, T+1):
        → Consume from ledger.entries: batch read Alice's June 3 entries
        → Ingest bank statement CSV from HDFC SFTP
        → Run match algorithm: HDFC-TXN-00001 ↔ entry-uuid-001 ✓ matched
        → Publish ReconciliationCompleted { matched=1247, mismatches=3 }

Step 11 Ops Alert Consumer consumes ReconciliationCompleted:
        → mismatch_count=3 > 0 → fire P1 Grafana alert
        → Post to #ops-reconciliation Slack channel
```

### Failure Path: Transient DB error in Ledger Service

```
Step 5a Ledger Service consumes PaymentInitiated
        → postDoubleEntry throws PSQLException (connection timeout)
        → DefaultErrorHandler catches exception

Step 5b Retry 1 (after 5 seconds):
        Event re-consumed from payment.events.retry-1
        → DB still unavailable → exception thrown again

Step 5c Retry 2 (after 30 seconds):
        Event re-consumed from payment.events.retry-2
        → DB recovered → INSERT succeeds → ack.acknowledge()
        → DONE — no data loss, no duplicate

Step 5d If retry-3 also fails:
        → DeadLetterPublishingRecoverer routes to ledger.entries.dlq
        → DlqMonitorConsumer reads from DLQ
        → P0 PagerDuty alert fired immediately
        → DlqRecord saved to DB for Ops dashboard
        → Engineer investigates, fixes issue, triggers manual replay
```

---

## 8. Kafka Spring Boot Configuration

```yaml
# application.yml — Payment Service
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 1
        linger.ms: 5
        compression.type: snappy
    consumer:
      group-id: payment-outbox-cg
      auto-offset-reset: earliest
      enable-auto-commit: false
      properties:
        isolation.level: read_committed
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
    listener:
      ack-mode: MANUAL_IMMEDIATE
      concurrency: 3
      observation-enabled: true

# Retry topic properties
spring.kafka.retry.topic.enabled: true
spring.kafka.retry.topic.attempts: 4
```

```yaml
# application.yml — Ledger Service
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: ledger-service-cg
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 50
      properties:
        isolation.level: read_committed
        max.poll.interval.ms: 300000
        session.timeout.ms: 30000
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
    listener:
      ack-mode: MANUAL_IMMEDIATE
      concurrency: 3
```

---

## 9. Topic Creation

### AdminClient (Java — runs at startup)

```java
// KafkaTopicConfig.java — runs in all services
@Configuration
public class KafkaTopicConfig {

    // ── Core Topics ──────────────────────────────────────────
    @Bean public NewTopic paymentEventsTopic() {
        return TopicBuilder.name("payment.events")
            .partitions(3).replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG,        "604800000")   // 7d
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,"1")
            .config(TopicConfig.COMPRESSION_TYPE_CONFIG,    "snappy")
            .build();
    }

    @Bean public NewTopic ledgerEntriesTopic() {
        return TopicBuilder.name("ledger.entries")
            .partitions(3).replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG,        "2592000000")  // 30d
            .build();
    }

    @Bean public NewTopic notificationEventsTopic() {
        return TopicBuilder.name("notification.events")
            .partitions(3).replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG,        "259200000")   // 3d
            .build();
    }

    @Bean public NewTopic reconciliationEventsTopic() {
        return TopicBuilder.name("reconciliation.events")
            .partitions(1).replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG,        "2592000000")  // 30d
            .build();
    }

    // ── Retry Topics ─────────────────────────────────────────
    @Bean public NewTopic paymentRetry1() {
        return TopicBuilder.name("payment.events.retry-1").partitions(1).replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG, "604800000").build();
    }
    @Bean public NewTopic paymentRetry2() {
        return TopicBuilder.name("payment.events.retry-2").partitions(1).replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG, "604800000").build();
    }
    @Bean public NewTopic paymentRetry3() {
        return TopicBuilder.name("payment.events.retry-3").partitions(1).replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG, "604800000").build();
    }

    // ── DLQ Topics ────────────────────────────────────────────
    @Bean public NewTopic paymentDlq() {
        return TopicBuilder.name("payment.events.dlq").partitions(1).replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG,        "7776000000")  // 90d
            .config(TopicConfig.CLEANUP_POLICY_CONFIG,      "compact,delete")
            .build();
    }

    @Bean public NewTopic ledgerDlq() {
        return TopicBuilder.name("ledger.entries.dlq").partitions(1).replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG,        "7776000000")  // 90d
            .build();
    }

    @Bean public NewTopic notificationDlq() {
        return TopicBuilder.name("notification.events.dlq").partitions(1).replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG,        "7776000000").build();
    }

    @Bean public NewTopic reconciliationDlq() {
        return TopicBuilder.name("reconciliation.events.dlq").partitions(1).replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG,        "7776000000").build();
    }
}
```

### Docker Compose — Kafka init

```yaml
# docker-compose.yml (Kafka section)
kafka:
  image: confluentinc/cp-kafka:7.6.0
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_MIN_INSYNC_REPLICAS: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"   # never allow auto-create in production
    KAFKA_LOG_RETENTION_HOURS: 168

kafka-init:
  image: confluentinc/cp-kafka:7.6.0
  depends_on: [kafka]
  command: |
    bash -c "
      kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists
        --topic payment.events --partitions 3 --replication-factor 1
      kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists
        --topic ledger.entries --partitions 3 --replication-factor 1
      kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists
        --topic notification.events --partitions 3 --replication-factor 1
      kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists
        --topic reconciliation.events --partitions 1 --replication-factor 1
      kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists
        --topic payment.events.dlq --partitions 1 --replication-factor 1
      kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists
        --topic ledger.entries.dlq --partitions 1 --replication-factor 1
      kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists
        --topic notification.events.dlq --partitions 1 --replication-factor 1
      kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists
        --topic reconciliation.events.dlq --partitions 1 --replication-factor 1
    "
```

---

## Key Design Decisions Summary

| Decision | Choice | Why |
|---|---|---|
| Producer acks | `acks=all` | No message loss even if leader crashes immediately after publish |
| Idempotent producer | `true` | Prevents duplicate events from producer retries |
| Offset commit | `MANUAL_IMMEDIATE` | Offset committed only after DB write — no ack without persistence |
| Consumer isolation | `read_committed` | Never read partial/aborted messages |
| Partition assignment | `CooperativeSticky` | Avoids stop-the-world rebalance during scale-out |
| Partition key | `accountId` | Per-account ordering — INITIATED always processed before SETTLED |
| Retry strategy | `ExponentialBackOff` | Transient errors self-heal without flooding the system |
| DLQ severity | Ledger = P0, others P1/P2 | Journal write failure is financial data corruption |
| Retry count | 3 (ledger), 3 (payment) | Balance between resilience and latency |
| Topic retention | 7-30 days core, 90 days DLQ | Enables replay within a billing cycle; DLQ for audit |

---

*Last updated: 2026-06-03 | Companion to HLD.md, LLD.md, SCHEMA.md | FinLedgerX Architecture Team*
