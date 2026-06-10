# FinLedgerX — Low-Level Design (LLD)

> **Principal Architect View** | Java 21 · Spring Boot 3 · PostgreSQL · Kafka · Redis
>
> Reference this document when generating code. Each section maps directly to a Java class, interface, or SQL file.

---

## Table of Contents

1. [Payment Service](#1-payment-service)
2. [Ledger Service](#2-ledger-service)
3. [Notification Service](#3-notification-service)
4. [Reconciliation Service](#4-reconciliation-service)
5. [Shared Components](#5-shared-components)
6. [Global Error Handling](#6-global-error-handling)

---

## 1. Payment Service

**Port:** 8081 | **Database:** payments_db | **Module:** `payment-service`

---

### 1.1 REST APIs

| Method | Endpoint | Description | Auth |
|---|---|---|---|
| POST | `/api/v1/payments` | Initiate a new payment | JWT |
| GET | `/api/v1/payments/{paymentId}` | Get payment by ID | JWT |
| GET | `/api/v1/payments?accountId=&page=&size=` | List payments for account | JWT |
| POST | `/api/v1/payments/{paymentId}/cancel` | Cancel a PENDING payment | JWT |
| POST | `/api/v1/payments/{paymentId}/reverse` | Reverse a SETTLED payment | JWT |
| GET | `/api/v1/payments/{paymentId}/status` | Lightweight status poll | JWT |
| GET | `/actuator/health` | Health check | None |
| GET | `/actuator/prometheus` | Metrics scrape | Internal |

**Request — POST /api/v1/payments:**
```http
POST /api/v1/payments
Authorization: Bearer <jwt>
X-Idempotency-Key: <uuid>
Content-Type: application/json

{
  "fromAccountId": "acc-uuid-alice",
  "toAccountId":   "acc-uuid-bob",
  "amount":        "1000.00",
  "currency":      "INR",
  "description":   "Invoice payment #INV-2026-001",
  "metadata": {
    "invoiceId": "INV-2026-001"
  }
}
```

**Response — 202 Accepted:**
```json
{
  "paymentId":     "pay-uuid",
  "status":        "INITIATED",
  "fromAccountId": "acc-uuid-alice",
  "toAccountId":   "acc-uuid-bob",
  "amount":        "1000.00",
  "currency":      "INR",
  "createdAt":     "2026-06-03T10:00:00Z",
  "links": {
    "self":   "/api/v1/payments/pay-uuid",
    "status": "/api/v1/payments/pay-uuid/status"
  }
}
```

---

### 1.2 Database Tables

```sql
-- payments_db

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE payments (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key  VARCHAR(255) NOT NULL UNIQUE,
    from_account_id  UUID NOT NULL,
    to_account_id    UUID NOT NULL,
    amount           NUMERIC(19, 4) NOT NULL CHECK (amount > 0),
    currency         CHAR(3) NOT NULL,
    status           VARCHAR(30) NOT NULL DEFAULT 'INITIATED',
    description      TEXT,
    metadata         JSONB,
    failure_reason   TEXT,
    version          BIGINT NOT NULL DEFAULT 0,         -- optimistic locking
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settled_at       TIMESTAMPTZ,
    CONSTRAINT chk_status CHECK (status IN (
        'INITIATED','VALIDATING','AUTHORIZED',
        'PROCESSING','SETTLED','FAILED','REVERSED','CANCELLED'
    ))
);

CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_id    UUID NOT NULL,               -- paymentId
    aggregate_type  VARCHAR(50) NOT NULL,         -- 'PAYMENT'
    event_type      VARCHAR(80) NOT NULL,         -- 'PAYMENT_AUTHORIZED'
    payload         JSONB NOT NULL,
    topic           VARCHAR(120) NOT NULL,        -- 'payment.events'
    partition_key   VARCHAR(255),                 -- accountId for ordering
    published       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox_events(published, created_at) WHERE published = FALSE;

CREATE TABLE idempotency_keys (
    idempotency_key  VARCHAR(255) PRIMARY KEY,
    payment_id       UUID NOT NULL,
    response_body    JSONB NOT NULL,
    http_status      INT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at       TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '24 hours'
);
CREATE INDEX idx_idempotency_expires ON idempotency_keys(expires_at);

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_payments_updated_at
BEFORE UPDATE ON payments
FOR EACH ROW EXECUTE FUNCTION update_updated_at();
```

---

### 1.3 Entities

```java
// Payment.java
@Entity
@Table(name = "payments")
@EntityListeners(AuditingEntityListener.class)
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Column(name = "from_account_id", nullable = false)
    private UUID fromAccountId;

    @Column(name = "to_account_id", nullable = false)
    private UUID toAccountId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status = PaymentStatus.INITIATED;

    private String description;

    @Type(JsonType.class)
    @Column(columnDefinition = "jsonb")
    private Map<String, Object> metadata;

    private String failureReason;

    @Version
    private Long version;                        // optimistic lock

    @CreatedDate
    private Instant createdAt;

    @LastModifiedDate
    private Instant updatedAt;

    private Instant settledAt;

    // State machine transition guard
    public void transitionTo(PaymentStatus next) {
        if (!this.status.canTransitionTo(next)) {
            throw new InvalidStateTransitionException(this.status, next);
        }
        this.status = next;
        if (next == PaymentStatus.SETTLED) this.settledAt = Instant.now();
    }
}

// PaymentStatus.java
public enum PaymentStatus {
    INITIATED, VALIDATING, AUTHORIZED, PROCESSING, SETTLED, FAILED, REVERSED, CANCELLED;

    private static final Map<PaymentStatus, Set<PaymentStatus>> TRANSITIONS = Map.of(
        INITIATED,   Set.of(VALIDATING, FAILED),
        VALIDATING,  Set.of(AUTHORIZED, FAILED),
        AUTHORIZED,  Set.of(PROCESSING, CANCELLED),
        PROCESSING,  Set.of(SETTLED, FAILED),
        SETTLED,     Set.of(REVERSED),
        FAILED,      Set.of(),
        REVERSED,    Set.of(),
        CANCELLED,   Set.of()
    );

    public boolean canTransitionTo(PaymentStatus next) {
        return TRANSITIONS.getOrDefault(this, Set.of()).contains(next);
    }
}

// OutboxEvent.java
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Type(JsonType.class)
    @Column(columnDefinition = "jsonb", nullable = false)
    private Object payload;

    @Column(nullable = false)
    private String topic;

    private String partitionKey;

    private boolean published = false;

    private Instant createdAt = Instant.now();
    private Instant publishedAt;
}
```

---

### 1.4 DTOs

```java
// PaymentRequest.java (inbound)
public record PaymentRequest(
    @NotNull @UUID String fromAccountId,
    @NotNull @UUID String toAccountId,
    @NotNull @Positive @DecimalMin("0.01") BigDecimal amount,
    @NotBlank @Size(min=3, max=3) String currency,
    @Size(max=500) String description,
    Map<String, Object> metadata
) {}

// PaymentResponse.java (outbound)
public record PaymentResponse(
    UUID paymentId,
    PaymentStatus status,
    UUID fromAccountId,
    UUID toAccountId,
    BigDecimal amount,
    String currency,
    String description,
    Instant createdAt,
    Instant settledAt,
    String failureReason,
    Map<String, String> links
) {}

// PaymentStatusResponse.java (lightweight poll)
public record PaymentStatusResponse(
    UUID paymentId,
    PaymentStatus status,
    Instant updatedAt
) {}

// PaymentEvent.java (Kafka payload)
public record PaymentEvent(
    String eventId,
    String eventType,
    UUID paymentId,
    UUID fromAccountId,
    UUID toAccountId,
    BigDecimal amount,
    String currency,
    PaymentStatus status,
    String failureReason,
    Instant timestamp,
    String correlationId
) {}
```

---

### 1.5 Repository Layer

```java
// PaymentRepository.java
@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    @Query("SELECT p FROM Payment p WHERE p.fromAccountId = :accountId OR p.toAccountId = :accountId")
    Page<Payment> findByAccountId(@Param("accountId") UUID accountId, Pageable pageable);

    @Query("SELECT p FROM Payment p WHERE p.status = :status AND p.createdAt < :cutoff")
    List<Payment> findStalePayments(@Param("status") PaymentStatus status,
                                    @Param("cutoff") Instant cutoff);

    @Lock(LockModeType.OPTIMISTIC)
    @Query("SELECT p FROM Payment p WHERE p.id = :id")
    Optional<Payment> findByIdWithLock(@Param("id") UUID id);
}

// OutboxEventRepository.java
@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query("SELECT o FROM OutboxEvent o WHERE o.published = false ORDER BY o.createdAt ASC")
    List<OutboxEvent> findUnpublishedEvents(Pageable pageable);

    @Modifying
    @Query("UPDATE OutboxEvent o SET o.published = true, o.publishedAt = :now WHERE o.id IN :ids")
    void markPublished(@Param("ids") List<UUID> ids, @Param("now") Instant now);
}
```

---

### 1.6 Service Layer

```java
// PaymentService.java (interface)
public interface PaymentService {
    PaymentResponse initiatePayment(PaymentRequest request, String idempotencyKey, String userId);
    PaymentResponse getPayment(UUID paymentId);
    Page<PaymentResponse> getPaymentsByAccount(UUID accountId, Pageable pageable);
    PaymentResponse cancelPayment(UUID paymentId);
    PaymentResponse reversePayment(UUID paymentId);
    PaymentStatusResponse getStatus(UUID paymentId);
}

// PaymentServiceImpl.java
@Service
@Slf4j
@Transactional
public class PaymentServiceImpl implements PaymentService {

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxRepository;
    private final IdempotencyService idempotencyService;
    private final PaymentValidator paymentValidator;
    private final MeterRegistry meterRegistry;

    @Override
    public PaymentResponse initiatePayment(PaymentRequest request,
                                            String idempotencyKey,
                                            String userId) {

        // 1. Idempotency check (Redis-backed)
        return idempotencyService.executeIdempotent(idempotencyKey, () -> {

            // 2. Validate
            paymentValidator.validate(request);

            // 3. Build entity
            Payment payment = buildPayment(request, idempotencyKey);
            payment.transitionTo(PaymentStatus.VALIDATING);
            payment.transitionTo(PaymentStatus.AUTHORIZED);

            // 4. Create outbox entry in same transaction
            OutboxEvent event = buildOutboxEvent(payment, "PAYMENT_AUTHORIZED");

            // 5. Persist atomically
            paymentRepository.save(payment);
            outboxRepository.save(event);

            // 6. Metrics
            meterRegistry.counter("payment.initiated.total",
                "currency", request.currency()).increment();

            log.info("Payment initiated paymentId={} idempotencyKey={}",
                payment.getId(), idempotencyKey);

            return toResponse(payment);
        });
    }

    // OutboxPoller — separate @Scheduled component
    // Reads unpublished outbox rows and publishes to Kafka
}

// OutboxPoller.java
@Component
@Slf4j
public class OutboxPoller {

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedDelay = 500)   // poll every 500ms
    @Transactional
    public void pollAndPublish() {
        List<OutboxEvent> events = outboxRepository
            .findUnpublishedEvents(PageRequest.of(0, 100));

        events.forEach(event -> {
            kafkaTemplate.send(event.getTopic(), event.getPartitionKey(),
                               event.getPayload())
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        outboxRepository.markPublished(
                            List.of(event.getId()), Instant.now());
                    } else {
                        log.error("Outbox publish failed eventId={}", event.getId(), ex);
                    }
                });
        });
    }
}
```

---

### 1.7 Kafka Components

```java
// PaymentEventProducer.java
@Component
public class PaymentEventProducer {

    public static final String PAYMENT_EVENTS_TOPIC = "payment.events";

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    // Called by OutboxPoller — not directly by service
    public void publish(PaymentEvent event) {
        ProducerRecord<String, PaymentEvent> record =
            new ProducerRecord<>(PAYMENT_EVENTS_TOPIC,
                                 event.fromAccountId().toString(),  // partition by accountId
                                 event);
        record.headers().add("correlationId",
            event.correlationId().getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record);
    }
}

// KafkaConfig.java
@Configuration
public class KafkaConfig {

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka.bootstrap-servers}");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");            // strongest durability
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public NewTopic paymentEventsTopic() {
        return TopicBuilder.name("payment.events")
            .partitions(3)
            .replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG, "604800000") // 7 days
            .build();
    }

    @Bean
    public NewTopic paymentDlqTopic() {
        return TopicBuilder.name("payment.dlq").partitions(1).replicas(1).build();
    }
}
```

---

### 1.8 Validation Rules

```java
// PaymentValidator.java
@Component
public class PaymentValidator {

    private static final BigDecimal MAX_AMOUNT = new BigDecimal("10000000.00"); // 1 Crore
    private static final Set<String> SUPPORTED_CURRENCIES = Set.of("INR", "USD", "EUR", "GBP");

    public void validate(PaymentRequest request) {
        List<String> errors = new ArrayList<>();

        if (request.fromAccountId().equals(request.toAccountId()))
            errors.add("fromAccountId and toAccountId must be different");

        if (request.amount().compareTo(BigDecimal.ZERO) <= 0)
            errors.add("Amount must be greater than zero");

        if (request.amount().compareTo(MAX_AMOUNT) > 0)
            errors.add("Amount exceeds maximum limit of " + MAX_AMOUNT);

        if (!SUPPORTED_CURRENCIES.contains(request.currency()))
            errors.add("Unsupported currency: " + request.currency());

        if (request.amount().scale() > 4)
            errors.add("Amount cannot have more than 4 decimal places");

        if (!errors.isEmpty())
            throw new PaymentValidationException(errors);
    }
}
```

**Business Rules:**
- `fromAccountId != toAccountId`
- `amount > 0` and `amount <= 10,000,000`
- Currency in supported set
- `X-Idempotency-Key` header required (UUID format)
- JWT must contain valid `sub` (userId)
- Account must exist and be `ACTIVE` (verified via inter-service call or cache)
- KYC status must be `VERIFIED`
- Velocity check: max 10 payments per account per minute (Redis counter)

---

## 2. Ledger Service

**Port:** 8082 | **Database:** ledger_db | **Module:** `ledger-service`

---

### 2.1 REST APIs

| Method | Endpoint | Description | Auth |
|---|---|---|---|
| GET | `/api/v1/accounts` | List all accounts | JWT |
| POST | `/api/v1/accounts` | Create account | JWT (ADMIN) |
| GET | `/api/v1/accounts/{accountId}` | Get account details | JWT |
| GET | `/api/v1/accounts/{accountId}/balance` | Get current balance | JWT |
| GET | `/api/v1/accounts/{accountId}/entries?from=&to=&page=&size=` | Get journal entries | JWT |
| GET | `/api/v1/accounts/{accountId}/entries/{entryId}` | Get single entry | JWT |
| POST | `/api/v1/ledger/verify` | Verify ledger balance (admin) | JWT (ADMIN) |
| GET | `/actuator/prometheus` | Metrics | Internal |

**Response — GET /api/v1/accounts/{id}/balance:**
```json
{
  "accountId":   "acc-uuid",
  "accountName": "Alice Wallet",
  "accountType": "WALLET",
  "currency":    "INR",
  "balance":     "4000.00",
  "holdAmount":  "0.00",
  "availableBalance": "4000.00",
  "lastEntryAt": "2026-06-03T10:00:00Z"
}
```

---

### 2.2 Database Tables

```sql
-- ledger_db

CREATE TYPE account_type AS ENUM ('WALLET', 'SUSPENSE', 'FEE', 'NOSTRO', 'LIABILITY');
CREATE TYPE entry_type   AS ENUM ('DEBIT', 'CREDIT');

CREATE TABLE accounts (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_name VARCHAR(255) NOT NULL,
    account_type account_type NOT NULL,
    currency     CHAR(3) NOT NULL,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE journal_entries (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id UUID NOT NULL,          -- logical grouping (payment ID)
    account_id     UUID NOT NULL REFERENCES accounts(id),
    entry_type     entry_type NOT NULL,
    amount         NUMERIC(19, 4) NOT NULL CHECK (amount > 0),
    currency       CHAR(3) NOT NULL,
    description    TEXT,
    reference_id   UUID,                   -- paymentId / reversalId
    reference_type VARCHAR(50),            -- 'PAYMENT' / 'REVERSAL' / 'FEE'
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    -- NO updated_at — this table is APPEND-ONLY
);

CREATE INDEX idx_entries_account    ON journal_entries(account_id, created_at DESC);
CREATE INDEX idx_entries_txn        ON journal_entries(transaction_id);
CREATE INDEX idx_entries_reference  ON journal_entries(reference_id);

-- APPEND-ONLY enforcement
CREATE OR REPLACE FUNCTION prevent_journal_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'journal_entries is append-only. No UPDATE or DELETE permitted.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER no_modify_journal
BEFORE UPDATE OR DELETE ON journal_entries
FOR EACH ROW EXECUTE FUNCTION prevent_journal_modification();

-- Materialized balance (maintained by trigger)
CREATE TABLE account_balances (
    account_id      UUID PRIMARY KEY REFERENCES accounts(id),
    balance         NUMERIC(19, 4) NOT NULL DEFAULT 0,
    hold_amount     NUMERIC(19, 4) NOT NULL DEFAULT 0,
    last_entry_at   TIMESTAMPTZ,
    version         BIGINT NOT NULL DEFAULT 0
);

CREATE OR REPLACE FUNCTION update_account_balance()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.entry_type = 'CREDIT' THEN
        UPDATE account_balances
        SET balance = balance + NEW.amount,
            last_entry_at = NEW.created_at,
            version = version + 1
        WHERE account_id = NEW.account_id;
    ELSE
        UPDATE account_balances
        SET balance = balance - NEW.amount,
            last_entry_at = NEW.created_at,
            version = version + 1
        WHERE account_id = NEW.account_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_balance
AFTER INSERT ON journal_entries
FOR EACH ROW EXECUTE FUNCTION update_account_balance();
```

---

### 2.3 Entities

```java
// Account.java
@Entity
@Table(name = "accounts")
public class Account {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "account_name", nullable = false)
    private String accountName;

    @Enumerated(EnumType.STRING)
    @Column(name = "account_type", nullable = false)
    @Type(PostgreSQLEnumType.class)
    private AccountType accountType;

    @Column(nullable = false, length = 3)
    private String currency;

    private boolean isActive = true;

    @CreatedDate
    private Instant createdAt;
}

// JournalEntry.java
@Entity
@Table(name = "journal_entries")
@Immutable                                        // Hibernate: no UPDATE generated
public class JournalEntry {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "account_id", nullable = false)
    private Account account;

    @Enumerated(EnumType.STRING)
    @Column(name = "entry_type", nullable = false)
    private EntryType entryType;                  // DEBIT | CREDIT

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    private String description;

    @Column(name = "reference_id")
    private UUID referenceId;

    @Column(name = "reference_type")
    private String referenceType;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();
}

// AccountBalance.java
@Entity
@Table(name = "account_balances")
public class AccountBalance {
    @Id
    private UUID accountId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal balance = BigDecimal.ZERO;

    @Column(name = "hold_amount", precision = 19, scale = 4)
    private BigDecimal holdAmount = BigDecimal.ZERO;

    @Column(name = "last_entry_at")
    private Instant lastEntryAt;

    @Version
    private Long version;
}
```

---

### 2.4 DTOs

```java
// JournalEntryCommand.java (internal — created from Kafka event)
public record JournalEntryCommand(
    UUID transactionId,
    UUID accountId,
    EntryType entryType,
    BigDecimal amount,
    String currency,
    String description,
    UUID referenceId,
    String referenceType
) {}

// DoubleEntryTransaction.java (groups two+ entries for one logical payment)
public record DoubleEntryTransaction(
    UUID transactionId,
    List<JournalEntryCommand> entries   // must sum to zero
) {}

// BalanceResponse.java
public record BalanceResponse(
    UUID accountId,
    String accountName,
    String accountType,
    String currency,
    BigDecimal balance,
    BigDecimal holdAmount,
    BigDecimal availableBalance,
    Instant lastEntryAt
) {}

// JournalEntryResponse.java
public record JournalEntryResponse(
    UUID entryId,
    UUID transactionId,
    UUID accountId,
    String entryType,
    BigDecimal amount,
    String currency,
    String description,
    UUID referenceId,
    String referenceType,
    Instant createdAt
) {}
```

---

### 2.5 Repository Layer

```java
// JournalEntryRepository.java
@Repository
public interface JournalEntryRepository extends JpaRepository<JournalEntry, UUID> {

    Page<JournalEntry> findByAccount_IdOrderByCreatedAtDesc(UUID accountId, Pageable pageable);

    Page<JournalEntry> findByAccount_IdAndCreatedAtBetween(
        UUID accountId, Instant from, Instant to, Pageable pageable);

    List<JournalEntry> findByTransactionId(UUID transactionId);

    // Verify invariant: sum of all entries must be zero
    @Query("SELECT COALESCE(SUM(CASE WHEN e.entryType = 'CREDIT' THEN e.amount ELSE -e.amount END), 0) " +
           "FROM JournalEntry e WHERE e.transactionId = :txnId")
    BigDecimal sumByTransactionId(@Param("txnId") UUID txnId);
}

// AccountBalanceRepository.java
@Repository
public interface AccountBalanceRepository extends JpaRepository<AccountBalance, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT b FROM AccountBalance b WHERE b.accountId = :accountId")
    Optional<AccountBalance> findByAccountIdForUpdate(@Param("accountId") UUID accountId);
}
```

---

### 2.6 Service Layer

```java
// LedgerService.java (interface)
public interface LedgerService {
    void postDoubleEntry(DoubleEntryTransaction transaction);
    BalanceResponse getBalance(UUID accountId);
    Page<JournalEntryResponse> getEntries(UUID accountId, Instant from, Instant to, Pageable pageable);
    void verifyLedgerIntegrity(UUID transactionId);
}

// LedgerServiceImpl.java
@Service
@Slf4j
@Transactional
public class LedgerServiceImpl implements LedgerService {

    private final JournalEntryRepository journalRepo;
    private final AccountBalanceRepository balanceRepo;
    private final MeterRegistry meterRegistry;

    @Override
    public void postDoubleEntry(DoubleEntryTransaction txn) {
        // Invariant: entries must net to zero
        BigDecimal net = txn.entries().stream()
            .map(e -> e.entryType() == EntryType.CREDIT ? e.amount() : e.amount().negate())
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        if (net.compareTo(BigDecimal.ZERO) != 0) {
            meterRegistry.counter("ledger.balance_mismatch.total").increment();
            throw new LedgerImbalanceException(txn.transactionId(), net);
        }

        txn.entries().forEach(cmd -> {
            JournalEntry entry = toEntity(cmd, txn.transactionId());
            journalRepo.save(entry);
            // Balance updated by DB trigger — no application-level update needed
        });

        meterRegistry.counter("ledger.entries_written.total",
            "count", String.valueOf(txn.entries().size())).increment();

        log.info("Double-entry posted transactionId={} entries={}", 
            txn.transactionId(), txn.entries().size());
    }

    @Override
    @Transactional(readOnly = true)
    public BalanceResponse getBalance(UUID accountId) {
        AccountBalance bal = balanceRepo.findById(accountId)
            .orElseThrow(() -> new AccountNotFoundException(accountId));
        Account acc = accountRepo.findById(accountId)
            .orElseThrow(() -> new AccountNotFoundException(accountId));

        return new BalanceResponse(
            accountId, acc.getAccountName(), acc.getAccountType().name(),
            acc.getCurrency(), bal.getBalance(), bal.getHoldAmount(),
            bal.getBalance().subtract(bal.getHoldAmount()), bal.getLastEntryAt()
        );
    }
}
```

---

### 2.7 Kafka Components (Consumer)

```java
// PaymentEventConsumer.java
@Component
@Slf4j
public class PaymentEventConsumer {

    private final LedgerService ledgerService;
    private final LedgerEntryMapper mapper;

    @KafkaListener(
        topics = "payment.events",
        groupId = "ledger-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload PaymentEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("Received paymentEvent eventType={} paymentId={} partition={} offset={}",
            event.eventType(), event.paymentId(), partition, offset);

        try {
            if ("PAYMENT_AUTHORIZED".equals(event.eventType())) {
                DoubleEntryTransaction txn = mapper.toDoubleEntry(event);
                ledgerService.postDoubleEntry(txn);
            }
            ack.acknowledge();
        } catch (LedgerImbalanceException e) {
            // Never ack — route to DLQ
            log.error("CRITICAL: Ledger imbalance paymentId={}", event.paymentId(), e);
            throw e;
        } catch (Exception e) {
            log.error("Ledger consumer error paymentId={}", event.paymentId(), e);
            throw e;  // triggers retry → DLQ after 3 retries
        }
    }
}

// KafkaConsumerConfig.java — Ledger Service
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, PaymentEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka.bootstrap-servers}");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ledger-service-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // manual ack
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        return new DefaultKafkaConsumerFactory<>(props,
            new StringDeserializer(), new JsonDeserializer<>(PaymentEvent.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentEvent>
    kafkaListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, PaymentEvent>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(3);                   // 3 threads = 3 partitions
        factory.setCommonErrorHandler(defaultErrorHandler());
        return factory;
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        return new DefaultErrorHandler(
            new DeadLetterPublishingRecoverer(kafkaTemplate(),
                (r, e) -> new TopicPartition("payment.dlq", 0)),
            new FixedBackOff(1000L, 3)               // 3 retries, 1s apart
        );
    }
}
```

---

## 3. Notification Service

**Port:** 8083 | **Database:** notifications_db | **Module:** `notification-service`

---

### 3.1 REST APIs

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/notifications/{userId}` | Get notifications for user |
| PUT | `/api/v1/notifications/{notifId}/read` | Mark notification as read |
| GET | `/api/v1/notifications/templates` | List templates (ADMIN) |
| POST | `/api/v1/notifications/templates` | Create template (ADMIN) |

---

### 3.2 Database Tables

```sql
-- notifications_db

CREATE TYPE channel_type     AS ENUM ('EMAIL', 'SMS', 'PUSH', 'WEBSOCKET');
CREATE TYPE delivery_status  AS ENUM ('PENDING', 'SENT', 'FAILED', 'READ');

CREATE TABLE notification_templates (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type   VARCHAR(80) NOT NULL UNIQUE,    -- 'PAYMENT_SETTLED'
    channel      channel_type NOT NULL,
    subject      TEXT,
    body_template TEXT NOT NULL,                 -- Thymeleaf template
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE delivery_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL,
    event_type      VARCHAR(80) NOT NULL,
    reference_id    UUID,                        -- paymentId
    channel         channel_type NOT NULL,
    recipient       VARCHAR(255) NOT NULL,        -- email / phone
    subject         TEXT,
    body            TEXT NOT NULL,
    status          delivery_status NOT NULL DEFAULT 'PENDING',
    failure_reason  TEXT,
    attempts        INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at         TIMESTAMPTZ,
    read_at         TIMESTAMPTZ
);
CREATE INDEX idx_delivery_user ON delivery_log(user_id, created_at DESC);
```

---

### 3.3 Entities & DTOs

```java
// NotificationTemplate.java
@Entity @Table(name = "notification_templates")
public class NotificationTemplate {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    private String eventType;
    @Enumerated(EnumType.STRING)
    private ChannelType channel;
    private String subject;
    @Column(name = "body_template")
    private String bodyTemplate;
    private boolean isActive;
    private Instant createdAt;
}

// DeliveryLog.java
@Entity @Table(name = "delivery_log")
public class DeliveryLog {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    private UUID userId;
    private String eventType;
    private UUID referenceId;
    @Enumerated(EnumType.STRING)
    private ChannelType channel;
    private String recipient;
    private String subject;
    @Column(columnDefinition = "TEXT")
    private String body;
    @Enumerated(EnumType.STRING)
    private DeliveryStatus status;
    private String failureReason;
    private int attempts;
    private Instant createdAt;
    private Instant sentAt;
    private Instant readAt;
}

// NotificationRequest.java (internal)
public record NotificationRequest(
    UUID userId,
    String eventType,
    UUID referenceId,
    Map<String, Object> templateVars
) {}
```

---

### 3.4 Service & Kafka Consumer

```java
// NotificationConsumer.java
@Component
@Slf4j
public class NotificationConsumer {

    private final NotificationService notificationService;

    @KafkaListener(topics = {"payment.events", "ledger.entries"},
                   groupId = "notification-service-group")
    public void consume(@Payload PaymentEvent event, Acknowledgment ack) {
        try {
            notificationService.sendNotification(new NotificationRequest(
                event.fromAccountId(),   // resolved to userId upstream
                event.eventType(),
                event.paymentId(),
                Map.of("amount", event.amount(), "currency", event.currency())
            ));
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Notification failed eventType={}", event.eventType(), e);
            throw e;
        }
    }
}

// NotificationServiceImpl.java
@Service
@Transactional
public class NotificationServiceImpl implements NotificationService {

    private final NotificationTemplateRepository templateRepo;
    private final DeliveryLogRepository deliveryLogRepo;
    private final TemplateEngine templateEngine;    // Thymeleaf
    private final EmailSender emailSender;

    @Override
    public void sendNotification(NotificationRequest request) {
        NotificationTemplate template = templateRepo
            .findByEventTypeAndIsActiveTrue(request.eventType())
            .orElseThrow(() -> new TemplateNotFoundException(request.eventType()));

        String body = renderTemplate(template.getBodyTemplate(), request.templateVars());

        DeliveryLog log = new DeliveryLog();
        log.setUserId(request.userId());
        log.setEventType(request.eventType());
        log.setReferenceId(request.referenceId());
        log.setBody(body);
        log.setStatus(DeliveryStatus.PENDING);

        try {
            emailSender.send(resolveEmail(request.userId()), template.getSubject(), body);
            log.setStatus(DeliveryStatus.SENT);
            log.setSentAt(Instant.now());
        } catch (Exception e) {
            log.setStatus(DeliveryStatus.FAILED);
            log.setFailureReason(e.getMessage());
        } finally {
            log.setAttempts(log.getAttempts() + 1);
            deliveryLogRepo.save(log);
        }
    }
}
```

---

## 4. Reconciliation Service

**Port:** 8084 | **Database:** reconciliation_db | **Module:** `reconciliation-service`

---

### 4.1 REST APIs

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/recon/run` | Trigger manual recon run | JWT (ADMIN) |
| GET | `/api/v1/recon/runs` | List all recon runs | JWT (ADMIN) |
| GET | `/api/v1/recon/runs/{runId}` | Get run details | JWT (ADMIN) |
| GET | `/api/v1/recon/mismatches?runId=&status=` | Get mismatches | JWT (ADMIN) |
| PUT | `/api/v1/recon/mismatches/{id}/resolve` | Mark mismatch resolved | JWT (ADMIN) |
| POST | `/api/v1/recon/bank-statements` | Upload bank statement | JWT (ADMIN) |

---

### 4.2 Database Tables

```sql
-- reconciliation_db

CREATE TYPE recon_status    AS ENUM ('RUNNING', 'COMPLETED', 'FAILED');
CREATE TYPE mismatch_type   AS ENUM ('MISSING_IN_BANK', 'MISSING_IN_LEDGER',
                                     'AMOUNT_MISMATCH', 'DUPLICATE');
CREATE TYPE mismatch_status AS ENUM ('OPEN', 'RESOLVED', 'IGNORED');

CREATE TABLE recon_runs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_date        DATE NOT NULL,
    status          recon_status NOT NULL DEFAULT 'RUNNING',
    total_ledger    INT NOT NULL DEFAULT 0,
    total_bank      INT NOT NULL DEFAULT 0,
    matched_count   INT NOT NULL DEFAULT 0,
    mismatch_count  INT NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    error_message   TEXT
);

CREATE TABLE bank_statements (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recon_run_id    UUID REFERENCES recon_runs(id),
    transaction_ref VARCHAR(255) NOT NULL UNIQUE,
    amount          NUMERIC(19, 4) NOT NULL,
    currency        CHAR(3) NOT NULL,
    transaction_date DATE NOT NULL,
    description     TEXT,
    raw_row         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE recon_mismatches (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recon_run_id    UUID NOT NULL REFERENCES recon_runs(id),
    mismatch_type   mismatch_type NOT NULL,
    ledger_entry_id UUID,
    bank_statement_id UUID REFERENCES bank_statements(id),
    ledger_amount   NUMERIC(19, 4),
    bank_amount     NUMERIC(19, 4),
    currency        CHAR(3),
    description     TEXT,
    status          mismatch_status NOT NULL DEFAULT 'OPEN',
    resolved_by     VARCHAR(255),
    resolved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_mismatch_run  ON recon_mismatches(recon_run_id);
CREATE INDEX idx_mismatch_open ON recon_mismatches(status) WHERE status = 'OPEN';
```

---

### 4.3 Service Layer

```java
// ReconciliationServiceImpl.java
@Service
@Slf4j
public class ReconciliationServiceImpl implements ReconciliationService {

    private final ReconRunRepository reconRunRepo;
    private final BankStatementRepository bankStmtRepo;
    private final ReconMismatchRepository mismatchRepo;
    private final LedgerServiceClient ledgerClient;     // Feign
    private final MeterRegistry meterRegistry;

    // Scheduled: daily at 02:00 AM
    @Scheduled(cron = "0 0 2 * * *")
    public void runDailyReconciliation() {
        runReconciliation(LocalDate.now().minusDays(1));
    }

    @Override
    @Transactional
    public ReconRunResponse runReconciliation(LocalDate date) {
        ReconRun run = new ReconRun();
        run.setRunDate(date);
        run.setStatus(ReconStatus.RUNNING);
        reconRunRepo.save(run);

        try {
            // 1. Fetch ledger entries for date
            List<JournalEntryResponse> ledgerEntries = ledgerClient.getEntriesByDate(date);

            // 2. Fetch bank statement lines for date
            List<BankStatement> bankLines = bankStmtRepo.findByTransactionDate(date);

            // 3. Match
            ReconciliationResult result = match(ledgerEntries, bankLines);

            // 4. Persist mismatches
            result.mismatches().forEach(m -> {
                ReconMismatch mismatch = toEntity(m, run.getId());
                mismatchRepo.save(mismatch);
            });

            // 5. Update run
            run.setStatus(ReconStatus.COMPLETED);
            run.setTotalLedger(ledgerEntries.size());
            run.setTotalBank(bankLines.size());
            run.setMatchedCount(result.matched());
            run.setMismatchCount(result.mismatches().size());
            run.setCompletedAt(Instant.now());

            meterRegistry.gauge("recon.mismatch.count", result.mismatches().size());
            log.info("Recon completed date={} matched={} mismatches={}",
                date, result.matched(), result.mismatches().size());

        } catch (Exception e) {
            run.setStatus(ReconStatus.FAILED);
            run.setErrorMessage(e.getMessage());
            log.error("Recon failed date={}", date, e);
        }

        return toResponse(reconRunRepo.save(run));
    }

    private ReconciliationResult match(List<JournalEntryResponse> ledger,
                                        List<BankStatement> bank) {
        Map<String, JournalEntryResponse> ledgerMap = ledger.stream()
            .collect(Collectors.toMap(e -> e.referenceId().toString(), e -> e));
        Map<String, BankStatement> bankMap = bank.stream()
            .collect(Collectors.toMap(BankStatement::getTransactionRef, b -> b));

        List<MismatchRecord> mismatches = new ArrayList<>();
        int matched = 0;

        // Missing in bank
        for (var entry : ledgerMap.entrySet()) {
            BankStatement bankLine = bankMap.get(entry.getKey());
            if (bankLine == null) {
                mismatches.add(new MismatchRecord(MismatchType.MISSING_IN_BANK,
                    entry.getValue().entryId(), null,
                    entry.getValue().amount(), null));
            } else if (entry.getValue().amount().compareTo(bankLine.getAmount()) != 0) {
                mismatches.add(new MismatchRecord(MismatchType.AMOUNT_MISMATCH,
                    entry.getValue().entryId(), bankLine.getId(),
                    entry.getValue().amount(), bankLine.getAmount()));
            } else {
                matched++;
            }
        }

        // Missing in ledger
        bankMap.keySet().stream()
            .filter(ref -> !ledgerMap.containsKey(ref))
            .forEach(ref -> {
                BankStatement b = bankMap.get(ref);
                mismatches.add(new MismatchRecord(MismatchType.MISSING_IN_LEDGER,
                    null, b.getId(), null, b.getAmount()));
            });

        return new ReconciliationResult(matched, mismatches);
    }
}
```

---

### 4.4 Kafka Consumer

```java
// ReconJobConsumer.java
@Component
public class ReconJobConsumer {

    private final ReconciliationService reconService;

    @KafkaListener(topics = "recon.jobs", groupId = "recon-service-group")
    public void consume(@Payload ReconJobEvent event, Acknowledgment ack) {
        reconService.runReconciliation(event.date());
        ack.acknowledge();
    }
}
```

---

## 5. Shared Components

### 5.1 Project Structure

```
finledgerx/
├── payment-service/
│   └── src/main/java/com/finledgerx/payment/
│       ├── controller/   PaymentController.java
│       ├── service/      PaymentService.java, PaymentServiceImpl.java
│       ├── repository/   PaymentRepository.java, OutboxEventRepository.java
│       ├── entity/       Payment.java, OutboxEvent.java
│       ├── dto/          PaymentRequest.java, PaymentResponse.java, PaymentEvent.java
│       ├── kafka/        PaymentEventProducer.java, OutboxPoller.java
│       ├── validator/    PaymentValidator.java
│       └── exception/    PaymentExceptions.java
├── ledger-service/
├── notification-service/
├── reconciliation-service/
└── shared-lib/            (common DTOs, exceptions, utils — internal Maven module)
```

---

### 5.2 Common Exception Hierarchy

```java
// Base
public abstract class FinLedgerXException extends RuntimeException {
    private final String errorCode;
    private final HttpStatus httpStatus;
    // ...
}

// Per-domain
public class PaymentNotFoundException      extends FinLedgerXException { /* 404 */ }
public class PaymentValidationException    extends FinLedgerXException { /* 400 */ }
public class InvalidStateTransitionException extends FinLedgerXException { /* 409 */ }
public class IdempotencyConflictException  extends FinLedgerXException { /* 409 */ }
public class InsufficientFundsException    extends FinLedgerXException { /* 422 */ }
public class AccountNotFoundException      extends FinLedgerXException { /* 404 */ }
public class LedgerImbalanceException      extends FinLedgerXException { /* 500 */ }
public class TemplateNotFoundException     extends FinLedgerXException { /* 404 */ }
public class ReconciliationException       extends FinLedgerXException { /* 500 */ }
```

### 5.3 Idempotency Service

```java
// IdempotencyService.java
@Service
public class IdempotencyService {

    private final RedisTemplate<String, IdempotencyRecord> redisTemplate;
    private static final Duration TTL = Duration.ofHours(24);

    public <T> T executeIdempotent(String key, Supplier<T> supplier) {
        String redisKey = "idempotency:" + key;
        IdempotencyRecord cached = redisTemplate.opsForValue().get(redisKey);

        if (cached != null) {
            log.info("Idempotency hit key={}", key);
            return (T) cached.getResponseBody();
        }

        T result = supplier.get();
        redisTemplate.opsForValue().set(redisKey,
            new IdempotencyRecord(result, 200), TTL);
        return result;
    }
}
```

---

## 6. Global Error Handling

```java
// GlobalExceptionHandler.java
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(PaymentValidationException.class)
    public ResponseEntity<ErrorResponse> handleValidation(PaymentValidationException ex) {
        return ResponseEntity.badRequest().body(ErrorResponse.of(
            "VALIDATION_ERROR", ex.getMessage(), ex.getErrors()
        ));
    }

    @ExceptionHandler(PaymentNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(PaymentNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ErrorResponse.of(
            "NOT_FOUND", ex.getMessage()
        ));
    }

    @ExceptionHandler(InvalidStateTransitionException.class)
    public ResponseEntity<ErrorResponse> handleStateTransition(InvalidStateTransitionException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ErrorResponse.of(
            "INVALID_STATE_TRANSITION", ex.getMessage()
        ));
    }

    @ExceptionHandler(LedgerImbalanceException.class)
    public ResponseEntity<ErrorResponse> handleLedgerImbalance(LedgerImbalanceException ex) {
        log.error("CRITICAL LEDGER IMBALANCE: {}", ex.getMessage());
        // This fires a Micrometer counter which triggers a Grafana P0 alert
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.of(
            "LEDGER_IMBALANCE", "A critical ledger error has occurred. Ops team notified."
        ));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleBeanValidation(MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
            .map(e -> e.getField() + ": " + e.getDefaultMessage())
            .toList();
        return ResponseEntity.badRequest().body(ErrorResponse.of("VALIDATION_ERROR", errors));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception ex) {
        log.error("Unhandled exception", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.of(
            "INTERNAL_ERROR", "An unexpected error occurred"
        ));
    }
}

// ErrorResponse.java
public record ErrorResponse(
    String errorCode,
    String message,
    List<String> details,
    Instant timestamp,
    String traceId
) {
    public static ErrorResponse of(String code, String message) {
        return new ErrorResponse(code, message, List.of(),
            Instant.now(), MDC.get("traceId"));
    }
    public static ErrorResponse of(String code, String message, List<String> details) {
        return new ErrorResponse(code, message, details,
            Instant.now(), MDC.get("traceId"));
    }
}
```

---

## Validation Rules Summary

| Service | Rule | Error |
|---|---|---|
| Payment | `fromAccountId != toAccountId` | 400 |
| Payment | `amount > 0 AND amount <= 10,000,000` | 400 |
| Payment | `currency IN (INR,USD,EUR,GBP)` | 400 |
| Payment | `X-Idempotency-Key` must be UUID | 400 |
| Payment | Account must be ACTIVE | 422 |
| Payment | KYC must be VERIFIED | 422 |
| Payment | Velocity: max 10/account/minute | 429 |
| Payment | `amount` max 4 decimal places | 400 |
| Ledger | Double-entry must sum to zero | 500 (P0) |
| Ledger | `amount > 0` per entry | 400 |
| Ledger | Currency consistency across entries | 400 |
| Ledger | Account must be ACTIVE | 422 |
| Recon | Bank statement CSV headers valid | 400 |
| Recon | `transaction_ref` must be unique | 409 |

---

*Last updated: 2026-06-03 | Companion to HLD.md | FinLedgerX Architecture Team*
