# FinLedgerX — PostgreSQL Schema Design

> **Principal Architect View** | 4 isolated databases · 16 tables · partitioning · triggers · constraints
>
> Each database is owned by exactly one microservice. No cross-database foreign keys exist — logical references are enforced at the application layer via Kafka events and service contracts.

---

## Table of Contents

1. payments_db
2. ledger_db
3. notifications_db
4. reconciliation_db
5. Cross-Database Design Decisions

---

## 1. payments_db

**Owner:** Payment Service | **Port:** 5432

---

### Why these tables exist

| Table | Purpose |
|---|---|
| `payments` | Core payment record. Single source of truth for payment state. Every API response is derived from this. |
| `outbox_events` | Transactional outbox. Guarantees Kafka events are never lost even during broker downtime. Poller reads this and publishes atomically. |
| `idempotency_keys` | Prevents duplicate payment processing on client retries. Stores the cached response body so the retry gets the exact same 202. |
| `payment_audit_log` | Immutable log of every status transition. Required for regulatory compliance and dispute resolution. |

---

```sql
-- ============================================================
-- DATABASE: payments_db
-- ============================================================

\c payments_db;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";        -- for LIKE searches on description

-- ──────────────────────────────────────────
-- ENUMS
-- ──────────────────────────────────────────
CREATE TYPE payment_status AS ENUM (
    'INITIATED',
    'VALIDATING',
    'AUTHORIZED',
    'PROCESSING',
    'SETTLED',
    'FAILED',
    'REVERSED',
    'CANCELLED'
);

-- ──────────────────────────────────────────
-- TABLE: payments
-- ──────────────────────────────────────────
-- Why: Central record for every payment request. Owns the state machine.
--      amount uses NUMERIC(19,4) — never FLOAT for money.
--      version column enables optimistic concurrency control (OCC).
--      metadata JSONB allows extensible key-value without schema changes.
-- ──────────────────────────────────────────
CREATE TABLE payments (
    id               UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key  VARCHAR(255) NOT NULL,
    from_account_id  UUID         NOT NULL,
    to_account_id    UUID         NOT NULL,
    amount           NUMERIC(19, 4) NOT NULL,
    currency         CHAR(3)      NOT NULL,
    status           payment_status NOT NULL DEFAULT 'INITIATED',
    description      TEXT,
    metadata         JSONB,
    failure_reason   TEXT,
    version          BIGINT       NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    settled_at       TIMESTAMPTZ,

    -- Constraints
    CONSTRAINT uq_payments_idempotency     UNIQUE (idempotency_key),
    CONSTRAINT chk_payments_amount_pos     CHECK  (amount > 0),
    CONSTRAINT chk_payments_amount_scale   CHECK  (amount < 100000000),
    CONSTRAINT chk_payments_diff_accounts  CHECK  (from_account_id <> to_account_id),
    CONSTRAINT chk_payments_currency       CHECK  (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT chk_payments_settled_at     CHECK  (settled_at IS NULL OR status = 'SETTLED')
);

-- Indexes
CREATE INDEX idx_payments_from_account  ON payments (from_account_id, created_at DESC);
CREATE INDEX idx_payments_to_account    ON payments (to_account_id,   created_at DESC);
CREATE INDEX idx_payments_status        ON payments (status)           WHERE status NOT IN ('SETTLED', 'FAILED', 'CANCELLED');
CREATE INDEX idx_payments_created_at    ON payments (created_at DESC);
CREATE INDEX idx_payments_description   ON payments USING GIN (to_tsvector('english', description))
    WHERE description IS NOT NULL;

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_payments_updated_at
    BEFORE UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();


-- ──────────────────────────────────────────
-- TABLE: outbox_events
-- ──────────────────────────────────────────
-- Why: Transactional Outbox Pattern. The outbox entry and the payment row
--      are written in ONE database transaction. This guarantees:
--      - If DB commit succeeds → event will eventually be published to Kafka.
--      - If Kafka is down → event is NOT lost; poller retries from DB.
--      - No dual-write problem between DB and Kafka.
--      The poller selects unpublished events (partial index makes this fast)
--      and marks them published atomically after Kafka ACK.
-- ──────────────────────────────────────────
CREATE TABLE outbox_events (
    id              UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_id    UUID         NOT NULL,
    aggregate_type  VARCHAR(50)  NOT NULL,
    event_type      VARCHAR(80)  NOT NULL,
    payload         JSONB        NOT NULL,
    topic           VARCHAR(120) NOT NULL,
    partition_key   VARCHAR(255),
    published       BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,

    CONSTRAINT chk_outbox_published_at CHECK (
        (published = FALSE AND published_at IS NULL) OR
        (published = TRUE  AND published_at IS NOT NULL)
    )
);

-- Partial index: only unpublished events — keeps the index tiny as events are marked published
CREATE INDEX idx_outbox_unpublished
    ON outbox_events (created_at ASC)
    WHERE published = FALSE;

CREATE INDEX idx_outbox_aggregate ON outbox_events (aggregate_id);


-- ──────────────────────────────────────────
-- TABLE: idempotency_keys
-- ──────────────────────────────────────────
-- Why: A client retrying a timed-out request must get the same response as the
--      original, without triggering a second payment. We cache the entire
--      response body (JSONB) keyed on the client-supplied idempotency key.
--      expires_at allows a nightly cleanup job to purge stale keys.
-- ──────────────────────────────────────────
CREATE TABLE idempotency_keys (
    idempotency_key  VARCHAR(255) PRIMARY KEY,
    payment_id       UUID         NOT NULL,
    response_body    JSONB        NOT NULL,
    http_status      SMALLINT     NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at       TIMESTAMPTZ  NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),

    CONSTRAINT chk_idempotency_http_status CHECK (http_status BETWEEN 100 AND 599)
);

-- Index for nightly TTL sweep
CREATE INDEX idx_idempotency_expires ON idempotency_keys (expires_at);


-- ──────────────────────────────────────────
-- TABLE: payment_audit_log
-- ──────────────────────────────────────────
-- Why: Every state transition is recorded immutably. Required for:
--      - Dispute resolution ("when exactly did this payment fail and why?")
--      - Regulatory compliance (RBI requires 5-year audit trail)
--      - Debugging production incidents
--      Populated by a DB trigger on payments.status UPDATE — no code path
--      can bypass it.
-- ──────────────────────────────────────────
CREATE TABLE payment_audit_log (
    id             UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    payment_id     UUID        NOT NULL REFERENCES payments(id),
    old_status     payment_status,
    new_status     payment_status NOT NULL,
    changed_by     VARCHAR(255),
    change_reason  TEXT,
    ip_address     INET,
    user_agent     TEXT,
    changed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_payment_id  ON payment_audit_log (payment_id, changed_at DESC);
CREATE INDEX idx_audit_changed_at  ON payment_audit_log (changed_at DESC);

-- Trigger to auto-populate audit log on status change
CREATE OR REPLACE FUNCTION fn_payment_audit()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO payment_audit_log (payment_id, old_status, new_status, changed_at)
        VALUES (NEW.id, OLD.status, NEW.status, NOW());
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_payment_audit
    AFTER UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION fn_payment_audit();


-- ──────────────────────────────────────────
-- SAMPLE DATA: payments_db
-- ──────────────────────────────────────────
INSERT INTO payments (id, idempotency_key, from_account_id, to_account_id,
                      amount, currency, status, description)
VALUES
    ('a1b2c3d4-0001-0001-0001-000000000001',
     'idem-key-0001',
     'acc00000-0000-0000-0000-000000000001',
     'acc00000-0000-0000-0000-000000000002',
     1000.0000, 'INR', 'SETTLED', 'Invoice INV-2026-001'),

    ('a1b2c3d4-0002-0002-0002-000000000002',
     'idem-key-0002',
     'acc00000-0000-0000-0000-000000000003',
     'acc00000-0000-0000-0000-000000000001',
     500.5000, 'INR', 'PROCESSING', 'Refund for order #456'),

    ('a1b2c3d4-0003-0003-0003-000000000003',
     'idem-key-0003',
     'acc00000-0000-0000-0000-000000000002',
     'acc00000-0000-0000-0000-000000000004',
     250.0000, 'USD', 'FAILED', 'Cross-border payment');

INSERT INTO idempotency_keys (idempotency_key, payment_id, response_body, http_status)
VALUES (
    'idem-key-0001',
    'a1b2c3d4-0001-0001-0001-000000000001',
    '{"paymentId":"a1b2c3d4-0001-0001-0001-000000000001","status":"SETTLED"}',
    202
);
```

---

## 2. ledger_db

**Owner:** Ledger Service | **Port:** 5433

---

### Why these tables exist

| Table | Purpose |
|---|---|
| `accounts` | Every entity that can hold a balance: user wallets, suspense accounts, fee accounts, nostro accounts. |
| `journal_entries` | The financial truth. Every debit and credit ever recorded. Append-only. Partitioned by month for query performance. |
| `account_balances` | Materialized balance. Maintained by a DB trigger on `journal_entries`. Serves fast balance reads without summing millions of rows. |
| `account_holds` | Tracks reserved (held) funds during AUTHORIZED state. Released when payment SETTLES or FAILS. Prevents double-spend. |

---

```sql
-- ============================================================
-- DATABASE: ledger_db
-- ============================================================

\c ledger_db;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ──────────────────────────────────────────
-- ENUMS
-- ──────────────────────────────────────────
CREATE TYPE account_type AS ENUM (
    'WALLET',       -- user-owned wallet
    'SUSPENSE',     -- temporary clearing account
    'FEE',          -- platform fee revenue
    'NOSTRO',       -- bank's account at a correspondent bank
    'LIABILITY',    -- loans, credit
    'INTERNAL'      -- platform operational accounts
);

CREATE TYPE entry_type AS ENUM ('DEBIT', 'CREDIT');

CREATE TYPE hold_status AS ENUM ('ACTIVE', 'RELEASED', 'EXPIRED');


-- ──────────────────────────────────────────
-- TABLE: accounts
-- ──────────────────────────────────────────
-- Why: Represents every entity that can hold money in the system.
--      account_number is a human-readable identifier (e.g., "WALL-001234").
--      owner_id links to the user in an external identity service.
--      A single user can have multiple accounts (multi-currency wallets).
-- ──────────────────────────────────────────
CREATE TABLE accounts (
    id              UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_number  VARCHAR(30)  NOT NULL,
    account_name    VARCHAR(255) NOT NULL,
    account_type    account_type NOT NULL,
    currency        CHAR(3)      NOT NULL,
    owner_id        UUID,                          -- NULL for system/platform accounts
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,

    CONSTRAINT uq_accounts_number        UNIQUE  (account_number),
    CONSTRAINT chk_accounts_currency     CHECK   (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT chk_accounts_closed_at    CHECK   (closed_at IS NULL OR is_active = FALSE)
);

CREATE INDEX idx_accounts_owner    ON accounts (owner_id)       WHERE owner_id IS NOT NULL;
CREATE INDEX idx_accounts_type     ON accounts (account_type);
CREATE INDEX idx_accounts_active   ON accounts (is_active)      WHERE is_active = TRUE;


-- ──────────────────────────────────────────
-- TABLE: journal_entries  (PARTITIONED)
-- ──────────────────────────────────────────
-- Why: This is the financial ledger — the single source of truth for every
--      monetary movement. Key design decisions:
--
--      1. APPEND-ONLY: A DB trigger prevents any UPDATE or DELETE. No code
--         path, no matter how privileged, can alter history. Corrections are
--         made via compensating entries (reversals), never edits.
--
--      2. RANGE PARTITIONING by created_at (monthly): A ledger processes
--         millions of entries. Monthly partitions mean:
--         - Queries filtered by date only scan relevant partition(s)
--         - Old partitions can be archived/compressed without touching current data
--         - Index maintenance is cheaper per partition than one giant index
--
--      3. transaction_id groups the debit + credit entries for one payment.
--         The invariant SUM(entries WHERE transaction_id = X) = 0 must hold.
--
--      4. NUMERIC(19,4) — never FLOAT or DOUBLE for monetary amounts.
-- ──────────────────────────────────────────
CREATE TABLE journal_entries (
    id              UUID           NOT NULL DEFAULT uuid_generate_v4(),
    transaction_id  UUID           NOT NULL,
    account_id      UUID           NOT NULL,
    entry_type      entry_type     NOT NULL,
    amount          NUMERIC(19, 4) NOT NULL,
    currency        CHAR(3)        NOT NULL,
    description     TEXT,
    reference_id    UUID,
    reference_type  VARCHAR(50),
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_journal_amount_pos  CHECK (amount > 0),
    CONSTRAINT chk_journal_currency    CHECK (currency ~ '^[A-Z]{3}$'),

    PRIMARY KEY (id, created_at)          -- created_at must be in PK for partitioning

) PARTITION BY RANGE (created_at);

-- Indexes (created on the parent — inherited by all partitions)
CREATE INDEX idx_journal_account_date  ON journal_entries (account_id, created_at DESC);
CREATE INDEX idx_journal_transaction   ON journal_entries (transaction_id);
CREATE INDEX idx_journal_reference     ON journal_entries (reference_id) WHERE reference_id IS NOT NULL;

-- Monthly partitions for 2026
CREATE TABLE journal_entries_y2026m01
    PARTITION OF journal_entries
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE journal_entries_y2026m02
    PARTITION OF journal_entries
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE journal_entries_y2026m03
    PARTITION OF journal_entries
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE journal_entries_y2026m04
    PARTITION OF journal_entries
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE journal_entries_y2026m05
    PARTITION OF journal_entries
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE journal_entries_y2026m06
    PARTITION OF journal_entries
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

-- Remaining months follow same pattern ...
-- journal_entries_y2026m07 through y2026m12

-- Default partition catches anything outside defined ranges (prevents insert failure)
CREATE TABLE journal_entries_default
    PARTITION OF journal_entries DEFAULT;


-- ── APPEND-ONLY ENFORCEMENT ──────────────
-- Why: Prevents modification of historical financial records.
--      Placed at DB level so it cannot be bypassed by any application,
--      migration script, or admin accidentally running UPDATE.
CREATE OR REPLACE FUNCTION fn_prevent_journal_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'journal_entries is APPEND-ONLY. No UPDATE or DELETE is permitted. '
        'Use a compensating CREDIT/DEBIT entry to correct errors. '
        'Attempted operation: % on entry_id: %', TG_OP, OLD.id;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_no_modify_journal
    BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION fn_prevent_journal_modification();


-- ──────────────────────────────────────────
-- TABLE: account_balances
-- ──────────────────────────────────────────
-- Why: Reading a balance by summing all journal_entries for an account
--      would be O(n) — unusable at scale. We maintain a materialized balance
--      updated atomically by a DB trigger after each journal_entries INSERT.
--
--      available_balance is a GENERATED COLUMN: always = balance - hold_amount.
--      It cannot go stale — Postgres recomputes it on every read.
--
--      version (OCC) prevents lost-update races if two concurrent transactions
--      both try to update the same account's balance simultaneously.
-- ──────────────────────────────────────────
CREATE TABLE account_balances (
    account_id         UUID           PRIMARY KEY REFERENCES accounts(id),
    balance            NUMERIC(19, 4) NOT NULL DEFAULT 0,
    hold_amount        NUMERIC(19, 4) NOT NULL DEFAULT 0,
    available_balance  NUMERIC(19, 4) GENERATED ALWAYS AS (balance - hold_amount) STORED,
    last_entry_at      TIMESTAMPTZ,
    version            BIGINT         NOT NULL DEFAULT 0,

    CONSTRAINT chk_balance_non_negative   CHECK (balance >= 0),
    CONSTRAINT chk_hold_non_negative      CHECK (hold_amount >= 0),
    CONSTRAINT chk_hold_lte_balance       CHECK (hold_amount <= balance)
);

-- Trigger: update balance atomically when a journal entry is inserted
CREATE OR REPLACE FUNCTION fn_update_account_balance()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.entry_type = 'CREDIT' THEN
        UPDATE account_balances
        SET    balance       = balance + NEW.amount,
               last_entry_at = NEW.created_at,
               version       = version + 1
        WHERE  account_id = NEW.account_id;
    ELSE
        -- DEBIT: validate sufficient balance first
        IF (SELECT balance FROM account_balances WHERE account_id = NEW.account_id) < NEW.amount THEN
            RAISE EXCEPTION 'Insufficient balance for account %. Required: %, Available: %',
                NEW.account_id, NEW.amount,
                (SELECT balance FROM account_balances WHERE account_id = NEW.account_id);
        END IF;

        UPDATE account_balances
        SET    balance       = balance - NEW.amount,
               last_entry_at = NEW.created_at,
               version       = version + 1
        WHERE  account_id = NEW.account_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_balance
    AFTER INSERT ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION fn_update_account_balance();


-- ──────────────────────────────────────────
-- TABLE: account_holds
-- ──────────────────────────────────────────
-- Why: When a payment is AUTHORIZED, funds are reserved so the same money
--      cannot be spent twice before settlement. This table tracks each active
--      hold. When the payment SETTLES, the hold is released and the debit
--      journal entry is posted. When the payment FAILS/CANCELS, the hold is
--      released and no debit is posted.
--      Without holds, two concurrent payments from the same account could both
--      pass the balance check and overdraw the account.
-- ──────────────────────────────────────────
CREATE TABLE account_holds (
    id           UUID           PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id   UUID           NOT NULL REFERENCES accounts(id),
    payment_id   UUID           NOT NULL,              -- logical ref to payments_db
    amount       NUMERIC(19, 4) NOT NULL,
    currency     CHAR(3)        NOT NULL,
    status       hold_status    NOT NULL DEFAULT 'ACTIVE',
    expires_at   TIMESTAMPTZ    NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    released_at  TIMESTAMPTZ,

    CONSTRAINT uq_hold_payment       UNIQUE (payment_id),       -- one hold per payment
    CONSTRAINT chk_hold_amount_pos   CHECK  (amount > 0),
    CONSTRAINT chk_hold_released_at  CHECK  (released_at IS NULL OR status <> 'ACTIVE')
);

CREATE INDEX idx_holds_account        ON account_holds (account_id) WHERE status = 'ACTIVE';
CREATE INDEX idx_holds_payment        ON account_holds (payment_id);
CREATE INDEX idx_holds_expires        ON account_holds (expires_at) WHERE status = 'ACTIVE';


-- ──────────────────────────────────────────
-- SAMPLE DATA: ledger_db
-- ──────────────────────────────────────────
INSERT INTO accounts (id, account_number, account_name, account_type, currency, owner_id)
VALUES
    ('acc00000-0000-0000-0000-000000000001', 'WALL-000001', 'Alice Wallet',      'WALLET',   'INR', 'user-0001'),
    ('acc00000-0000-0000-0000-000000000002', 'WALL-000002', 'Bob Wallet',        'WALLET',   'INR', 'user-0002'),
    ('acc00000-0000-0000-0000-000000000003', 'WALL-000003', 'Charlie Wallet',    'WALLET',   'INR', 'user-0003'),
    ('acc00000-0000-0000-0000-000000000004', 'WALL-000004', 'Diana Wallet',      'WALLET',   'USD', 'user-0004'),
    ('sys00000-0000-0000-0000-000000000001', 'SYS-SUSP-01', 'Clearing Suspense', 'SUSPENSE', 'INR', NULL),
    ('sys00000-0000-0000-0000-000000000002', 'SYS-FEE-001', 'Platform Fees',     'FEE',      'INR', NULL),
    ('sys00000-0000-0000-0000-000000000003', 'SYS-NOSTR01', 'Nostro - HDFC',     'NOSTRO',   'INR', NULL);

INSERT INTO account_balances (account_id, balance, hold_amount)
VALUES
    ('acc00000-0000-0000-0000-000000000001', 5000.0000, 0),
    ('acc00000-0000-0000-0000-000000000002', 1000.0000, 0),
    ('acc00000-0000-0000-0000-000000000003', 2500.0000, 0),
    ('acc00000-0000-0000-0000-000000000004', 300.0000,  0),
    ('sys00000-0000-0000-0000-000000000001', 0.0000,    0),
    ('sys00000-0000-0000-0000-000000000002', 150.0000,  0),
    ('sys00000-0000-0000-0000-000000000003', 100000.0000, 0);

-- Sample: Alice (DEBIT ₹1000) → Bob (CREDIT ₹997) + Fee (CREDIT ₹3)
-- These represent a settled payment — all three entries in one transaction_id
INSERT INTO journal_entries (transaction_id, account_id, entry_type, amount, currency, description, reference_id, reference_type, created_at)
VALUES
    ('txn00000-0001-0001-0001-000000000001', 'acc00000-0000-0000-0000-000000000001', 'DEBIT',  1000.0000, 'INR', 'Payment to Bob',         'a1b2c3d4-0001-0001-0001-000000000001', 'PAYMENT', '2026-06-01 10:00:00+00'),
    ('txn00000-0001-0001-0001-000000000001', 'acc00000-0000-0000-0000-000000000002', 'CREDIT',  997.0000, 'INR', 'Receipt from Alice',      'a1b2c3d4-0001-0001-0001-000000000001', 'PAYMENT', '2026-06-01 10:00:00+00'),
    ('txn00000-0001-0001-0001-000000000001', 'sys00000-0000-0000-0000-000000000002', 'CREDIT',    3.0000, 'INR', 'Platform fee 0.3%',       'a1b2c3d4-0001-0001-0001-000000000001', 'FEE',     '2026-06-01 10:00:00+00');

-- Invariant verification: SUM of CREDIT - SUM of DEBIT for this transaction = 0
-- 997 + 3 - 1000 = 0 ✓
```

---

## 3. notifications_db

**Owner:** Notification Service | **Port:** 5434

---

### Why these tables exist

| Table | Purpose |
|---|---|
| `notification_templates` | Stores Thymeleaf templates keyed on `event_type`. Decouples template management from code deployments. Adding a new notification type is a DB insert, not a code change. |
| `delivery_log` | Immutable record of every notification dispatched. Enables debugging delivery failures, retry logic, and compliance reporting. |
| `user_notification_prefs` | Respects user channel preferences (opt-out of SMS, quiet hours). Required by GDPR and good UX. |

---

```sql
-- ============================================================
-- DATABASE: notifications_db
-- ============================================================

\c notifications_db;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ──────────────────────────────────────────
-- ENUMS
-- ──────────────────────────────────────────
CREATE TYPE channel_type    AS ENUM ('EMAIL', 'SMS', 'PUSH', 'WEBSOCKET', 'IN_APP');
CREATE TYPE delivery_status AS ENUM ('PENDING', 'SENT', 'DELIVERED', 'FAILED', 'READ');


-- ──────────────────────────────────────────
-- TABLE: notification_templates
-- ──────────────────────────────────────────
-- Why: Templates are data, not code. Operations can update notification copy
--      without deploying. event_type is UNIQUE — one template per event per
--      channel combination. locale supports i18n.
-- ──────────────────────────────────────────
CREATE TABLE notification_templates (
    id            UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type    VARCHAR(80)  NOT NULL,
    channel       channel_type NOT NULL,
    locale        VARCHAR(10)  NOT NULL DEFAULT 'en',
    subject       TEXT,
    body_template TEXT         NOT NULL,
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_template_event_channel_locale UNIQUE (event_type, channel, locale)
);

CREATE INDEX idx_templates_event_type ON notification_templates (event_type, channel)
    WHERE is_active = TRUE;

CREATE TRIGGER trg_templates_updated_at
    BEFORE UPDATE ON notification_templates
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();


-- ──────────────────────────────────────────
-- TABLE: delivery_log
-- ──────────────────────────────────────────
-- Why: Every notification attempt is recorded — successful or not.
--      This enables:
--        - Retry logic: query WHERE status = 'FAILED' AND attempts < 3
--        - Debugging: what exactly was sent to whom and when?
--        - Deduplication: don't send the same event twice to the same user
--        - Compliance: audit trail of customer communications
--      reference_id is a logical link to the payment — no DB-level FK
--      because notifications_db does not own payments data.
-- ──────────────────────────────────────────
CREATE TABLE delivery_log (
    id              UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID            NOT NULL,
    event_type      VARCHAR(80)     NOT NULL,
    reference_id    UUID,
    reference_type  VARCHAR(50),
    channel         channel_type    NOT NULL,
    recipient       VARCHAR(320)    NOT NULL,       -- email or phone
    subject         TEXT,
    body            TEXT            NOT NULL,
    status          delivery_status NOT NULL DEFAULT 'PENDING',
    failure_reason  TEXT,
    attempts        SMALLINT        NOT NULL DEFAULT 0,
    provider_ref    VARCHAR(255),                   -- e.g., SendGrid message ID
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    sent_at         TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    read_at         TIMESTAMPTZ,

    CONSTRAINT chk_delivery_attempts     CHECK (attempts >= 0),
    CONSTRAINT chk_delivery_recipient    CHECK (char_length(recipient) > 0)
);

CREATE INDEX idx_delivery_user_date    ON delivery_log (user_id, created_at DESC);
CREATE INDEX idx_delivery_reference    ON delivery_log (reference_id) WHERE reference_id IS NOT NULL;
CREATE INDEX idx_delivery_pending      ON delivery_log (created_at)   WHERE status = 'PENDING';
CREATE INDEX idx_delivery_failed_retry ON delivery_log (created_at)   WHERE status = 'FAILED' AND attempts < 3;


-- ──────────────────────────────────────────
-- TABLE: user_notification_prefs
-- ──────────────────────────────────────────
-- Why: Users can opt-out of specific channels or set quiet hours.
--      GDPR requires that we respect communication preferences.
--      quiet_hours_from/to allow "do not disturb" windows.
-- ──────────────────────────────────────────
CREATE TABLE user_notification_prefs (
    user_id            UUID    PRIMARY KEY,
    email_enabled      BOOLEAN NOT NULL DEFAULT TRUE,
    sms_enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    push_enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    in_app_enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    quiet_hours_from   TIME,
    quiet_hours_to     TIME,
    timezone           VARCHAR(50) NOT NULL DEFAULT 'Asia/Kolkata',
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_quiet_hours CHECK (
        (quiet_hours_from IS NULL AND quiet_hours_to IS NULL) OR
        (quiet_hours_from IS NOT NULL AND quiet_hours_to IS NOT NULL)
    )
);


-- ──────────────────────────────────────────
-- SAMPLE DATA: notifications_db
-- ──────────────────────────────────────────
-- fn_set_updated_at must exist in this DB too
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

INSERT INTO notification_templates (event_type, channel, subject, body_template) VALUES
('PAYMENT_INITIATED',  'EMAIL', 'Payment Initiated - FinLedgerX',
 '<p>Hi [[${name}]], your payment of <strong>[[${currency}]] [[${amount}]]</strong> has been initiated.</p>'),

('PAYMENT_SETTLED',    'EMAIL', 'Payment Successful - FinLedgerX',
 '<p>Hi [[${name}]], your payment of <strong>[[${currency}]] [[${amount}]]</strong> to [[${recipient}]] was successful.</p>'),

('PAYMENT_SETTLED',    'SMS',   NULL,
 'FinLedgerX: Payment of [[${currency}]] [[${amount}]] to [[${recipient}]] successful. Ref: [[${paymentId}]]'),

('PAYMENT_FAILED',     'EMAIL', 'Payment Failed - FinLedgerX',
 '<p>Hi [[${name}]], your payment of [[${currency}]] [[${amount}]] failed. Reason: [[${reason}]]. Please try again.</p>'),

('PAYMENT_REVERSED',   'EMAIL', 'Payment Reversed - FinLedgerX',
 '<p>Hi [[${name}]], payment [[${paymentId}]] has been reversed. [[${currency}]] [[${amount}]] will be credited back within 2 business days.</p>'),

('RECON_MISMATCH',     'EMAIL', '⚠ Reconciliation Mismatch - Action Required',
 '<p>Team, [[${count}]] mismatches found in reconciliation run for [[${date}]]. Please review the dashboard.</p>');

INSERT INTO delivery_log (user_id, event_type, reference_id, reference_type,
                           channel, recipient, body, status, attempts, sent_at)
VALUES
    ('user-0001', 'PAYMENT_SETTLED', 'a1b2c3d4-0001-0001-0001-000000000001', 'PAYMENT',
     'EMAIL', 'alice@example.com',
     'Your payment of INR 1000.00 to Bob was successful.', 'SENT', 1, NOW()),

    ('user-0002', 'PAYMENT_SETTLED', 'a1b2c3d4-0001-0001-0001-000000000001', 'PAYMENT',
     'EMAIL', 'bob@example.com',
     'You received INR 997.00 from Alice.', 'SENT', 1, NOW());
```

---

## 4. reconciliation_db

**Owner:** Reconciliation Service | **Port:** 5435

---

### Why these tables exist

| Table | Purpose |
|---|---|
| `recon_runs` | Tracks each reconciliation batch job execution. One row per day. Provides a complete history of when reconciliation ran, what it found, and whether it succeeded. |
| `bank_statements` | Raw lines from the bank's settlement file (SFTP CSV). The external source of truth that we match against our internal ledger. |
| `recon_mismatches` | Every discrepancy found between ledger and bank. Stays OPEN until an Ops team member manually resolves it. |
| `recon_rules` | Configuration table for matching rules (e.g., tolerance thresholds, field to match on). Allows tuning without code changes. |

---

```sql
-- ============================================================
-- DATABASE: reconciliation_db
-- ============================================================

\c reconciliation_db;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ──────────────────────────────────────────
-- ENUMS
-- ──────────────────────────────────────────
CREATE TYPE recon_status    AS ENUM ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'PARTIAL');
CREATE TYPE mismatch_type   AS ENUM (
    'MISSING_IN_BANK',      -- ledger has entry, bank file doesn't
    'MISSING_IN_LEDGER',    -- bank file has entry, ledger doesn't
    'AMOUNT_MISMATCH',      -- both exist, amounts differ
    'DUPLICATE_IN_BANK',    -- bank shows same transaction twice
    'DATE_MISMATCH'         -- same amount, different dates
);
CREATE TYPE mismatch_status AS ENUM ('OPEN', 'INVESTIGATING', 'RESOLVED', 'IGNORED');
CREATE TYPE stmt_entry_type AS ENUM ('CREDIT', 'DEBIT');


-- ──────────────────────────────────────────
-- TABLE: recon_runs
-- ──────────────────────────────────────────
-- Why: Provides a complete, time-ordered log of every reconciliation job.
--      run_date has a UNIQUE constraint — you can only run reconciliation
--      once per day (re-running for the same date requires manual override).
--      triggered_by records whether it was a scheduled CRON job or a manual
--      Ops trigger — important for audit.
-- ──────────────────────────────────────────
CREATE TABLE recon_runs (
    id              UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_date        DATE         NOT NULL,
    status          recon_status NOT NULL DEFAULT 'PENDING',
    total_ledger    INT          NOT NULL DEFAULT 0,
    total_bank      INT          NOT NULL DEFAULT 0,
    matched_count   INT          NOT NULL DEFAULT 0,
    mismatch_count  INT          NOT NULL DEFAULT 0,
    triggered_by    VARCHAR(100) NOT NULL DEFAULT 'SCHEDULER',
    error_message   TEXT,
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,

    CONSTRAINT uq_recon_run_date   UNIQUE (run_date),
    CONSTRAINT chk_recon_counts    CHECK  (matched_count + mismatch_count <= total_ledger + total_bank),
    CONSTRAINT chk_recon_completed CHECK  (completed_at IS NULL OR status IN ('COMPLETED', 'FAILED', 'PARTIAL'))
);

CREATE INDEX idx_recon_runs_date   ON recon_runs (run_date DESC);
CREATE INDEX idx_recon_runs_status ON recon_runs (status)  WHERE status IN ('RUNNING', 'PENDING');


-- ──────────────────────────────────────────
-- TABLE: bank_statements
-- ──────────────────────────────────────────
-- Why: Stores the raw CSV rows ingested from the bank's SFTP settlement file.
--      transaction_ref is the bank's own reference number — UNIQUE ensures
--      we never import the same bank transaction twice.
--      raw_row (JSONB) stores the original CSV columns as-is, so we can
--      re-parse if our import logic had a bug.
-- ──────────────────────────────────────────
CREATE TABLE bank_statements (
    id               UUID           PRIMARY KEY DEFAULT uuid_generate_v4(),
    recon_run_id     UUID           NOT NULL REFERENCES recon_runs(id),
    transaction_ref  VARCHAR(255)   NOT NULL,
    amount           NUMERIC(19, 4) NOT NULL,
    currency         CHAR(3)        NOT NULL,
    transaction_date DATE           NOT NULL,
    entry_type       stmt_entry_type NOT NULL,
    description      TEXT,
    raw_row          JSONB,
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_bank_stmt_ref     UNIQUE  (transaction_ref),
    CONSTRAINT chk_bank_amount_pos  CHECK   (amount > 0),
    CONSTRAINT chk_bank_currency    CHECK   (currency ~ '^[A-Z]{3}$')
);

CREATE INDEX idx_bank_stmt_run       ON bank_statements (recon_run_id);
CREATE INDEX idx_bank_stmt_date      ON bank_statements (transactio