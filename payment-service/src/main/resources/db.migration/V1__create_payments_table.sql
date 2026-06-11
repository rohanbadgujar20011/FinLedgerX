-- ============================================================
-- V1: payments table
-- ============================================================

CREATE TABLE IF NOT EXISTS payments (
    id               UUID          NOT NULL DEFAULT gen_random_uuid(),
    from_account_id  UUID          NOT NULL,
    to_account_id    UUID          NOT NULL,
    amount           NUMERIC(19,4) NOT NULL,
    currency         CHAR(3)       NOT NULL,
    status           VARCHAR(20)   NOT NULL DEFAULT 'INITIATED',
    payment_method   VARCHAR(30)   NOT NULL,
    description      VARCHAR(500),
    idempotency_key  VARCHAR(255)  NOT NULL,
    correlation_id   UUID          NOT NULL,
    failure_reason   VARCHAR(1000),
    processed_at     TIMESTAMPTZ,
    settled_at       TIMESTAMPTZ,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    version          BIGINT        NOT NULL DEFAULT 0,

    CONSTRAINT pk_payments                   PRIMARY KEY (id),
    CONSTRAINT uq_payments_idempotency_key   UNIQUE      (idempotency_key),
    CONSTRAINT chk_payments_diff_accounts    CHECK       (from_account_id <> to_account_id),
    CONSTRAINT chk_payments_amount_pos       CHECK       (amount > 0),
    CONSTRAINT chk_payments_currency         CHECK       (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT chk_payments_status           CHECK       (status IN (
        'INITIATED', 'VALIDATING', 'AUTHORIZED', 'PROCESSING',
        'SETTLED', 'RECONCILED', 'FAILED', 'REVERSED', 'CANCELLED'
    ))
);

-- ── Indexes ──────────────────────────────────────────────────
CREATE INDEX idx_payments_from_account ON payments (from_account_id);
CREATE INDEX idx_payments_to_account   ON payments (to_account_id);
CREATE INDEX idx_payments_status       ON payments (status);
CREATE INDEX idx_payments_created_at   ON payments (created_at DESC);
CREATE INDEX idx_payments_correlation  ON payments (correlation_id);

-- ── Auto-update updated_at ────────────────────────────────────
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_payments_updated_at
    BEFORE UPDATE ON payments
    FOR EACH ROW
    EXECUTE FUNCTION fn_set_updated_at();
