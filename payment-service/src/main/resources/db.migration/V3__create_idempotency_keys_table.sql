-- ============================================================
-- V3: idempotency_keys table
-- Durable fallback for idempotency (Redis is the first line).
-- ============================================================

CREATE TABLE IF NOT EXISTS idempotency_keys (
    id               UUID         NOT NULL DEFAULT gen_random_uuid(),
    idempotency_key  VARCHAR(255) NOT NULL,
    payment_id       UUID,
    request_hash     VARCHAR(64),
    response_body    TEXT,
    status           VARCHAR(20)  NOT NULL DEFAULT 'IN_FLIGHT',
    expires_at       TIMESTAMPTZ  NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_idempotency_keys           PRIMARY KEY (id),
    CONSTRAINT uq_idempotency_key            UNIQUE      (idempotency_key),
    CONSTRAINT chk_idempotency_status        CHECK       (status IN ('IN_FLIGHT', 'COMPLETED', 'FAILED'))
);

CREATE INDEX idx_idempotency_key        ON idempotency_keys (idempotency_key);
CREATE INDEX idx_idempotency_expires_at ON idempotency_keys (expires_at);
