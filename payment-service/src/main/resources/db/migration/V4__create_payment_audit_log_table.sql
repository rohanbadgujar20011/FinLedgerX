-- ============================================================
-- V4: payment_audit_log table (append-only)
-- ============================================================

CREATE TABLE IF NOT EXISTS payment_audit_log (
    id            UUID         NOT NULL DEFAULT gen_random_uuid(),
    payment_id    UUID         NOT NULL,
    action        VARCHAR(100) NOT NULL,
    from_status   VARCHAR(20),
    to_status     VARCHAR(20),
    actor         VARCHAR(100) NOT NULL,
    details       TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_payment_audit_log PRIMARY KEY (id)
);

CREATE INDEX idx_audit_payment_id ON payment_audit_log (payment_id);
CREATE INDEX idx_audit_created_at ON payment_audit_log (created_at DESC);

-- ── Enforce append-only: no UPDATE or DELETE permitted ────────
CREATE OR REPLACE FUNCTION fn_prevent_audit_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'payment_audit_log is APPEND-ONLY. No UPDATE or DELETE permitted.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_no_modify_audit
    BEFORE UPDATE OR DELETE ON payment_audit_log
    FOR EACH ROW
    EXECUTE FUNCTION fn_prevent_audit_modification();
