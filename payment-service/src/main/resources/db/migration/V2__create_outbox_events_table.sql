-- ============================================================
-- V2: outbox_events table (Transactional Outbox Pattern)
-- ============================================================

CREATE TABLE IF NOT EXISTS outbox_events (
    id             UUID         NOT NULL DEFAULT gen_random_uuid(),
    topic          VARCHAR(200) NOT NULL,
    partition_key  VARCHAR(255) NOT NULL,
    event_type     VARCHAR(100) NOT NULL,
    aggregate_id   VARCHAR(255) NOT NULL,
    payload        TEXT         NOT NULL,
    published      BOOLEAN      NOT NULL DEFAULT FALSE,
    published_at   TIMESTAMPTZ,
    retry_count    INT          NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_outbox_events PRIMARY KEY (id)
);

-- ── Partial index: only unpublished rows are ever queried by the poller.
-- This index is tiny and extremely fast — it excludes all published rows.
CREATE INDEX idx_outbox_unpublished
    ON outbox_events (created_at ASC)
    WHERE published = FALSE;

CREATE INDEX idx_outbox_aggregate_id ON outbox_events (aggregate_id);
