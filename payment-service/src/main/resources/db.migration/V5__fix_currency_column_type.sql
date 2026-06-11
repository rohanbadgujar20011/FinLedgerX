```yaml
-- V5: Fix currency column type — CHAR(3) → VARCHAR(3)
-- Hibernate expects VARCHAR; CHAR(3) was created in V1 by mistake.
ALTER TABLE payments ALTER COLUMN currency TYPE VARCHAR(3);

```