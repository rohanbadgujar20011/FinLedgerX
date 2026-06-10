# FinLedgerX — High-Level Design (HLD)

> **Principal Architect View** | Java 21 · Spring Boot 3 · PostgreSQL · Kafka · Redis · Prometheus · Grafana · React UI
>
> This document is the single source of truth for FinLedgerX architecture. Reference it when generating code, writing LLD, or designing schemas.

---

## Table of Contents

1. [Business Problem](#1-business-problem)
2. [Architecture Overview](#2-architecture-overview)
3. [Service Responsibilities](#3-service-responsibilities)
4. [Request Flow (Synchronous)](#4-request-flow-synchronous)
5. [Kafka Event Flow (Asynchronous)](#5-kafka-event-flow-asynchronous)
6. [Database Ownership](#6-database-ownership)
7. [Monitoring Architecture](#7-monitoring-architecture)
8. [Deployment Architecture](#8-deployment-architecture)
9. [Key Design Decisions](#9-key-design-decisions)

---

## 1. Business Problem

FinLedgerX is a **Real-Time Payment Processing & Double-Entry Ledger Platform**.

Every fintech must solve: *process payments at scale without ever creating money, losing money, or misattributing money — and prove it to regulators on demand.*

Core invariant: **Sum of all ledger entries across all accounts always equals zero.**

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                      CLIENT TIER                        │
│              React UI  (Dashboard · Payments)           │
└──────────────────────────┬──────────────────────────────┘
                           │ HTTPS / REST
┌──────────────────────────▼──────────────────────────────┐
│                     GATEWAY TIER                        │
│         API Gateway  (Auth · Rate Limit · Routing)      │
└────┬──────────────┬───────────────┬──────────────┬──────┘
     │              │               │              │
┌────▼────┐   ┌─────▼─────┐  ┌─────▼──────┐  ┌───▼──────────┐
│ Payment │   │  Ledger   │  │Notification│  │Reconciliation│
│ Service │   │  Service  │  │  Service   │  │   Service    │
│  :8081  │   │   :8082   │  │   :8083    │  │    :8084     │
└────┬────┘   └─────┬─────┘  └─────┬──────┘  └───┬──────────┘
     │              │               │              │
     └──────────────┴───────────────┴──────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                    KAFKA EVENT BUS  :9092                │
│  payment.events │ ledger.entries │ notification.events  │
│  recon.jobs     │ payment.dlq (Dead Letter Queue)       │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                      DATA TIER                          │
│  payments_db  │  ledger_db  │  notifications_db         │
│  reconciliation_db          │  Redis  :6379             │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                  OBSERVABILITY TIER                     │
│   Prometheus :9090  →  Grafana :3000                    │
│   Micrometer (Spring Actuator)  │  Structured Logging   │
└─────────────────────────────────────────────────────────┘
```

---

## 3. Service Responsibilities

### Payment Service (:8081)
- **Owns** the payment state machine: `INITIATED → VALIDATING → AUTHORIZED → PROCESSING → SETTLED → FAILED`
- Enforces **idempotency** via Redis-backed idempotency key store (TTL: 24h)
- Runs fraud/velocity checks before authorization
- Uses **Transactional Outbox Pattern**: writes outbox row + payment row in the same DB transaction; a separate poller publishes to Kafka
- Never publishes to Kafka directly inside a business transaction
- Exposes REST APIs: `POST /api/v1/payments`, `GET /api/v1/payments/{id}`

### Ledger Service (:8082)
- **Owns** financial truth — all double-entry journal entries
- Consumes `payment.events` from Kafka only — zero direct API writes from outside
- Writes **immutable** journal entries (append-only enforced at DB trigger level)
- Maintains materialized `account_balances` table for performance
- Balance is always recomputable via `SUM(journal_entries)` — materialized view is a cache, journal is the truth
- Exposes read APIs: `GET /api/v1/accounts/{id}/balance`, `GET /api/v1/accounts/{id}/entries`

### Notification Service (:8083)
- Pure **consumer** — subscribes to all event topics
- Renders templates (Thymeleaf) and dispatches via Email / SMS / WebSocket
- Writes `delivery_log` for traceability and retry
- Zero write path to financial data — fully isolated from business state
- Exposes: `GET /api/v1/notifications/{userId}`

### Reconciliation Service (:8084)
- Runs **scheduled batch jobs** (Quartz) at T+1 daily
- Ingests bank settlement files (CSV/SFTP), matches against `ledger.journal_entries`
- Flags discrepancies into `recon_mismatches` table
- Exposes on-demand reconciliation APIs for Ops
- Exposes: `POST /api/v1/recon/run`, `GET /api/v1/recon/mismatches`

---

## 4. Request Flow (Synchronous)

```
1. User submits payment via React UI
2. API Gateway validates JWT, checks rate limit (Redis), routes to Payment Service
3. Payment Service:
   a. Check idempotency key in Redis — if hit, return cached 202
   b. Validate payload (amount, currency, account existence)
   c. Run fraud/velocity rules
   d. Reserve funds — write HOLD journal entry to outbox
   e. Persist payment row (status = AUTHORIZED)
   f. Commit DB transaction (outbox entry now visible to poller)
   g. Return 202 ACCEPTED + { paymentId }
4. Outbox Poller (separate thread) publishes event to Kafka: payment.events
5. Client polls GET /payments/{id} or listens on WebSocket for status updates
```

**Rule:** Never block the synchronous API call on Kafka. The 202 is returned before Kafka publish.

---

## 5. Kafka Event Flow (Asynchronous)

```
PUBLISHERS:
  Payment Service      → payment.events   (on every state transition)
  Ledger Service       → ledger.entries   (on every journal write)
  Reconciliation Svc   → recon.jobs       (scheduled trigger)

CONSUMERS:
  payment.events       → Ledger Service        (write journal entries)
                       → Notification Service  (send payment alerts)
                       → Reconciliation Svc    (register txns for matching)

  ledger.entries       → Notification Service  (large transaction alerts)
                       → Reconciliation Svc    (cross-reference)

  payment.dlq          → Ops Alert System      (manual review after 3 retries)
```

**Topic Configuration:**
- Partitions: 3 minimum, keyed on `accountId` for per-account ordering
- Retention: 7 days
- Each service has its own consumer group (independent offset tracking)
- DLQ: events routed here after 3 retry failures; triggers Grafana alert

**Event Schema (payment.events example):**
```json
{
  "eventId": "uuid",
  "eventType": "PAYMENT_AUTHORIZED",
  "paymentId": "uuid",
  "fromAccountId": "uuid",
  "toAccountId": "uuid",
  "amount": "1000.00",
  "currency": "INR",
  "timestamp": "2026-06-03T10:00:00Z",
  "correlationId": "uuid"
}
```

---

## 6. Database Ownership

**Rule: No service ever touches another service's database.**

| Service | Database | Port | Key Tables |
|---|---|---|---|
| Payment Service | `payments_db` | 5432 | `payments`, `idempotency_keys`, `outbox_events` |
| Ledger Service | `ledger_db` | 5433 | `accounts`, `journal_entries`, `account_balances` |
| Notification Service | `notifications_db` | 5434 | `notification_templates`, `delivery_log` |
| Reconciliation Service | `reconciliation_db` | 5435 | `recon_runs`, `recon_mismatches`, `bank_statements` |
| All Services | Redis | 6379 | Idempotency TTL, rate-limit counters, sessions, balance cache |

**`journal_entries` immutability rule:**
```sql
-- DB trigger enforced at Postgres level
CREATE OR REPLACE FUNCTION prevent_journal_modification()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'journal_entries is append-only. Modifications are not permitted.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER no_update_journal BEFORE UPDATE OR DELETE ON journal_entries
FOR EACH ROW EXECUTE FUNCTION prevent_journal_modification();
```

---

## 7. Monitoring Architecture

**Metrics pipeline:**
```
Spring Boot Actuator (/actuator/prometheus)
  → Micrometer (metric instrumentation)
    → Prometheus (scrape every 15s)
      → Grafana (dashboards + alerting)
```

**Key Metrics per Service:**

| Service | Metric | Alert Threshold |
|---|---|---|
| Payment Service | `payment_initiated_total` | — |
| Payment Service | `payment_failed_total` rate | > 5% → P1 |
| Payment Service | `idempotency_hit_ratio` | — |
| Payment Service | `http_request_duration_p99` | > 2s → P1 |
| Ledger Service | `journal_entries_written_total` | — |
| Ledger Service | `balance_mismatch_total` | > 0 → P0 🚨 |
| Notification Svc | `notification_delivery_success_rate` | < 95% → P2 |
| Reconciliation | `recon_mismatch_count` | > 0 → P1 |
| Kafka | `consumer_lag` | > 10,000 → P1 |
| All | `service UP` | == 0 → P0 🚨 |

**Structured Logging (Logback JSON):**
- Every log line includes: `correlationId`, `traceId`, `spanId`, `paymentId`, `accountId`
- Enables tracing a single payment across all four services in log aggregation (ELK/Loki)

---

## 8. Deployment Architecture

### Local / Dev — Docker Compose
```yaml
# Services: payment-service, ledger-service, notification-service, recon-service
# Infra:    postgres x4, redis, kafka, zookeeper, prometheus, grafana
# Ports as defined above
```

Multi-stage Dockerfile (Java 21):
```dockerfile
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /app
COPY . .
RUN ./mvnw package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
```

### Production — Kubernetes
- Each service: `Deployment` (3 replicas) + `HorizontalPodAutoscaler` (CPU + Kafka lag metric)
- Databases: `StatefulSet` with PVCs, or managed RDS/Cloud SQL
- Kafka: MSK (AWS) or Confluent Cloud — not self-managed in prod
- Secrets: Kubernetes Secrets + HashiCorp Vault (never in `application.yml`)
- Network policies: Services communicate only via Kafka (async) or ClusterIP (sync). Cross-DB access blocked by `NetworkPolicy`

### CI/CD Pipeline
```
GitHub Push
  → GitHub Actions
    → Maven build + unit tests (JUnit 5 + Mockito)
    → Integration tests (Testcontainers — real Postgres + Kafka)
    → Docker build + push to ECR
    → Helm chart deploy to K8s staging
    → Smoke tests
    → Promote to production
```

---

## 9. Key Design Decisions

### Transactional Outbox Pattern
> Never publish to Kafka directly inside a business transaction. Write to `outbox_events` table atomically with the business record. A separate poller reads and publishes. Guarantees exactly-once semantics between DB and Kafka — no lost events even on Kafka downtime.

### Ledger Immutability (Append-Only)
> `journal_entries` is enforced append-only at the **Postgres trigger level**, not just application code. No UPDATE or DELETE is physically possible. A compromised service or developer mistake cannot corrupt financial history.

### Balance Consistency Strategy
> Maintain a materialized `account_balances` for read performance, but it is **always recomputable** from `journal_entries`. If they ever diverge, `balance_mismatch_total` metric fires a P0 alert. The materialized view is a cache — the journal is the truth.

### Idempotency as First-Class Concern
> Every payment request carries an `X-Idempotency-Key` header. Payment Service checks Redis before processing. Same key within 24h returns the cached response. Prevents duplicate charges on client retries.

### No Cross-Service DB Access
> Services communicate only through Kafka events or REST APIs. No shared DB schemas. Enforced by Docker network isolation in dev and K8s `NetworkPolicy` in prod.

---

## Tech Stack Reference

| Component | Technology | Version |
|---|---|---|
| Language | Java | 21 |
| Framework | Spring Boot | 3.x |
| Database | PostgreSQL | 16 (Docker) |
| Message Bus | Apache Kafka | 3.x (Docker) |
| Cache | Redis | 7.x (Docker) |
| Metrics | Prometheus | Latest (Docker) |
| Dashboards | Grafana | Latest (Docker) |
| Frontend | React | 18.x |
| DB Migration | Flyway | — |
| Testing | JUnit 5 + Testcontainers | — |
| Containerization | Docker + Docker Compose | — |
| Prod Orchestration | Kubernetes + Helm | — |

---

*Last updated: 2026-06-03 | Author: FinLedgerX Architecture Team*
