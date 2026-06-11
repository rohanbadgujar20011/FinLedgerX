# FinLedgerX — Docker Architecture

> **Code generation reference.** Everything in this file maps 1-to-1 to `docker-compose.yml`.

---

## 1. Container Inventory

| Container | Image | Ports (host→container) | Role |
|---|---|---|---|
| `finledgerx-payments-db` | postgres:16-alpine | 5432→5432 | payments_db |
| `finledgerx-ledger-db` | postgres:16-alpine | 5433→5432 | ledger_db |
| `finledgerx-notifications-db` | postgres:16-alpine | 5434→5432 | notifications_db |
| `finledgerx-reconciliation-db` | postgres:16-alpine | 5435→5432 | reconciliation_db |
| `finledgerx-redis` | redis:7-alpine | 6379→6379 | Cache / locks / rate limiter |
| `finledgerx-zookeeper` | cp-zookeeper:7.6.0 | — (internal) | Kafka coordination |
| `finledgerx-kafka` | cp-kafka:7.6.0 | 9092, 9093 (local), 9999 (JMX) | Message broker |
| `finledgerx-schema-registry` | cp-schema-registry:7.6.0 | 8085→8081 | Avro schema registry |
| `finledgerx-kafka-init` | cp-kafka:7.6.0 | — (one-shot) | Topic creation |
| `finledgerx-payment-service` | finledgerx/payment-service | 8081, 9101 (metrics) | Payment microservice |
| `finledgerx-ledger-service` | finledgerx/ledger-service | 8082, 9102 (metrics) | Ledger microservice |
| `finledgerx-notification-service` | finledgerx/notification-service | 8083, 9103 (metrics) | Notification microservice |
| `finledgerx-reconciliation-service` | finledgerx/reconciliation-service | 8084, 9104 (metrics) | Reconciliation microservice |
| `finledgerx-react-ui` | finledgerx/react-ui | 3001→80 | React frontend |
| `finledgerx-mailhog` | mailhog/mailhog | 1025 (SMTP), 8025 (UI) | Dev email catcher |
| `finledgerx-prometheus` | prom/prometheus | 9090→9090 | Metrics collection |
| `finledgerx-grafana` | grafana/grafana | 3000→3000 | Dashboards & alerts |

**Dev-only** (docker-compose.override.yml):

| Container | Image | Port | Role |
|---|---|---|---|
| `finledgerx-kafka-ui` | provectuslabs/kafka-ui | 8090→8080 | Kafka browser |
| `finledgerx-pgadmin` | dpage/pgadmin4 | 5050→5050 | DB browser |

---

## 2. Network Topology

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        HOST MACHINE                                     │
│                                                                         │
│  3001 → react-ui    9090 → prometheus    3000 → grafana                 │
│  8081-8084 → services   5432-5435 → DBs (dev only)                      │
│                                                                         │
│  ┌──────────────────┐   ┌─────────────────────────────────────────────┐ │
│  │ finledgerx-      │   │         finledgerx-backend                  │ │
│  │ frontend         │   │                                             │ │
│  │                  │   │  payment-service   ledger-service           │ │
│  │  react-ui ───────┼───┤  notification-service  recon-service        │ │
│  │                  │   │  kafka   zookeeper  schema-registry         │ │
│  │                  │   │  mailhog                                    │ │
│  └──────────────────┘   └────────────────┬────────────────────────────┘ │
│                                          │                              │
│                    ┌─────────────────────▼────────────────────────────┐ │
│                    │            finledgerx-data                        │ │
│                    │                                                   │ │
│                    │  payments-db  ledger-db  notifications-db         │ │
│                    │  reconciliation-db  redis                         │ │
│                    └───────────────────────────────────────────────────┘ │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ finledgerx-monitoring                                            │    │
│  │  prometheus  grafana                                             │    │
│  │  (also connected to finledgerx-backend to scrape /actuator)     │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Startup Wave Order

Dependency chain enforced by `depends_on: condition: service_healthy`:

```
Wave 1 (data tier — all parallel)
  payments-db ─────┐
  ledger-db ───────┤
  notifications-db ┤ ──> Wave 2
  reconciliation-db┤
  redis ───────────┘

Wave 2 (messaging)
  zookeeper ──> kafka ──> schema-registry
                   │
                   └──> Wave 3

Wave 3 (topic init — one-shot, exits 0)
  kafka-init ──> Wave 4

Wave 4 (microservices — all parallel, each waits for its own DB + kafka + redis)
  payment-service ─────┐
  ledger-service ──────┤ ──> Wave 5
  notification-service ┤
  reconciliation-service┘

Wave 5 (frontend)
  react-ui (waits for payment-service healthy)

Wave 6 (observability — waits for at least payment-service healthy)
  prometheus ──> grafana
```

---

## 4. Volume Strategy

| Volume | Container | Purpose |
|---|---|---|
| `finledgerx-payments-db-data` | payments-db | Persistent Postgres data |
| `finledgerx-ledger-db-data` | ledger-db | Persistent Postgres data |
| `finledgerx-notifications-db-data` | notifications-db | Persistent Postgres data |
| `finledgerx-reconciliation-db-data` | reconciliation-db | Persistent Postgres data |
| `finledgerx-zookeeper-data` | zookeeper | ZK snapshots |
| `finledgerx-zookeeper-log` | zookeeper | ZK transaction log |
| `finledgerx-kafka-data` | kafka | Kafka log segments |
| `finledgerx-redis-data` | redis | AOF persistence (everysec) |
| `finledgerx-prometheus-data` | prometheus | 15-day TSDB retention |
| `finledgerx-grafana-data` | grafana | Dashboard state |

All volumes have explicit `name:` so they survive `docker compose down` (data is NOT lost).  
To wipe everything: `docker compose down -v`

---

## 5. Microservice Dockerfile — Multi-Stage Build

`Dockerfile.service` (copy into each service directory as `Dockerfile`):

| Stage | Base Image | What happens |
|---|---|---|
| `build` | eclipse-temurin:21-jdk-alpine | Maven build, skip tests |
| `extract` | eclipse-temurin:21-jdk-alpine | `java -Djarmode=layertools extract` — creates 4 cache-friendly layers |
| `runtime` | eclipse-temurin:21-jre-alpine | Non-root user `finledgerx`, only JRE, 4 layers copied in cache-optimal order |

**Why layered jar?**  
On each rebuild only the `application/` layer changes. `dependencies/` (hundreds of MBs) stays cached → fast CI/CD pushes.

**JVM flags baked in:**
```
-XX:+UseContainerSupport      # reads cgroup memory limit, not host RAM
-XX:MaxRAMPercentage=75.0     # use 75% of container limit for heap
-XX:+UseG1GC                  # low-pause GC
-Djava.security.egd=file:/dev/./urandom  # fast SecureRandom on Linux
```

---

## 6. Environment Variables Reference

All variables live in `.env` (git-ignored). `docker-compose.yml` reads them with `${VAR:-default}`.

| Variable | Used by | Example |
|---|---|---|
| `PAYMENTS_DB_PASSWORD` | payments-db, payment-service | `change_me_payments_pass` |
| `LEDGER_DB_PASSWORD` | ledger-db, ledger-service | `change_me_ledger_pass` |
| `NOTIFICATIONS_DB_PASSWORD` | notifications-db, notification-service | `change_me_notif_pass` |
| `RECONCILIATION_DB_PASSWORD` | reconciliation-db, reconciliation-service | `change_me_recon_pass` |
| `REDIS_PASSWORD` | redis, all services using Redis | `change_me_redis_pass` |
| `GRAFANA_ADMIN_PASSWORD` | grafana | `change_me_grafana_pass` |
| `APP_VERSION` | all service images | `latest` / `1.0.0` |

---

## 7. Health Check Design

Every container has a `healthcheck` configured. Services use:

```yaml
test: ["CMD-SHELL", "curl -sf http://localhost:808X/actuator/health || exit 1"]
interval: 15s
timeout:  5s
retries:  5
start_period: 45s   # JVM warmup time before first check
```

`start_period` prevents false-positive failures during JVM startup.  
`depends_on: condition: service_healthy` means downstream services **wait** until upstream passes health check — not just "container started".

---

## 8. Prometheus Scrape Targets

All 4 services expose `/actuator/prometheus` on management port 9090 (not the API port).  
Prometheus scrapes every 15s. Custom metrics to register:

| Metric Name | Type | Trigger |
|---|---|---|
| `ledger_balance_mismatch_total` | Counter | `LedgerImbalanceException` thrown |
| `payment_status_transitions_total{to_status}` | Counter | Each state machine transition |
| `reconciliation_mismatches_total` | Counter | Each mismatch row inserted |
| `kafka_consumer_group_lag{topic,group}` | Gauge | Via Kafka JMX exporter (optional) |

---

## 9. Alert Severity Levels

Defined in `infra/prometheus/alerts.yml`:

| Tier | Alert | Condition | Tolerance |
|---|---|---|---|
| P0 | `LedgerImbalanceDetected` | `ledger_balance_mismatch_total` > 0 | **Zero tolerance** — fires immediately |
| P1 | `PaymentDlqMessagesGrowing` | DLQ lag > 0 for 2 min | Page on-call |
| P1 | `PaymentServiceDown` | `up == 0` for 1 min | Page on-call |
| P2 | `SlowPaymentP99` | p99 > 2s for 5 min | Notify team |
| P3 | `ReconciliationMismatchFound` | Any mismatch in 1h | Ticket |

---

## 10. Quick-Start Commands

```bash
# 1. Copy and fill in secrets
cp .env.example .env
# edit .env — change all "change_me_*" values

# 2. First-time start (builds images + starts all containers)
docker compose up -d --build

# 3. Watch startup progress
docker compose ps
docker compose logs -f kafka-init   # confirm topics created

# 4. Verify services are healthy
docker compose ps --format "table {{.Name}}\t{{.Status}}"

# 5. Access UIs
#   Payment API:    http://localhost:8081/actuator/health
#   Kafka UI (dev): http://localhost:8090
#   pgAdmin (dev):  http://localhost:5050
#   Prometheus:     http://localhost:9090
#   Grafana:        http://localhost:3000  (admin / see .env)
#   MailHog:        http://localhost:8025
#   React UI:       http://localhost:3001

# 6. Stop (keeps volumes)
docker compose down

# 7. Full wipe (deletes all volumes + data)
docker compose down -v
```

---

## 11. JPMorgan Interview Talking Points

**Q: Why not just use a single docker-compose with all services sharing the same DB?**  
Each service owns its own database (Database-per-Service pattern). This enforces bounded contexts — the Ledger Service cannot write directly to the Payments DB. Cross-service data sharing happens only through Kafka events. This is a prerequisite for extracting services to separate hosts or Kubernetes namespaces later.

**Q: Why does kafka-init use `restart: "no"` and `condition: service_completed_successfully`?**  
Topic creation is idempotent (`--if-not-exists`) and should run exactly once at startup. `restart: "no"` prevents Docker from restarting the one-shot container after it exits 0. Downstream services declare `condition: service_completed_successfully` — they won't start until topic creation confirms success.

**Q: What's the purpose of the `start_period` in health checks?**  
Spring Boot with JPA + Flyway + Kafka consumer initialization can take 30–40 seconds on a cold JVM. Without `start_period`, Docker immediately starts the 5-retry countdown and marks the service unhealthy before it has even finished starting. `start_period: 45s` means Docker won't count failures until 45 seconds after container start.
