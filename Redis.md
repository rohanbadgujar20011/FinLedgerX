# FinLedgerX — Redis Architecture

> **Principal Architect View** | Redis 7.x · Lettuce Client (Spring Boot default) · Spring Cache Abstraction
>
> Redis in FinLedgerX is not optional infrastructure — it is on the critical path for idempotency, double-spend prevention, and API rate limiting. This document is the single source of truth for all Redis usage patterns. Reference when writing service code, tuning TTLs, or debugging cache behaviour.

---

## Table of Contents

1. What Should Be Cached
2. Cache Keys — Complete Reference
3. TTL Strategy
4. Cache Invalidation
5. Distributed Locking
6. Rate Limiting
7. Redis Configuration
8. Monitoring & Alerts

---

## 1. What Should Be Cached

The decision framework: cache data that is **read frequently**, **expensive to recompute**, and **tolerable to serve slightly stale** — with explicit invalidation for the parts that cannot be stale.

| Data | Cache? | Reason |
|---|---|---|
| Idempotency key → response | ✅ Yes | Prevents duplicate payments on client retry. Must be 100% reliable. |
| Account balance | ✅ Yes (short TTL) | Read on every payment. DB `SUM()` is O(n) without materialized view. 5min TTL + event-driven invalidation. |
| Payment status | ✅ Yes | Clients poll `GET /payments/{id}` after submitting. Cache avoids DB hit on every poll. |
| JWT blacklist | ✅ Yes | O(1) logout/revoke check per request without hitting auth DB. |
| User session | ✅ Yes | Sliding TTL. Avoids DB lookup on every authenticated request. |
| Notification templates | ✅ Yes | Read on every notification dispatch. Templates rarely change. |
| Reconciliation rules | ✅ Yes | Read at start of every batch job. Rules rarely change. |
| Rate limit counters | ✅ Yes | Must be in-process shared state across service replicas. |
| Distributed locks | ✅ Yes | Cross-replica mutual exclusion. Only Redis can do this atomically. |
| Full payment record | ❌ No | Mutable state with business logic. Risk of serving stale data during state transitions. Read from DB. |
| Journal entries | ❌ No | Append-only, financial truth. Read from DB for audit. Do not cache. |
| User PII (email, phone) | ❌ No | GDPR compliance. Never cache PII in Redis unless encrypted. |

---

## 2. Cache Keys — Complete Reference

### Key Naming Convention

```
{namespace}:{service}:{entity}:{identifier}[:{qualifier}]
```

All keys are lowercase with colons as separators. No spaces. UUIDs in lowercase.

### Complete Key Inventory

```
# ── Payment Service ──────────────────────────────────────────────
idempotency:payment:{idempotencyKey}
  Type:    STRING
  Value:   JSON { paymentId, status, httpStatus, responseBody }
  TTL:     86400s (24h)
  Example: idempotency:payment:550e8400-e29b-41d4-a716-446655440000

velocity:payment:{accountId}
  Type:    STRING (INCR counter)
  Value:   Integer count of payments in current window
  TTL:     60s (1 minute window)
  Example: velocity:payment:acc-uuid-alice

payment:status:{paymentId}
  Type:    HASH
  Fields:  status, updatedAt
  TTL:     600s (10min)
  Example: payment:status:pay-uuid-001

lock:payment:{paymentId}
  Type:    STRING
  Value:   {ownerId} (UUID of lock holder — for safe release)
  TTL:     30000ms (30s)
  Example: lock:payment:pay-uuid-001

# ── Ledger Service ────────────────────────────────────────────────
balance:{accountId}
  Type:    HASH
  Fields:  balance, holdAmount, currency, cachedAt
  TTL:     300s (5min)
  Example: balance:acc-uuid-alice

lock:account:debit:{accountId}
  Type:    STRING
  Value:   {ownerId}
  TTL:     10000ms (10s)
  Example: lock:account:debit:acc-uuid-alice

# ── Notification Service ──────────────────────────────────────────
notif:template:{eventType}:{channel}:{locale}
  Type:    HASH
  Fields:  subject, bodyTemplate, isActive
  TTL:     3600s (1h)
  Example: notif:template:PAYMENT_SETTLED:EMAIL:en

# ── Reconciliation Service ────────────────────────────────────────
recon:rules:active
  Type:    LIST (LPUSH, ordered by priority)
  Value:   JSON array of ReconRule objects
  TTL:     1800s (30min)
  Example: recon:rules:active

lock:recon:job:{runDate}
  Type:    STRING
  Value:   {ownerId}
  TTL:     300000ms (5min)
  Example: lock:recon:job:2026-06-03

# ── API Gateway / All Services ────────────────────────────────────
ratelimit:api:{userId}:{windowStartMs}
  Type:    SORTED SET (member=requestId, score=timestampMs)
  TTL:     60s (window size)
  Example: ratelimit:api:user-uuid-alice:1748908800000

ratelimit:ip:{ipAddress}
  Type:    STRING (INCR counter)
  TTL:     3600s (1h)
  Example: ratelimit:ip:192.168.1.100

jwt:blacklist:{jti}
  Type:    STRING
  Value:   "revoked"
  TTL:     Remaining JWT lifetime (exp - now)
  Example: jwt:blacklist:jwt-id-uuid-abc

session:{sessionId}
  Type:    HASH
  Fields:  userId, roles, email, lastActive, createdAt
  TTL:     1800s (30min, sliding)
  Example: session:sess-uuid-xyz
```

---

## 3. TTL Strategy

TTL is not a constant — it's a business decision for each data type.

```java
// RedisKeyTTL.java — centralised TTL constants (never scatter magic numbers)
public final class RedisKeyTTL {

    private RedisKeyTTL() {}

    // Idempotency — must cover the full client retry window
    public static final Duration IDEMPOTENCY         = Duration.ofHours(24);

    // Balance — short enough to avoid stale reads on rapid payments
    // A user making 10 payments quickly will invalidate the cache anyway
    public static final Duration BALANCE             = Duration.ofMinutes(5);

    // Payment status — clients poll this; stale for 10min is acceptable
    public static final Duration PAYMENT_STATUS      = Duration.ofMinutes(10);

    // Locks — must be >= operation duration but short enough to auto-release on crash
    public static final Duration LOCK_PAYMENT        = Duration.ofSeconds(30);
    public static final Duration LOCK_ACCOUNT_DEBIT  = Duration.ofSeconds(10);
    public static final Duration LOCK_RECON_JOB      = Duration.ofMinutes(5);

    // Rate limiting — matches window size
    public static final Duration RATE_LIMIT_API      = Duration.ofMinutes(1);
    public static final Duration RATE_LIMIT_IP       = Duration.ofHours(1);
    public static final Duration VELOCITY_PAYMENT    = Duration.ofMinutes(1);

    // Session/auth
    public static final Duration SESSION             = Duration.ofMinutes(30);
    // JWT blacklist TTL is dynamic: remaining = Instant.now().until(jwtExpiry)

    // Config cache — rarely changes, long TTL acceptable
    public static final Duration NOTIF_TEMPLATE      = Duration.ofHours(1);
    public static final Duration RECON_RULES         = Duration.ofMinutes(30);
}
```

### TTL Decision Rules

**Rule 1 — Financial data gets short TTLs.**
Balance cache is 5 minutes. If Alice's balance is stale and she submits a payment, the worst case is a failed DB-level check. We never make an irreversible financial decision from a stale cache value alone. The DB is always the final guard.

**Rule 2 — Locks must TTL <= the operation they protect.**
If the payment processing lock is 30 seconds, the payment processing itself must complete in under 30 seconds. If it can't, the lock expires, another pod could pick up the same payment. Design operations to be idempotent so this is safe.

**Rule 3 — Idempotency TTL must cover the client retry window.**
If your API contract says "clients may retry for up to 24 hours", your idempotency keys must live for 24 hours. If they expire earlier, a retry after 25 hours creates a duplicate payment.

**Rule 4 — Config TTL can be long because invalidation is explicit.**
Notification templates have 1h TTL, but they are also explicitly deleted from Redis when the DB record is updated. The TTL is just the failsafe.

---

## 4. Cache Invalidation

### Strategy per Data Type

```java
// CacheInvalidationService.java
@Service
@Slf4j
public class CacheInvalidationService {

    private final RedisTemplate<String, Object> redisTemplate;

    // ── Balance invalidation (event-driven via Kafka) ──────────
    // Called by LedgerService Kafka consumer after posting journal entries
    public void invalidateBalance(UUID accountId) {
        String key = "balance:" + accountId;
        Boolean deleted = redisTemplate.delete(key);
        log.debug("Balance cache invalidated accountId={} deleted={}", accountId, deleted);
    }

    // Invalidate both sides of a payment atomically
    public void invalidatePaymentAccounts(UUID fromAccountId, UUID toAccountId) {
        redisTemplate.delete(List.of(
            "balance:" + fromAccountId,
            "balance:" + toAccountId
        ));
        log.info("Balance cache invalidated fromAccount={} toAccount={}",
            fromAccountId, toAccountId);
    }

    // ── Payment status invalidation ────────────────────────────
    public void invalidatePaymentStatus(UUID paymentId) {
        redisTemplate.delete("payment:status:" + paymentId);
    }

    // ── Template invalidation (called from NotificationTemplateService.save) ──
    public void invalidateNotificationTemplate(String eventType, String channel) {
        // Use scan pattern — not KEYS in production (KEYS blocks Redis)
        ScanOptions options = ScanOptions.scanOptions()
            .match("notif:template:" + eventType + ":" + channel + ":*")
            .count(100).build();

        try (Cursor<byte[]> cursor = redisTemplate.getConnectionFactory()
                .getConnection().scan(options)) {
            cursor.forEachRemaining(key ->
                redisTemplate.delete(new String(key)));
        }
        log.info("Notification template cache invalidated eventType={} channel={}",
            eventType, channel);
    }

    // ── Recon rules invalidation ───────────────────────────────
    public void invalidateReconRules() {
        redisTemplate.delete("recon:rules:active");
    }
}
```

### Event-Driven Invalidation via Kafka

```java
// CacheInvalidationKafkaConsumer.java (in Ledger Service)
@Component
@Slf4j
public class CacheInvalidationKafkaConsumer {

    private final CacheInvalidationService cacheInvalidationService;

    // Listen to payment.events to invalidate balances immediately after settlement
    @KafkaListener(
        topics = "payment.events",
        groupId = "ledger-cache-invalidation-cg",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentEvent(@Payload PaymentEvent event, Acknowledgment ack) {
        switch (event.getEventType()) {
            case "PAYMENT_COMPLETED" -> {
                cacheInvalidationService.invalidatePaymentAccounts(
                    event.getFromAccountId(), event.getToAccountId());
                cacheInvalidationService.invalidatePaymentStatus(event.getPaymentId());
            }
            case "PAYMENT_FAILED", "PAYMENT_REVERSED", "PAYMENT_CANCELLED" -> {
                cacheInvalidationService.invalidateBalance(event.getFromAccountId());
                cacheInvalidationService.invalidatePaymentStatus(event.getPaymentId());
            }
        }
        ack.acknowledge();
    }
}
```

### Spring @CacheEvict for Config Data

```java
// NotificationTemplateService.java
@Service
public class NotificationTemplateService {

    private final NotificationTemplateRepository templateRepo;

    @Cacheable(
        value = "notif:template",
        key = "#eventType + ':' + #channel + ':' + #locale"
    )
    public NotificationTemplate getTemplate(String eventType,
                                            String channel,
                                            String locale) {
        return templateRepo.findByEventTypeAndChannelAndLocale(eventType, channel, locale)
            .orElseThrow(() -> new TemplateNotFoundException(eventType, channel));
    }

    @CacheEvict(
        value = "notif:template",
        key = "#template.eventType + ':' + #template.channel + ':' + #template.locale"
    )
    @Transactional
    public NotificationTemplate saveTemplate(NotificationTemplate template) {
        return templateRepo.save(template);
    }

    @CacheEvict(value = "notif:template", allEntries = true)
    public void invalidateAllTemplates() {
        // Called by admin endpoint or deploy hook
        log.info("All notification template caches invalidated");
    }
}
```

### Cache-Aside Pattern (Balance)

```java
// BalanceCacheService.java
@Service
@Slf4j
public class BalanceCacheService {

    private final RedisTemplate<String, BalanceCacheEntry> redisTemplate;
    private final AccountBalanceRepository balanceRepository;

    public BalanceCacheEntry getBalance(UUID accountId) {
        String key = "balance:" + accountId;

        // 1. Try cache
        BalanceCacheEntry cached = (BalanceCacheEntry)
            redisTemplate.opsForValue().get(key);

        if (cached != null) {
            log.debug("Balance cache HIT accountId={}", accountId);
            return cached;
        }

        // 2. Cache MISS — load from DB
        log.debug("Balance cache MISS accountId={} — loading from DB", accountId);
        AccountBalance dbBalance = balanceRepository.findById(accountId)
            .orElseThrow(() -> new AccountNotFoundException(accountId));

        BalanceCacheEntry entry = BalanceCacheEntry.from(dbBalance);

        // 3. Populate cache — SET with TTL
        redisTemplate.opsForValue().set(key, entry, RedisKeyTTL.BALANCE);

        return entry;
    }

    // IMPORTANT: Never make a payment decision purely from cached balance.
    // Cache is for display/estimation. Actual debit uses DB with pessimistic lock.
    public boolean hasApproximateSufficientBalance(UUID accountId, BigDecimal amount) {
        BalanceCacheEntry cached = getBalance(accountId);
        // Add 10% buffer — if stale, DB will catch it
        return cached.getAvailableBalance()
                     .compareTo(amount.multiply(new BigDecimal("0.9"))) >= 0;
    }
}
```

---

## 5. Distributed Locking

### Why Distributed Locks in a Fintech System?

Without locks, two pods running simultaneously can both read the same account balance from the DB (before either debit is committed), both pass the "sufficient balance" check, and both post debits — resulting in an overdraft. Redis distributed locks provide cross-pod mutual exclusion.

### Implementation — Redisson

```xml
<!-- pom.xml -->
<dependency>
    <groupId>org.redisson</groupId>
    <artifactId>redisson-spring-boot-starter</artifactId>
    <version>3.27.0</version>
</dependency>
```

```java
// DistributedLockService.java
@Service
@Slf4j
public class DistributedLockService {

    private final RedissonClient redissonClient;

    /**
     * Executes supplier within a distributed lock.
     * Lock is automatically released after supplier completes OR on timeout.
     *
     * @param lockKey    Redis key for the lock
     * @param ttlMs      Lock TTL in milliseconds (auto-release on crash)
     * @param waitMs     Max time to wait to acquire lock (0 = fail-fast)
     * @param supplier   Business logic to execute under lock
     */
    public <T> T executeWithLock(String lockKey,
                                  long ttlMs,
                                  long waitMs,
                                  Supplier<T> supplier) {
        RLock lock = redissonClient.getLock(lockKey);
        boolean acquired = false;

        try {
            acquired = lock.tryLock(waitMs, ttlMs, TimeUnit.MILLISECONDS);
            if (!acquired) {
                throw new LockAcquisitionException(
                    "Could not acquire lock: " + lockKey +
                    ". Another operation is in progress.");
            }
            log.debug("Lock acquired key={} ttlMs={}", lockKey, ttlMs);
            return supplier.get();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LockAcquisitionException("Lock acquisition interrupted: " + lockKey, e);
        } finally {
            if (acquired && lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.debug("Lock released key={}", lockKey);
            }
        }
    }

    public void executeWithLock(String lockKey, long ttlMs, long waitMs,
                                 Runnable action) {
        executeWithLock(lockKey, ttlMs, waitMs, () -> {
            action.run();
            return null;
        });
    }
}
```

### Lock Usage — Payment Processing

```java
// PaymentServiceImpl.java — critical section
@Service
public class PaymentServiceImpl implements PaymentService {

    private final DistributedLockService lockService;
    private final LedgerServiceClient ledgerClient;

    @Override
    public PaymentResponse processPayment(UUID paymentId) {
        String lockKey = "lock:payment:" + paymentId;

        return lockService.executeWithLock(
            lockKey,
            30_000L,   // TTL: 30 seconds
            0L,        // waitMs: fail immediately if locked (idempotency handles retries)
            () -> {
                Payment payment = paymentRepository.findByIdWithLock(paymentId)
                    .orElseThrow(() -> new PaymentNotFoundException(paymentId));

                if (payment.getStatus() != PaymentStatus.AUTHORIZED) {
                    // Already processed by another pod — idempotent return
                    return toResponse(payment);
                }

                // Safe to process — we hold the lock
                payment.transitionTo(PaymentStatus.PROCESSING);
                paymentRepository.save(payment);

                // Call external rail...
                return toResponse(payment);
            }
        );
    }
}
```

### Lock Usage — Account Debit (Double-Spend Prevention)

```java
// LedgerServiceImpl.java
@Service
public class LedgerServiceImpl implements LedgerService {

    private final DistributedLockService lockService;

    @Override
    public void postDebitEntry(UUID accountId, BigDecimal amount,
                                UUID transactionId) {
        String lockKey = "lock:account:debit:" + accountId;

        lockService.executeWithLock(
            lockKey,
            10_000L,   // TTL: 10 seconds (debit takes <1s)
            5_000L,    // waitMs: wait up to 5s to acquire
            () -> {
                // Under lock: re-read balance from DB (not cache)
                AccountBalance balance = balanceRepository
                    .findByAccountIdForUpdate(accountId)      // pessimistic DB lock too
                    .orElseThrow(() -> new AccountNotFoundException(accountId));

                if (balance.getBalance().compareTo(amount) < 0) {
                    throw new InsufficientFundsException(accountId, amount,
                        balance.getBalance());
                }

                // Post the debit journal entry
                JournalEntry entry = buildDebitEntry(accountId, amount, transactionId);
                journalEntryRepository.save(entry);
                // Balance updated by DB trigger
            }
        );
    }
}
```

### Lock Usage — Reconciliation Job

```java
// ReconciliationServiceImpl.java
@Scheduled(cron = "0 0 2 * * *")
public void runDailyReconciliation() {
    LocalDate yesterday = LocalDate.now().minusDays(1);
    String lockKey = "lock:recon:job:" + yesterday;

    try {
        lockService.executeWithLock(
            lockKey,
            300_000L,   // TTL: 5 minutes
            0L,         // fail-fast: only one scheduler should win
            () -> {
                log.info("Recon job started for date={}", yesterday);
                runReconciliation(yesterday);
                return null;
            }
        );
    } catch (LockAcquisitionException e) {
        // Another pod (or manual trigger) is already running this job — skip
        log.info("Recon job already running for date={}. Skipping.", yesterday);
    }
}
```

### Manual Lock Implementation (without Redisson)

```java
// For simple cases — pure Lettuce / RedisTemplate
// Uses SET key value NX PX ttl  (atomic: set only if not exists)
public boolean acquireLock(String lockKey, String ownerId, long ttlMs) {
    Boolean result = redisTemplate.opsForValue()
        .setIfAbsent(lockKey, ownerId, Duration.ofMillis(ttlMs));
    return Boolean.TRUE.equals(result);
}

// Release lock — Lua script ensures atomic check-and-delete
// Critical: only the lock owner can release. Prevents one pod releasing another's lock.
private static final String RELEASE_SCRIPT =
    "if redis.call('get', KEYS[1]) == ARGV[1] then " +
    "    return redis.call('del', KEYS[1]) " +
    "else " +
    "    return 0 " +
    "end";

public boolean releaseLock(String lockKey, String ownerId) {
    DefaultRedisScript<Long> script = new DefaultRedisScript<>(RELEASE_SCRIPT, Long.class);
    Long result = redisTemplate.execute(script, List.of(lockKey), ownerId);
    return Long.valueOf(1L).equals(result);
}
```

---

## 6. Rate Limiting

### Two Algorithms Used

**Fixed Window Counter** — Used for velocity checks (simple, minimal memory).
**Sliding Window Log** — Used for API rate limiting (accurate, no burst at window boundary).

### Sliding Window Rate Limiter

```java
// SlidingWindowRateLimiter.java
@Component
@Slf4j
public class SlidingWindowRateLimiter {

    private final RedisTemplate<String, String> redisTemplate;

    private static final String SLIDING_WINDOW_SCRIPT =
        "local key = KEYS[1] " +
        "local now = tonumber(ARGV[1]) " +
        "local windowMs = tonumber(ARGV[2]) " +
        "local limit = tonumber(ARGV[3]) " +
        "local requestId = ARGV[4] " +
        // Remove entries outside the window
        "redis.call('ZREMRANGEBYSCORE', key, 0, now - windowMs) " +
        // Count remaining in window
        "local count = redis.call('ZCARD', key) " +
        "if count < limit then " +
        // Add this request
        "    redis.call('ZADD', key, now, requestId) " +
        "    redis.call('PEXPIRE', key, windowMs) " +
        "    return {1, count + 1} " +      -- allowed, new count
        "else " +
        "    return {0, count} " +           -- denied, current count
        "end";

    /**
     * Check and increment sliding window rate limit.
     *
     * @return RateLimitResult with allowed=true/false and current count
     */
    public RateLimitResult checkLimit(String identifier,
                                       String limitType,
                                       int limit,
                                       Duration window) {
        String key = "ratelimit:" + limitType + ":" + identifier;
        long nowMs = Instant.now().toEpochMilli();
        long windowMs = window.toMillis();
        String requestId = UUID.randomUUID().toString();

        DefaultRedisScript<List> script =
            new DefaultRedisScript<>(SLIDING_WINDOW_SCRIPT, List.class);

        List<Long> result = redisTemplate.execute(
            script,
            List.of(key),
            String.valueOf(nowMs),
            String.valueOf(windowMs),
            String.valueOf(limit),
            requestId
        );

        boolean allowed = result.get(0) == 1L;
        long currentCount = result.get(1);

        if (!allowed) {
            log.warn("Rate limit exceeded key={} count={} limit={}", key, currentCount, limit);
        }

        return new RateLimitResult(allowed, currentCount, limit,
            allowed ? 0 : (windowMs - (nowMs % windowMs)));  // retryAfterMs
    }
}

// RateLimitResult.java
public record RateLimitResult(
    boolean allowed,
    long currentCount,
    int limit,
    long retryAfterMs
) {}
```

### API Gateway Rate Limit Filter

```java
// RateLimitFilter.java (Spring Gateway / OncePerRequestFilter)
@Component
@Slf4j
public class RateLimitFilter extends OncePerRequestFilter {

    private final SlidingWindowRateLimiter rateLimiter;
    private final VelocityRateLimiter velocityLimiter;

    // Limits configuration
    private static final int API_LIMIT_PER_MINUTE   = 100;
    private static final int IP_LIMIT_PER_HOUR      = 1000;
    private static final int PAYMENT_VELOCITY_LIMIT = 10;   // per account, per minute

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                     HttpServletResponse response,
                                     FilterChain chain)
            throws ServletException, IOException {

        String userId = extractUserId(request);      // from JWT
        String ipAddress = getClientIp(request);
        String path = request.getRequestURI();

        // 1. IP rate limit (brute force protection — no JWT required)
        RateLimitResult ipResult = rateLimiter.checkLimit(
            ipAddress, "ip", IP_LIMIT_PER_HOUR, Duration.ofHours(1));
        if (!ipResult.allowed()) {
            sendRateLimitResponse(response, ipResult, "IP rate limit exceeded");
            return;
        }

        // 2. User API rate limit (per-user general limit)
        if (userId != null) {
            RateLimitResult apiResult = rateLimiter.checkLimit(
                userId, "api", API_LIMIT_PER_MINUTE, Duration.ofMinutes(1));
            if (!apiResult.allowed()) {
                sendRateLimitResponse(response, apiResult, "API rate limit exceeded");
                return;
            }
        }

        // 3. Payment velocity check (payment-specific endpoint)
        if (path.equals("/api/v1/payments") && "POST".equals(request.getMethod())) {
            String accountId = extractAccountIdFromBody(request);
            if (accountId != null) {
                RateLimitResult velocityResult = rateLimiter.checkLimit(
                    accountId, "velocity:payment",
                    PAYMENT_VELOCITY_LIMIT, Duration.ofMinutes(1));
                if (!velocityResult.allowed()) {
                    sendRateLimitResponse(response, velocityResult,
                        "Payment velocity limit exceeded. Max 10 payments per minute.");
                    return;
                }
            }
        }

        chain.doFilter(request, response);
    }

    private void sendRateLimitResponse(HttpServletResponse response,
                                        RateLimitResult result,
                                        String message) throws IOException {
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setHeader("Retry-After",
            String.valueOf(result.retryAfterMs() / 1000));
        response.setHeader("X-RateLimit-Limit",
            String.valueOf(result.limit()));
        response.setHeader("X-RateLimit-Remaining", "0");
        response.setContentType("application/json");
        response.getWriter().write(
            "{\"errorCode\":\"RATE_LIMIT_EXCEEDED\",\"message\":\"" + message + "\"}"
        );
    }
}
```

### Fixed Window Velocity Check (Simple Counter)

```java
// VelocityCheckService.java
@Service
public class VelocityCheckService {

    private final RedisTemplate<String, String> redisTemplate;

    /**
     * Increment velocity counter and check limit.
     * Uses INCR + EXPIRE pipeline — atomic and efficient.
     */
    public boolean checkAndIncrementVelocity(String accountId, int limit) {
        String key = "velocity:payment:" + accountId;

        List<Object> results = redisTemplate.executePipelined((RedisCallback<Object>) conn -> {
            byte[] rawKey = key.getBytes(StandardCharsets.UTF_8);
            conn.stringCommands().incr(rawKey);
            conn.keyCommands().expire(rawKey, 60);   // 60s window
            return null;
        });

        Long count = (Long) results.get(0);
        return count != null && count <= limit;
    }
}
```

---

## 7. Redis Configuration

```java
// RedisConfig.java
@Configuration
@EnableCaching
public class RedisConfig {

    @Value("${spring.redis.host:localhost}")
    private String host;

    @Value("${spring.redis.port:6379}")
    private int port;

    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration config =
            new RedisStandaloneConfiguration(host, port);

        // Connection pool — critical for high-throughput financial services
        LettucePoolingClientConfiguration poolConfig =
            LettucePoolingClientConfiguration.builder()
                .poolConfig(buildPoolConfig())
                .commandTimeout(Duration.ofMillis(500))  // fail fast, not hang
                .build();

        return new LettuceConnectionFactory(config, poolConfig);
    }

    private GenericObjectPoolConfig<StatefulRedisConnection<String, String>>
    buildPoolConfig() {
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig =
            new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(50);          // max connections
        poolConfig.setMaxIdle(20);           // idle connections kept alive
        poolConfig.setMinIdle(5);            // always-open connections
        poolConfig.setMaxWait(Duration.ofMillis(200));  // queue wait limit
        return poolConfig;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(
            LettuceConnectionFactory factory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);

        // Key: plain string (human-readable, debuggable with redis-cli)
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());

        // Value: JSON (human-readable, schema-flexible)
        Jackson2JsonRedisSerializer<Object> jsonSerializer =
            new Jackson2JsonRedisSerializer<>(Object.class);
        template.setValueSerializer(jsonSerializer);
        template.setHashValueSerializer(jsonSerializer);

        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public RedisCacheManager cacheManager(LettuceConnectionFactory factory) {
        RedisCacheConfiguration defaultConfig = RedisCacheConfiguration
            .defaultCacheConfig()
            .entryTtl(Duration.ofMinutes(5))    // safe default
            .disableCachingNullValues()           // never cache null — masks DB errors
            .serializeKeysWith(RedisSerializationContext.SerializationPair
                .fromSerializer(new StringRedisSerializer()))
            .serializeValuesWith(RedisSerializationContext.SerializationPair
                .fromSerializer(new GenericJackson2JsonRedisSerializer()));

        // Per-cache TTL overrides
        Map<String, RedisCacheConfiguration> cacheConfigs = Map.of(
            "notif:template",   defaultConfig.entryTtl(Duration.ofHours(1)),
            "balance",          defaultConfig.entryTtl(Duration.ofMinutes(5)),
            "payment:status",   defaultConfig.entryTtl(Duration.ofMinutes(10)),
            "recon:rules",      defaultConfig.entryTtl(Duration.ofMinutes(30))
        );

        return RedisCacheManager.builder(factory)
            .cacheDefaults(defaultConfig)
            .withInitialCacheConfigurations(cacheConfigs)
            .build();
    }
}
```

```yaml
# application.yml — Redis section
spring:
  redis:
    host: ${REDIS_HOST:localhost}
    port: ${REDIS_PORT:6379}
    password: ${REDIS_PASSWORD:}
    timeout: 500ms
    lettuce:
      pool:
        max-active: 50
        max-idle: 20
        min-idle: 5
        max-wait: 200ms

# Redis server config (redis.conf)
# maxmemory 512mb
# maxmemory-policy allkeys-lru
# appendonly yes
# appendfsync everysec      <- balance between durability and performance
# hz 10                     <- background task frequency
# save 900 1                <- RDB snapshot: save if 1 key changed in 15 min
```

---

## 8. Monitoring & Alerts

### Micrometer Metrics (auto-instrumented by Spring Cache)

```java
// RedisMetricsConfig.java
@Configuration
public class RedisMetricsConfig {

    @Bean
    public MicrometerCommandLatencyRecorder micrometerCommandLatencyRecorder(
            MeterRegistry meterRegistry) {
        // Automatically records Redis command latency in Prometheus
        return new MicrometerCommandLatencyRecorder(meterRegistry,
            MicrometerOptions.builder()
                .histogram(true)
                .targetPercentiles(0.5, 0.95, 0.99)
                .build());
    }
}
```

### Key Metrics to Watch

| Metric | Warning | Critical | Action |
|---|---|---|---|
| `redis.memory.used` | > 70% max | > 85% max | Scale Redis, review TTLs |
| `redis.commands.latency.p99` | > 10ms | > 50ms | Check pool exhaustion, slow commands |
| `cache.gets[result=miss]` rate | > 50% | > 80% | TTLs too short, or cache eviction |
| `redis.connected_clients` | > 40 | > 48 (of 50 pool) | Increase pool or reduce hold time |
| Lock acquisition failures | > 5/min | > 20/min | Contention — investigate processing bottleneck |
| `velocity:payment` rejections | > 10/min | > 50/min | Possible fraud or DDoS |

### Grafana Alert Rules

```yaml
# Alert: Redis memory > 80%
- alert: RedisHighMemory
  expr: redis_memory_used_bytes / redis_memory_max_bytes > 0.80
  severity: P1
  message: "Redis memory at {{ $value | humanizePercentage }}. Risk of LRU eviction of locks/idempotency keys."

# Alert: High cache miss rate
- alert: HighCacheMissRate
  expr: rate(cache_gets_total{result="miss"}[5m]) /
        rate(cache_gets_total[5m]) > 0.6
  severity: P2
  message: "Cache miss rate {{ $value | humanizePercentage }} — possible TTL misconfiguration."

# Alert: Redis connection pool near exhaustion
- alert: RedisPoolNearExhaustion
  expr: redis_connected_clients > 45
  severity: P1
  message: "Redis connection pool near max ({{ $value }}/50). Risk of connection timeouts."
```

---

## Design Decisions Summary

| Decision | Choice | Reason |
|---|---|---|
| Client library | Lettuce (async, non-blocking) | Default in Spring Boot. Handles Redis cluster/sentinel. Better than Jedis for reactive workloads. |
| Lock library | Redisson | Battle-tested distributed lock with watchdog (auto-renew TTL for long operations), fair queuing, and safe Lua script release. |
| Serialization | Jackson JSON | Human-readable. Debuggable with `redis-cli`. Avoids Java serialization fragility. |
| Balance TTL | 5 minutes | Short enough to stay fresh for rapid payments. Long enough to absorb burst reads. DB is always the final guard. |
| Rate limit algorithm | Sliding window (ZSET) | No burst at window boundary (unlike fixed window). Slightly more memory per key, but fair for users. |
| `disableCachingNullValues` | Enabled | A null from cache is indistinguishable from a cache miss. Caching nulls masks DB errors and hides "account not found" bugs. |
| `allkeys-lru` eviction | Enabled | When memory is full, LRU-evicts cold keys. Hot keys (active idempotency, locks) survive. Alternative `noeviction` would crash clients — worse. |
| Lock TTL > operation time | Required | Lock TTL is the safety net for crashes. If your operation takes 5s, set lock TTL to 30s. The extra time is insurance, not waste. |

---

*Last updated: 2026-06-03 | Companion to HLD.md, LLD.md, SCHEMA.md, KAFKA.md | FinLedgerX Architecture Team*
