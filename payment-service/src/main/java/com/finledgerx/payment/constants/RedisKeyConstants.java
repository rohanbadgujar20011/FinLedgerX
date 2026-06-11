package com.finledgerx.payment.constants;

import java.util.UUID;

/**
 * Central registry for all Redis key patterns used by the Payment Service.
 * Convention: {namespace}:{service}:{entity}:{identifier}
 */
public final class RedisKeyConstants {

    private RedisKeyConstants() {}

    // ── Idempotency ──────────────────────────────────────────────
    // Stores: "IN_FLIGHT" or JSON of PaymentResponse
    public static String idempotencyKey(String key) {
        return "idem:payment-svc:payment:" + key;
    }

    // ── Distributed locks ────────────────────────────────────────
    // Account debit lock — prevents double-spend across pods
    public static String accountDebitLock(UUID accountId) {
        return "lock:payment-svc:account-debit:" + accountId;
    }

    // Payment-level lock — prevents concurrent status updates
    public static String paymentLock(UUID paymentId) {
        return "lock:payment-svc:payment:" + paymentId;
    }

    // ── Rate limiting ────────────────────────────────────────────
    // Sliding window sorted set key
    public static String rateLimitKey(String clientId) {
        return "ratelimit:payment-svc:api:" + clientId;
    }

    // ── Cache keys ────────────────────────────────────────────────
    public static String paymentCacheKey(UUID paymentId) {
        return "cache:payment-svc:payment:" + paymentId;
    }

    // ── Sentinel values ───────────────────────────────────────────
    public static final String IN_FLIGHT_MARKER = "IN_FLIGHT";
}
