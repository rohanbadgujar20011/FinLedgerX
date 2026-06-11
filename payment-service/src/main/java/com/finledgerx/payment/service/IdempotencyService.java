package com.finledgerx.payment.service;

import com.finledgerx.payment.constants.RedisKeyConstants;
import com.finledgerx.payment.domain.entity.IdempotencyKey;
import com.finledgerx.payment.exception.IdempotencyConflictException;
import com.finledgerx.payment.repository.IdempotencyKeyRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Two-layer idempotency enforcement:
 *
 * Layer 1 — Redis (fast path):
 *   SET NX with 24h TTL. Returns immediately if key already exists.
 *   The value is either "IN_FLIGHT" (request in progress) or
 *   the serialized PaymentResponse (request completed).
 *
 * Layer 2 — PostgreSQL (durable fallback):
 *   Persists idempotency_keys row. Survives Redis flush / cache restart.
 *   Checked only on Redis miss (cold start, Redis cleared, TTL expired).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private final RedisTemplate<String, String>  redisTemplate;
    private final IdempotencyKeyRepository       idempotencyKeyRepository;

    @Value("${finledgerx.idempotency.ttl-hours:24}")
    private int ttlHours;

    /**
     * Attempt to reserve the idempotency key.
     *
     * - If the key is NOT present → mark it IN_FLIGHT and return empty (caller may proceed).
     * - If the key IS present and value == IN_FLIGHT → another thread is processing → throw 409.
     * - If the key IS present with a JSON response → return the cached response (replay).
     *
     * @return Optional<String> — empty if caller should proceed, or contains the cached response JSON.
     */
    public Optional<String> checkAndReserve(String idempotencyKey) {
        String redisKey = RedisKeyConstants.idempotencyKey(idempotencyKey);
        Duration ttl    = Duration.ofHours(ttlHours);

        // Atomic SET NX — only succeeds if key does not exist in Redis
        Boolean isNew = redisTemplate.opsForValue()
                .setIfAbsent(redisKey, RedisKeyConstants.IN_FLIGHT_MARKER, ttl);

        if (Boolean.TRUE.equals(isNew)) {
            // Redis key was absent. Could be a genuine new request OR a Redis cache miss
            // (restart, eviction, TTL expiry). Always check DB before proceeding to avoid
            // duplicate inserts on idempotency_keys when Redis loses its state.
            Optional<String> dbResult = checkDatabaseFallback(idempotencyKey);
            if (dbResult.isPresent()) {
                // Already completed — restore the Redis cache so next call hits fast path
                redisTemplate.opsForValue().set(redisKey, dbResult.get(), ttl);
                log.debug("Idempotency DB fallback hit (Redis was cold): {}", idempotencyKey);
                return dbResult;
            }
            // Truly new request — proceed
            log.debug("Idempotency key reserved: {}", idempotencyKey);
            return Optional.empty();
        }

        // Key already exists in Redis — inspect the value
        String existing = redisTemplate.opsForValue().get(redisKey);

        if (RedisKeyConstants.IN_FLIGHT_MARKER.equals(existing)) {
            // Still processing — concurrent duplicate request
            log.warn("Duplicate in-flight request for idempotency key: {}", idempotencyKey);
            throw new IdempotencyConflictException(idempotencyKey);
        }

        if (existing != null) {
            // Already completed — return cached response for replay
            log.debug("Idempotency cache hit: {}", idempotencyKey);
            return Optional.of(existing);
        }

        // Redis key exists but value is null (race) — fall back to DB
        return checkDatabaseFallback(idempotencyKey);
    }

    /**
     * Persist the idempotency key to PostgreSQL for durability.
     * Called INSIDE the same transaction as the payment creation.
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public IdempotencyKey persistInFlight(String idempotencyKey) {
        IdempotencyKey record = IdempotencyKey.builder()
                .idempotencyKey(idempotencyKey)
                .status(IdempotencyKey.IdempotencyStatus.IN_FLIGHT)
                .expiresAt(Instant.now().plus(Duration.ofHours(ttlHours)))
                .build();
        return idempotencyKeyRepository.save(record);
    }

    /**
     * Mark the key as completed after successful payment creation.
     * Also updates Redis value from IN_FLIGHT → serialized response.
     */
    public void complete(String idempotencyKey, UUID paymentId, String responseJson) {
        // Update Redis
        String redisKey = RedisKeyConstants.idempotencyKey(idempotencyKey);
        redisTemplate.opsForValue().set(redisKey, responseJson, Duration.ofHours(ttlHours));

        // Update DB
        idempotencyKeyRepository.findByIdempotencyKey(idempotencyKey)
                .ifPresent(record -> {
                    record.complete(paymentId, responseJson);
                    idempotencyKeyRepository.save(record);
                });
    }

    /**
     * Mark the key as failed so it can be retried.
     * Deletes the Redis key so the next request is treated as a new attempt.
     */
    public void fail(String idempotencyKey) {
        // Remove from Redis — allows a clean retry
        redisTemplate.delete(RedisKeyConstants.idempotencyKey(idempotencyKey));

        // Mark failed in DB
        idempotencyKeyRepository.findByIdempotencyKey(idempotencyKey)
                .ifPresent(record -> {
                    record.fail();
                    idempotencyKeyRepository.save(record);
                });
    }

    // ── Private helpers ──────────────────────────────────────────

    private Optional<String> checkDatabaseFallback(String idempotencyKey) {
        return idempotencyKeyRepository.findByIdempotencyKey(idempotencyKey)
                .flatMap(record -> {
                    if (record.isExpired()) {
                        return Optional.empty();
                    }
                    return switch (record.getStatus()) {
                        case IN_FLIGHT  -> throw new IdempotencyConflictException(idempotencyKey);
                        case COMPLETED  -> Optional.ofNullable(record.getResponseBody());
                        case FAILED     -> Optional.empty(); // allow retry
                    };
                });
    }
}
