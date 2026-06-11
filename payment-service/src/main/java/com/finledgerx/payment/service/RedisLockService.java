package com.finledgerx.payment.service;

import com.finledgerx.payment.exception.DistributedLockException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Wrapper around Redisson's RLock for distributed locking.
 *
 * Why Redisson over raw Redis SET NX?
 * Redisson's RLock has a built-in watchdog that automatically extends
 * the lock TTL while the thread is still holding it. This prevents stale
 * lock expiry for long-running operations without needing to manually
 * calculate a safe TTL upfront.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class RedisLockService {

    private final RedissonClient redissonClient;

    /**
     * Executes {@code task} while holding a distributed lock on {@code lockKey}.
     *
     * @param lockKey    Redis key for the lock
     * @param ttlMs      Maximum hold time in milliseconds (watchdog extends this automatically)
     * @param waitMs     Maximum time to wait for lock acquisition
     * @param task       Business logic to execute under the lock
     * @param <T>        Return type of the task
     * @return Result of the task
     * @throws DistributedLockException if the lock could not be acquired within waitMs
     */
    public <T> T executeWithLock(String lockKey, long ttlMs, long waitMs, Supplier<T> task) {
        RLock lock = redissonClient.getLock(lockKey);
        boolean acquired;

        try {
            acquired = lock.tryLock(waitMs, ttlMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DistributedLockException(lockKey);
        }

        if (!acquired) {
            log.warn("Could not acquire lock for key={} within {}ms", lockKey, waitMs);
            throw new DistributedLockException(lockKey);
        }

        try {
            log.debug("Lock acquired: key={}", lockKey);
            return task.get();
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.debug("Lock released: key={}", lockKey);
            }
        }
    }

    /**
     * Void variant for operations that don't return a value.
     */
    public void executeWithLock(String lockKey, long ttlMs, long waitMs, Runnable task) {
        executeWithLock(lockKey, ttlMs, waitMs, () -> {
            task.run();
            return null;
        });
    }
}
