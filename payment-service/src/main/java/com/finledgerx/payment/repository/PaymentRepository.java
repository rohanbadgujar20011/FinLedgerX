package com.finledgerx.payment.repository;

import com.finledgerx.payment.domain.entity.Payment;
import com.finledgerx.payment.domain.enums.PaymentStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    /**
     * Pessimistic write lock — used when we need to update status
     * and absolutely cannot have concurrent updates on the same row.
     * Use sparingly; prefer OCC (@Version) for most updates.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM Payment p WHERE p.id = :id")
    Optional<Payment> findByIdForUpdate(@Param("id") UUID id);

    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    boolean existsByIdempotencyKey(String idempotencyKey);

    // ── Queries for list / reporting endpoints ───────────────────

    Page<Payment> findByFromAccountId(UUID fromAccountId, Pageable pageable);

    Page<Payment> findByFromAccountIdAndStatus(UUID fromAccountId, PaymentStatus status, Pageable pageable);

    Page<Payment> findByToAccountId(UUID toAccountId, Pageable pageable);

    Page<Payment> findByStatus(PaymentStatus status, Pageable pageable);

    @Query("""
        SELECT p FROM Payment p
        WHERE (:fromAccountId IS NULL OR p.fromAccountId = :fromAccountId)
          AND (:toAccountId   IS NULL OR p.toAccountId   = :toAccountId)
          AND (:status        IS NULL OR p.status        = :status)
          AND (:fromDate      IS NULL OR p.createdAt     >= :fromDate)
          AND (:toDate        IS NULL OR p.createdAt     <= :toDate)
        ORDER BY p.createdAt DESC
        """)
    Page<Payment> searchPayments(
            @Param("fromAccountId") UUID fromAccountId,
            @Param("toAccountId")   UUID toAccountId,
            @Param("status")        PaymentStatus status,
            @Param("fromDate")      Instant fromDate,
            @Param("toDate")        Instant toDate,
            Pageable pageable
    );

    // ── Reconciliation queries ───────────────────────────────────

    @Query("""
        SELECT p FROM Payment p
        WHERE p.status = 'SETTLED'
          AND p.settledAt >= :from
          AND p.settledAt < :to
        """)
    List<Payment> findSettledBetween(@Param("from") Instant from, @Param("to") Instant to);

    // ── Stuck-payment detection (for ops alerting) ───────────────

    @Query("""
        SELECT p FROM Payment p
        WHERE p.status NOT IN ('SETTLED', 'FAILED', 'REVERSED', 'CANCELLED', 'RECONCILED')
          AND p.createdAt < :threshold
        """)
    List<Payment> findStuckPayments(@Param("threshold") Instant threshold);
}
