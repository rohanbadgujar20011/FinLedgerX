package com.finledgerx.payment.domain.entity;

import com.finledgerx.payment.domain.enums.PaymentMethod;
import com.finledgerx.payment.domain.enums.PaymentStatus;
import jakarta.persistence.*;
import lombok.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "payments",
        indexes = {
                @Index(name = "idx_payments_from_account",    columnList = "from_account_id"),
                @Index(name = "idx_payments_to_account",      columnList = "to_account_id"),
                @Index(name = "idx_payments_status",          columnList = "status"),
                @Index(name = "idx_payments_idempotency_key", columnList = "idempotency_key", unique = true),
                @Index(name = "idx_payments_created_at",      columnList = "created_at")
        }
)
@EntityListeners(AuditingEntityListener.class)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "from_account_id", nullable = false, updatable = false)
    private UUID fromAccountId;

    @Column(name = "to_account_id", nullable = false, updatable = false)
    private UUID toAccountId;

    @Column(name = "amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private PaymentStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "payment_method", nullable = false, length = 30)
    private PaymentMethod paymentMethod;

    @Column(name = "description", length = 500)
    private String description;

    /**
     * Caller-provided unique key to prevent duplicate submissions.
     * Stored as-is (UUID string or any opaque token).
     */
    @Column(name = "idempotency_key", nullable = false, unique = true, length = 255)
    private String idempotencyKey;

    /**
     * Correlation ID threads all events for this payment across services.
     * Set once at creation; never changes.
     */
    @Column(name = "correlation_id", nullable = false, updatable = false)
    private UUID correlationId;

    @Column(name = "failure_reason", length = 1000)
    private String failureReason;

    @Column(name = "processed_at")
    private Instant processedAt;

    @Column(name = "settled_at")
    private Instant settledAt;

    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @LastModifiedDate
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    /**
     * Optimistic Concurrency Control.
     * Prevents lost-update race conditions when multiple threads
     * try to update the same payment row.
     */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    // ── Domain logic ────────────────────────────────────────────

    /**
     * State machine transition guard.
     * Throws InvalidStateTransitionException if the transition is illegal.
     * All status changes MUST go through this method — never set status directly.
     */
    public void transitionTo(PaymentStatus newStatus) {
        this.status.assertCanTransitionTo(newStatus, this.id);
        this.status = newStatus;

        if (newStatus == PaymentStatus.PROCESSING) {
            this.processedAt = Instant.now();
        }
        if (newStatus == PaymentStatus.SETTLED) {
            this.settledAt = Instant.now();
        }
    }

    public void fail(String reason) {
        transitionTo(PaymentStatus.FAILED);
        this.failureReason = reason;
    }

    public boolean isTerminal() {
        return this.status.isTerminal();
    }
}
