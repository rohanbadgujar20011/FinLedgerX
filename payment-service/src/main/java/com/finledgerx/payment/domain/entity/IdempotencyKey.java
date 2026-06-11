package com.finledgerx.payment.domain.entity;

import jakarta.persistence.*;
import lombok.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.Instant;
import java.util.UUID;

/**
 * Durable idempotency record stored in PostgreSQL.
 *
 * Redis is the first-line idempotency check (sub-millisecond).
 * This table is the durable fallback — if Redis is flushed or
 * the TTL expires before 24h, the DB prevents a second execution.
 *
 * The response_body column stores the serialized PaymentResponse
 * so we can return the exact same response on a duplicate request
 * without re-executing the business logic.
 */
@Entity
@Table(
        name = "idempotency_keys",
        indexes = {
                @Index(name = "idx_idempotency_key",        columnList = "idempotency_key", unique = true),
                @Index(name = "idx_idempotency_expires_at",  columnList = "expires_at")
        }
)
@EntityListeners(AuditingEntityListener.class)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IdempotencyKey {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, unique = true, length = 255)
    private String idempotencyKey;

    /**
     * The payment ID that was created for this key.
     * Null while the request is still in-flight (status = IN_FLIGHT).
     */
    @Column(name = "payment_id")
    private UUID paymentId;

    @Column(name = "request_hash", length = 64)
    private String requestHash;

    /**
     * JSON of the PaymentResponse returned to the client.
     * Populated once the request completes successfully.
     */
    @Column(name = "response_body", columnDefinition = "TEXT")
    private String responseBody;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    @Builder.Default
    private IdempotencyStatus status = IdempotencyStatus.IN_FLIGHT;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    public enum IdempotencyStatus {
        IN_FLIGHT,
        COMPLETED,
        FAILED
    }

    public void complete(UUID paymentId, String responseBody) {
        this.paymentId    = paymentId;
        this.responseBody = responseBody;
        this.status       = IdempotencyStatus.COMPLETED;
    }

    public void fail() {
        this.status = IdempotencyStatus.FAILED;
    }

    public boolean isExpired() {
        return Instant.now().isAfter(this.expiresAt);
    }
}
