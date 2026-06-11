package com.finledgerx.payment.domain.entity;

import jakarta.persistence.*;
import lombok.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.Instant;
import java.util.UUID;

/**
 * Transactional Outbox Pattern entity.
 *
 * Written atomically in the SAME transaction as the Payment row.
 * The OutboxPoller reads unpublished rows every 500ms and sends them to Kafka.
 *
 * Why: If Kafka is down when a payment is created, the payment is still
 * safely recorded. The event will be published once Kafka recovers.
 * No event can ever be lost.
 */
@Entity
@Table(
        name = "outbox_events",
        indexes = {
                @Index(name = "idx_outbox_unpublished", columnList = "published, created_at")
        }
)
@EntityListeners(AuditingEntityListener.class)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "topic", nullable = false, length = 200)
    private String topic;

    /**
     * Kafka partition key — typically fromAccountId so all events
     * for the same account go to the same partition (ordering guarantee).
     */
    @Column(name = "partition_key", nullable = false, length = 255)
    private String partitionKey;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @Column(name = "aggregate_id", nullable = false, length = 255)
    private String aggregateId;

    /**
     * JSON-serialized event payload (e.g. PaymentInitiatedEvent as JSON string).
     * Stored as TEXT. No schema versioning at DB level — handled by consumer.
     */
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Builder.Default
    @Column(name = "published", nullable = false)
    private boolean published = false;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Builder.Default
    @Column(name = "retry_count", nullable = false)
    private int retryCount = 0;

    @CreatedDate
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    public void markPublished() {
        this.published   = true;
        this.publishedAt = Instant.now();
    }

    public void incrementRetry() {
        this.retryCount++;
    }
}
