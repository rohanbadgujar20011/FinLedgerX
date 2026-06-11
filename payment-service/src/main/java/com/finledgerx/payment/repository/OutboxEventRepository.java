package com.finledgerx.payment.repository;

import com.finledgerx.payment.domain.entity.OutboxEvent;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Fetch the oldest N unpublished events, ordered by creation time
     * to preserve approximate ordering within a partition.
     *
     * The partial index idx_outbox_unpublished (WHERE published = false)
     * makes this query extremely fast — it never scans published rows.
     */
    @Query("""
        SELECT o FROM OutboxEvent o
        WHERE o.published = false
        ORDER BY o.createdAt ASC
        """)
    List<OutboxEvent> findUnpublishedEvents(Pageable pageable);

    /**
     * Bulk-mark events as published in a single UPDATE statement
     * instead of N individual saves — reduces DB round trips.
     */
    @Modifying
    @Transactional
    @Query("""
        UPDATE OutboxEvent o
        SET o.published    = true,
            o.publishedAt  = :publishedAt
        WHERE o.id IN :ids
        """)
    void markPublishedBatch(@Param("ids") List<UUID> ids, @Param("publishedAt") Instant publishedAt);

    /**
     * Clean up old published events to prevent the table from growing unboundedly.
     * Called by a scheduled maintenance job (daily).
     */
    @Modifying
    @Transactional
    @Query("""
        DELETE FROM OutboxEvent o
        WHERE o.published = true
          AND o.publishedAt < :before
        """)
    int deletePublishedBefore(@Param("before") Instant before);

    long countByPublishedFalse();
}
