package com.finledgerx.payment.dto.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.Instant;

/**
 * All Kafka events published by the Payment Service extend this base.
 * Carries envelope metadata that consumers use for tracing and deduplication.
 */
@Getter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public abstract class BaseEvent {

    /** UUID of this specific event instance — used for consumer deduplication. */
    private String eventId;

    /** Discriminator field — e.g. "PAYMENT_INITIATED". Enables polymorphic routing. */
    private String eventType;

    /** Threads all events for one business transaction across services. */
    private String correlationId;

    /** The event that caused this event (e.g., the command / previous event ID). */
    private String causationId;

    /** Wall-clock time the event was created in the producing service. */
    private Instant occurredAt;

    /** Semantic version of this event schema, e.g. "1.0". */
    private String schemaVersion;
}
