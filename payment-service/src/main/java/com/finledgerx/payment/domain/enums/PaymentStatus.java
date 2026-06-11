package com.finledgerx.payment.domain.enums;

import com.finledgerx.payment.exception.InvalidStateTransitionException;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Payment state machine.
 *
 * Valid transitions:
 *
 *   INITIATED ──► VALIDATING ──► AUTHORIZED ──► PROCESSING ──► SETTLED ──► RECONCILED
 *       │               │              │              │
 *       └──► FAILED     └──► FAILED    └──► CANCELLED └──► FAILED
 *                                                           │
 *                                                  SETTLED ─┴──► REVERSED
 */
public enum PaymentStatus {

    INITIATED,
    VALIDATING,
    AUTHORIZED,
    PROCESSING,
    SETTLED,
    RECONCILED,
    FAILED,
    REVERSED,
    CANCELLED;

    private static final Map<PaymentStatus, Set<PaymentStatus>> TRANSITIONS = Map.of(
            INITIATED,   Set.of(VALIDATING, FAILED),
            VALIDATING,  Set.of(AUTHORIZED, FAILED),
            AUTHORIZED,  Set.of(PROCESSING, CANCELLED),
            PROCESSING,  Set.of(SETTLED, FAILED),
            SETTLED,     Set.of(RECONCILED, REVERSED),
            RECONCILED,  Set.of(),
            FAILED,      Set.of(),
            REVERSED,    Set.of(),
            CANCELLED,   Set.of()
    );

    public boolean canTransitionTo(PaymentStatus next) {
        return TRANSITIONS.getOrDefault(this, Set.of()).contains(next);
    }

    /**
     * Guards a transition and throws a typed exception on violation.
     * Call this from Payment.transitionTo() so the entity itself enforces the FSM.
     */
    public void assertCanTransitionTo(PaymentStatus next, UUID paymentId) {
        if (!canTransitionTo(next)) {
            throw new InvalidStateTransitionException(paymentId, this, next);
        }
    }

    public boolean isTerminal() {
        return this == RECONCILED || this == FAILED || this == REVERSED || this == CANCELLED;
    }
}
