package com.finledgerx.payment.exception;

import com.finledgerx.payment.domain.enums.PaymentStatus;

import java.util.UUID;

public class InvalidStateTransitionException extends FinLedgerXException {

    public InvalidStateTransitionException(UUID paymentId,
                                           PaymentStatus from,
                                           PaymentStatus to) {
        super("INVALID_STATE_TRANSITION",
                String.format("Payment [%s]: cannot transition from %s to %s", paymentId, from, to));
    }
}
