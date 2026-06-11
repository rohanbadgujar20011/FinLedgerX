package com.finledgerx.payment.dto.response;

/**
 * Internal wrapper returned by PaymentService.createPayment().
 *
 * Separates two concerns:
 *   1. The actual payment data (always present).
 *   2. Whether this was a fresh creation or an idempotency replay.
 *
 * The controller uses `replay` to:
 *   - Return HTTP 200 (instead of 201) for replays.
 *   - Add the `Idempotent-Replayed: true` response header.
 */
public record PaymentCreationResult(
        PaymentResponse payment,
        boolean replay
) {
    public static PaymentCreationResult fresh(PaymentResponse payment) {
        return new PaymentCreationResult(payment, false);
    }

    public static PaymentCreationResult replayed(PaymentResponse payment) {
        return new PaymentCreationResult(payment, true);
    }
}
