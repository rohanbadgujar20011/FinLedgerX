package com.finledgerx.payment.exception;

/**
 * Thrown when a duplicate request arrives while the original is still in-flight.
 * HTTP 409 Conflict — the client should wait and retry.
 */
public class IdempotencyConflictException extends FinLedgerXException {

    public IdempotencyConflictException(String idempotencyKey) {
        super("IDEMPOTENCY_CONFLICT",
                "A request with idempotency key [" + idempotencyKey + "] is already in progress.");
    }
}
