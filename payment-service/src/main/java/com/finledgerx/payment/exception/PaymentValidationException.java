package com.finledgerx.payment.exception;

public class PaymentValidationException extends FinLedgerXException {

    public PaymentValidationException(String message) {
        super("PAYMENT_VALIDATION_ERROR", message);
    }
}
