package com.finledgerx.payment.exception;

/**
 * Root of the FinLedgerX exception hierarchy.
 * Every service-specific exception extends this class.
 */
public abstract class FinLedgerXException extends RuntimeException {

    private final String errorCode;

    protected FinLedgerXException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    protected FinLedgerXException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
