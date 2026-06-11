package com.finledgerx.payment.exception;

public class DistributedLockException extends FinLedgerXException {

    public DistributedLockException(String lockKey) {
        super("LOCK_ACQUISITION_FAILED",
                "Could not acquire distributed lock for key: " + lockKey +
                        ". Another operation oan this resource is in progress.");
    }
}
