package com.finledgerx.payment.dto.request;

import com.finledgerx.payment.domain.enums.PaymentStatus;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Internal request body for PATCH /api/v1/payments/{id}/status.
 * Called by other internal services (e.g. Ledger Service callback).
 * Should NOT be exposed on the public API — secure with internal network or mTLS.
 */
public record UpdatePaymentStatusRequest(

        @NotNull(message = "status is required")
        PaymentStatus status,

        @Size(max = 1000, message = "failureReason must not exceed 1000 characters")
        String failureReason,

        @Size(max = 100, message = "actor must not exceed 100 characters")
        String actor
) {}
