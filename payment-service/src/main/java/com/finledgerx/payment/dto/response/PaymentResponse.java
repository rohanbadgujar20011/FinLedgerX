package com.finledgerx.payment.dto.response;

import com.finledgerx.payment.domain.enums.PaymentMethod;
import com.finledgerx.payment.domain.enums.PaymentStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentResponse(
        UUID        id,
        UUID        fromAccountId,
        UUID        toAccountId,
        BigDecimal  amount,
        String      currency,
        PaymentStatus  status,
        PaymentMethod  paymentMethod,
        String      description,
        UUID        correlationId,
        String      failureReason,
        Instant     processedAt,
        Instant     settledAt,
        Instant     createdAt,
        Instant     updatedAt,
        Long        version
) {}
