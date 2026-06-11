package com.finledgerx.payment.dto.request;

import com.finledgerx.payment.domain.enums.PaymentMethod;
import com.finledgerx.payment.validation.ValidCurrency;
import jakarta.validation.constraints.*;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Request body for POST /api/v1/payments.
 *
 * Uses Java 21 records — Bean Validation annotations on record components
 * are fully supported by Spring Boot 3 / Hibernate Validator 8.
 */
public record CreatePaymentRequest(

        @NotNull(message = "fromAccountId is required")
        UUID fromAccountId,

        @NotNull(message = "toAccountId is required")
        UUID toAccountId,

        @NotNull(message = "amount is required")
        @DecimalMin(value = "0.01", message = "amount must be greater than 0")
        @Digits(integer = 15, fraction = 4, message = "amount must have at most 15 integer digits and 4 decimal places")
        BigDecimal amount,

        @NotBlank(message = "currency is required")
        @ValidCurrency
        String currency,

        @NotNull(message = "paymentMethod is required")
        PaymentMethod paymentMethod,

        @Size(max = 500, message = "description must not exceed 500 characters")
        String description
) {
    /**
     * Compact constructor: normalise currency to uppercase.
     */
    public CreatePaymentRequest {
        if (currency != null) {
            currency = currency.toUpperCase();
        }
    }
}
