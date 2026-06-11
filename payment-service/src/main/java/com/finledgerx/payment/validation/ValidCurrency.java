package com.finledgerx.payment.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.*;

/**
 * Custom Bean Validation annotation that ensures the currency string
 * is a valid ISO 4217 3-letter currency code (e.g. "INR", "USD", "EUR").
 */
@Documented
@Constraint(validatedBy = CurrencyValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.RECORD_COMPONENT})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidCurrency {

    String message() default "Invalid currency code. Must be a valid ISO 4217 3-letter code (e.g. INR, USD, EUR)";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
