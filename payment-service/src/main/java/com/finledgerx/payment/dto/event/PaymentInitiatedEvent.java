package com.finledgerx.payment.dto.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;

/**
 * Published to Kafka topic "payment.events" when a new payment is created.
 * The Ledger Service consumes this event to perform the double-entry debit/credit.
 */
@Getter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentInitiatedEvent extends BaseEvent {

    private String paymentId;
    private String fromAccountId;
    private String  toAccountId;
    private BigDecimal amount;
    private String  currency;
    private String  paymentMethod;
    private String  description;
}
