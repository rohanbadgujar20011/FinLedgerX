package com.finledgerx.payment.service;


import com.finledgerx.payment.dto.request.CreatePaymentRequest;
import com.finledgerx.payment.dto.response.PaymentCreationResult;
import com.finledgerx.payment.dto.response.PaymentResponse;

import java.util.UUID;

public interface PaymentService {

    /**
     * Initiate a new payment.
     *
     * @param request        Validated payment creation request.
     * @param idempotencyKey Caller-supplied unique key to prevent duplicate submissions.
     * @return PaymentCreationResult — wraps the payment response and a flag indicating
     *         whether this was a fresh creation ({@code replay=false}) or an idempotency
     *         replay of a previously completed request ({@code replay=true}).
     */
    PaymentCreationResult createPayment(CreatePaymentRequest request, String idempotencyKey);

    PaymentResponse getPayment(UUID paymentId);

}
