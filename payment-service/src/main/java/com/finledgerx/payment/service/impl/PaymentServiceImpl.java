package com.finledgerx.payment.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.finledgerx.payment.constants.RedisKeyConstants;
import com.finledgerx.payment.domain.entity.OutboxEvent;
import com.finledgerx.payment.domain.entity.Payment;
import com.finledgerx.payment.domain.enums.PaymentStatus;
import com.finledgerx.payment.dto.event.PaymentInitiatedEvent;
import com.finledgerx.payment.dto.request.CreatePaymentRequest;
import com.finledgerx.payment.dto.response.PaymentCreationResult;
import com.finledgerx.payment.dto.response.PaymentResponse;
import com.finledgerx.payment.exception.PaymentValidationException;
import com.finledgerx.payment.mapper.PaymentMapper;
import com.finledgerx.payment.repository.IdempotencyKeyRepository;
import com.finledgerx.payment.repository.OutboxEventRepository;
import com.finledgerx.payment.repository.PaymentRepository;
import com.finledgerx.payment.service.IdempotencyService;
import com.finledgerx.payment.service.PaymentAuditService;
import com.finledgerx.payment.service.PaymentService;
import com.finledgerx.payment.service.RedisLockService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentServiceImpl implements PaymentService {

    private final IdempotencyService idempotencyService;
    private final RedisLockService lockService;
    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final PaymentAuditService auditService;
    private final  PaymentMapper paymentMapper;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;

    @Autowired
    @Lazy
    private PaymentServiceImpl self;

    @Value("${finledgerx.redis.lock.account-debit-ttl-ms:10000}")
    private long accountDebitLockTtlMs;

    @Value("${finledgerx.redis.lock.wait-ms:5000}")
    private long lockWaitMs;


    @Override
    public PaymentCreationResult createPayment(CreatePaymentRequest request, String idempotencyKey) {

        // ── Step 1: Idempotency check ────────────────────────────
        Optional<String> cached = idempotencyService.checkAndReserve(idempotencyKey);
        if (cached.isPresent()) {
            log.info("Idempotent replay for key={}", idempotencyKey);
            return PaymentCreationResult.replayed(deserializeResponse(cached.get()));
        }

        // Step 2: Business rule validation
        if (request.fromAccountId().equals(request.toAccountId())) {
            idempotencyService.fail(idempotencyKey);
            throw new PaymentValidationException("Source and destination accounts must be different.");
        }

        // ── Step 3: Distributed lock on source account ───────────
        String lockKey = RedisKeyConstants.accountDebitLock(request.fromAccountId());

        try {
            PaymentResponse response = lockService.executeWithLock(lockKey, accountDebitLockTtlMs, lockWaitMs,
                    () -> self.executePaymentCreation(request, idempotencyKey));
            return PaymentCreationResult.fresh(response);
        } catch (Exception ex) {
            // Clean up idempotency reservation so the client can retry
            idempotencyService.fail(idempotencyKey);
            throw ex;
        }
    }

    @Transactional
    public PaymentResponse executePaymentCreation(CreatePaymentRequest request,
                                                  String idempotencyKey) {
        UUID correlationId = UUID.randomUUID();

        // ── 4a: Create Payment entity
        Payment payment = Payment.builder()
                .fromAccountId(request.fromAccountId())
                .toAccountId(request.toAccountId())
                .amount(request.amount())
                .currency(request.currency())
                .status(PaymentStatus.INITIATED)
                .paymentMethod(request.paymentMethod())
                .description(request.description())
                .idempotencyKey(idempotencyKey)
                .correlationId(correlationId)
                .build();

        payment = paymentRepository.save(payment);
        log.info("Payment created: id={}, correlationId={}", payment.getId(), correlationId);

        // ── 4b: Persist idempotency key record ───────────────────
        idempotencyService.persistInFlight(idempotencyKey);

        // ── 4c: Write outbox event (atomically with payment) ─────
        PaymentInitiatedEvent event = PaymentInitiatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("PAYMENT_INITIATED")
                .correlationId(correlationId.toString())
                .causationId(payment.getId().toString())
                .occurredAt(Instant.now())
                .schemaVersion("1.0")
                .paymentId(payment.getId().toString())
                .fromAccountId(payment.getFromAccountId().toString())
                .toAccountId(payment.getToAccountId().toString())
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .paymentMethod(payment.getPaymentMethod().name())
                .description(payment.getDescription())
                .build();

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .topic("payment.events")
                .partitionKey(payment.getFromAccountId().toString()) // same account → same partition → ordering
                .eventType("PAYMENT_INITIATED")
                .aggregateId(payment.getId().toString())
                .payload(serializeEvent(event))
                .build();

        outboxEventRepository.save(outboxEvent);

        // ── 4d: Audit entry ──────────────────────────────────────
        auditService.record(payment.getId(), "PAYMENT_CREATED",
                null, PaymentStatus.INITIATED, "SYSTEM");

        PaymentResponse response = paymentMapper.toResponse(payment);

        // ── Step 5: Complete idempotency reservation ─────────────
        idempotencyService.complete(idempotencyKey, payment.getId(), serializeResponse(response));

        // ── Step 6: Metrics ──────────────────────────────────────
        meterRegistry.counter("payment.initiated.total",
                "currency",       payment.getCurrency(),
                "paymentMethod",  payment.getPaymentMethod().name()
        ).increment();

        return response;
    }


    private String resolveEventType(PaymentStatus status) {
        return switch (status) {
            case SETTLED   -> "PAYMENT_COMPLETED";
            case FAILED    -> "PAYMENT_FAILED";
            case CANCELLED -> "PAYMENT_CANCELLED";
            case REVERSED  -> "PAYMENT_REVERSED";
            default        -> "PAYMENT_STATUS_CHANGED";
        };
    }

    private String serializeEvent(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event: " + event.getClass().getSimpleName(), e);
        }
    }

    private String serializeResponse(PaymentResponse response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize PaymentResponse", e);
        }
    }

    private PaymentResponse deserializeResponse(String json) {
        try {
            return objectMapper.readValue(json, PaymentResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize cached PaymentResponse", e);
        }
    }

}
