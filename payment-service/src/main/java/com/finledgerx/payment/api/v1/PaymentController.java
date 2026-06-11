package com.finledgerx.payment.api.v1;

import com.finledgerx.payment.domain.entity.PaymentAuditLog;
import com.finledgerx.payment.domain.enums.PaymentStatus;
import com.finledgerx.payment.dto.request.CreatePaymentRequest;
import com.finledgerx.payment.dto.request.UpdatePaymentStatusRequest;
import com.finledgerx.payment.dto.response.PagedResponse;
import com.finledgerx.payment.dto.response.PaymentCreationResult;
import com.finledgerx.payment.dto.response.PaymentResponse;
import com.finledgerx.payment.service.PaymentService;
import io.micrometer.core.annotation.Timed;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payments")
@RequiredArgsConstructor
@Validated
@Slf4j
public class PaymentController {

    private static final String IDEMPOTENCY_HEADER = "X-Idempotency-Key";

    private final PaymentService paymentService;

    // ── POST /api/v1/payments ────────────────────────────────────
    /**
     * Initiate a new payment.
     *
     * The X-Idempotency-Key header is REQUIRED.
     * Clients must supply a UUID (or any opaque unique string) generated
     * on their side. If they retry the exact same request, they'll get
     * back the original response instead of a duplicate payment.
     *
     * Response: 201 Created with Location header pointing to the payment.
     */
    @PostMapping
    @Timed(value = "payment.api.create", description = "Time taken to initiate a payment")
    public ResponseEntity<PaymentResponse> createPayment(
            @RequestHeader(IDEMPOTENCY_HEADER) @NotBlank String idempotencyKey,
            @RequestBody @Valid CreatePaymentRequest request) {

        log.info("POST /api/v1/payments idempotencyKey={} fromAccount={} amount={} {}",
                idempotencyKey, request.fromAccountId(), request.amount(), request.currency());

        PaymentCreationResult result = paymentService.createPayment(request, idempotencyKey);

        if (result.replay()) {
            // Duplicate request — return the original response with an indicator header.
            // HTTP 200 (not 201) signals the resource already exists.
            log.info("Duplicate request replayed for idempotencyKey={}", idempotencyKey);
            return ResponseEntity
                    .status(HttpStatus.OK)
                    .header("Idempotent-Replayed", "true")
                    .header("X-Duplicate-Request", "Duplicate request - payment already processed")
                    .header("Location", "/api/v1/payments/" + result.payment().id())
                    .body(result.payment());
        }

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .header("Location", "/api/v1/payments/" + result.payment().id())
                .body(result.payment());
    }

    // ── GET /api/v1/payments/{paymentId} ─────────────────────────
    /**
     * Retrieve a single payment by its ID.
     * Response is cached in Redis for 5 minutes (see RedisConfig).
     */
    @GetMapping("/{paymentId}")
    @Timed(value = "payment.api.get", description = "Time taken to fetch a payment")
    public ResponseEntity<PaymentResponse> getPayment(@PathVariable UUID paymentId) {
        log.debug("GET /api/v1/payments/{}", paymentId);
        return ResponseEntity.ok(paymentService.getPayment(paymentId));
    }

    // ── GET /api/v1/payments ─────────────────────────────────────
    /**
     * Search/list payments with optional filters.
     * Supports pagination and sorting.
     *
     * Example:
     *   GET /api/v1/payments?fromAccountId=uuid&status=SETTLED&page=0&size=20&sort=createdAt,desc
     */
    @GetMapping
    public ResponseEntity<PagedResponse<PaymentResponse>> searchPayments(
            @RequestParam(required = false) UUID fromAccountId,
            @RequestParam(required = false) UUID toAccountId,
            @RequestParam(required = false) PaymentStatus status,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant fromDate,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant toDate,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "createdAt") String sortBy,
            @RequestParam(defaultValue = "DESC") Sort.Direction sortDir) {

        // Cap page size to prevent DoS via huge payloads
        int cappedSize = Math.min(size, 100);
        Pageable pageable = PageRequest.of(page, cappedSize, Sort.by(sortDir, sortBy));

        return ResponseEntity.ok(
                paymentService.searchPayments(fromAccountId, toAccountId,
                        status, fromDate, toDate, pageable));
    }

    // ── POST /api/v1/payments/{paymentId}/cancel ─────────────────
    /**
     * Cancel a payment that is in AUTHORIZED status.
     * Only valid before processing has started.
     */
    @PostMapping("/{paymentId}/cancel")
    @Timed(value = "payment.api.cancel", description = "Time taken to cancel a payment")
    public ResponseEntity<PaymentResponse> cancelPayment(
            @PathVariable UUID paymentId,
            @RequestHeader(value = "X-Actor", defaultValue = "USER") String actor) {

        log.info("POST /api/v1/payments/{}/cancel actor={}", paymentId, actor);
        return ResponseEntity.ok(paymentService.cancelPayment(paymentId, actor));
    }

    // ── PATCH /api/v1/payments/{paymentId}/status ─────────────────
    /**
     * Internal endpoint — updates payment status.
     * Called by the Ledger Service or Reconciliation Service via internal network.
     *
     * In production: protect with mTLS or an internal API gateway rule
     * so this endpoint is unreachable from the public internet.
     */
    @PatchMapping("/{paymentId}/status")
    public ResponseEntity<PaymentResponse> updateStatus(
            @PathVariable UUID paymentId,
            @RequestBody @Valid UpdatePaymentStatusRequest request) {

        log.info("PATCH /api/v1/payments/{}/status newStatus={}", paymentId, request.status());
        return ResponseEntity.ok(paymentService.updateStatus(paymentId, request));
    }

    // ── GET /api/v1/payments/{paymentId}/audit ────────────────────
    /**
     * Returns the full immutable audit trail for a payment.
     * Useful for compliance, dispute resolution, and ops investigation.
     */
    @GetMapping("/{paymentId}/audit")
    public ResponseEntity<List<PaymentAuditLog>> getAuditLog(@PathVariable UUID paymentId) {
        return ResponseEntity.ok(paymentService.getAuditLog(paymentId));
    }
}
