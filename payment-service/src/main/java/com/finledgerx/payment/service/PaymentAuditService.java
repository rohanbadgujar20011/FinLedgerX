package com.finledgerx.payment.service;

import com.finledgerx.payment.domain.entity.PaymentAuditLog;
import com.finledgerx.payment.domain.enums.PaymentStatus;
import com.finledgerx.payment.repository.PaymentAuditLogRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PaymentAuditService {

    private final PaymentAuditLogRepository auditLogRepository;

    /**
     * Records a synchronous audit entry within the current transaction.
     * Use this when the audit record must be committed atomically with the payment update.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void record(UUID paymentId,
                       String action,
                       PaymentStatus fromStatus,
                       PaymentStatus toStatus,
                       String actor) {
        record(paymentId, action, fromStatus, toStatus, actor, null);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void record(UUID paymentId,
                       String action,
                       PaymentStatus fromStatus,
                       PaymentStatus toStatus,
                       String actor,
                       String details) {
        PaymentAuditLog log = PaymentAuditLog.builder()
                .paymentId(paymentId)
                .action(action)
                .fromStatus(fromStatus)
                .toStatus(toStatus)
                .actor(actor)
                .details(details)
                .build();

        auditLogRepository.save(log);
    }

    /**
     * Async variant — use for audit entries that don't need to be in the same DB transaction
     * (e.g. read events, cache hits). Does NOT propagate the caller's transaction.
     */
    @Async
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordAsync(UUID paymentId,
                            String action,
                            PaymentStatus fromStatus,
                            PaymentStatus toStatus,
                            String actor) {
        record(paymentId, action, fromStatus, toStatus, actor);
    }

    @Transactional(readOnly = true)
    public List<PaymentAuditLog> getAuditLog(UUID paymentId) {
        return auditLogRepository.findByPaymentIdOrderByCreatedAtAsc(paymentId);
    }
}
