package com.finledgerx.payment.repository;

import com.finledgerx.payment.domain.entity.PaymentAuditLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface PaymentAuditLogRepository extends JpaRepository<PaymentAuditLog, UUID> {

    List<PaymentAuditLog> findByPaymentIdOrderByCreatedAtAsc(UUID paymentId);
}
