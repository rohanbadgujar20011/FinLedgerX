package com.finledgerx.payment.mapper;

import com.finledgerx.payment.domain.entity.Payment;
import com.finledgerx.payment.dto.response.PaymentResponse;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

/**
 * MapStruct mapper — generates a compile-time implementation.
 * componentModel = "spring" means the generated impl is a @Component
 * and can be @Autowired / injected by Spring.
 */
@Mapper(componentModel = "spring")
public interface PaymentMapper {

    @Mapping(target = "id",            source = "id")
    @Mapping(target = "fromAccountId", source = "fromAccountId")
    @Mapping(target = "toAccountId",   source = "toAccountId")
    @Mapping(target = "amount",        source = "amount")
    @Mapping(target = "currency",      source = "currency")
    @Mapping(target = "status",        source = "status")
    @Mapping(target = "paymentMethod", source = "paymentMethod")
    @Mapping(target = "description",   source = "description")
    @Mapping(target = "correlationId", source = "correlationId")
    @Mapping(target = "failureReason", source = "failureReason")
    @Mapping(target = "processedAt",   source = "processedAt")
    @Mapping(target = "settledAt",     source = "settledAt")
    @Mapping(target = "createdAt",     source = "createdAt")
    @Mapping(target = "updatedAt",     source = "updatedAt")
    @Mapping(target = "version",       source = "version")
    PaymentResponse toResponse(Payment payment);
}
