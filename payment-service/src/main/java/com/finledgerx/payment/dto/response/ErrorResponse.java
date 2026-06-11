package com.finledgerx.payment.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorResponse(
        String  errorCode,
        String  message,
        Instant timestamp,
        String path,
        List<String> validationErrors
) {
    public static ErrorResponse of(String errorCode, String message, String path) {
        return new ErrorResponse(errorCode, message, Instant.now(), path, null);
    }

    public static ErrorResponse ofValidation(String errorCode, String message,
                                             String path, List<String> validationErrors) {
        return new ErrorResponse(errorCode, message, Instant.now(), path, validationErrors);
    }
}
