package com.example.sportsbook.bet.controller;

import com.example.sportsbook.bet.dto.BetOutcome;
import com.example.sportsbook.bet.dto.PlaceBetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

@RestControllerAdvice
public class BetExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(BetExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<PlaceBetResponse> handleValidation(MethodArgumentNotValidException exception) {
        String detail = exception.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining("; "));

        log.info("BET_REJECTED_INVALID_REQUEST detail={}", detail);
        return ResponseEntity.badRequest()
                .body(PlaceBetResponse.rejected(BetOutcome.INVALID_REQUEST, detail));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<PlaceBetResponse> handleUnexpected(Exception exception) {
        log.error("UNEXPECTED_ERROR", exception);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(PlaceBetResponse.rejected(BetOutcome.INVALID_REQUEST, "Unexpected error"));
    }
}
