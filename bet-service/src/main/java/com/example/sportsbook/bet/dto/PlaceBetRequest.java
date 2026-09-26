package com.example.sportsbook.bet.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record PlaceBetRequest(
        @NotBlank String userId,
        @NotBlank String eventId,
        @NotBlank String marketId,
        @NotBlank String selectionId,

        @NotNull
        @DecimalMin(value = "0.01", message = "stake must be positive")
        @Digits(integer = 15, fraction = 4)
        BigDecimal stake,

        @NotNull
        @DecimalMin(value = "1.01", message = "expectedOdds must exceed 1.00")
        @Digits(integer = 6, fraction = 4)
        BigDecimal expectedOdds,

        @NotBlank String idempotencyKey
) {
}
