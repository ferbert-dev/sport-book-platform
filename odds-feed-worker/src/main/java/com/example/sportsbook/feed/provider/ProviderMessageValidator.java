package com.example.sportsbook.feed.provider;

import java.math.BigDecimal;

/** Structural validation of raw provider messages, before normalization. */
public final class ProviderMessageValidator {

    private ProviderMessageValidator() {
    }

    public static boolean isValid(ProviderMessage message) {
        if (message == null || message.messageType() == null || message.sentAt() == null) {
            return false;
        }
        if (isBlank(message.matchId()) || message.sequenceNumber() < 0) {
            return false;
        }
        return switch (message.messageType()) {
            // A price change without a positive price or an outcome is unusable.
            case "PRICE_CHANGE" -> !isBlank(message.marketRef())
                    && !isBlank(message.outcomeRef())
                    && message.price() != null
                    && message.price().compareTo(BigDecimal.ONE) > 0;
            case "MARKET_LOCK", "MARKET_UNLOCK" -> !isBlank(message.marketRef());
            case "MARKET_RESULT" -> !isBlank(message.marketRef()) && !isBlank(message.winnerRef());
            case "MATCH_START", "MATCH_END" -> true;
            default -> false;
        };
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
