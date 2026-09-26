package com.example.sportsbook.bet.dto;

/** Every way POST /api/v1/bets can resolve. Rejections are synchronous HTTP, never Kafka events. */
public enum BetOutcome {
    ACCEPTED,
    ODDS_CHANGED,
    MARKET_SUSPENDED,
    EVENT_NOT_LIVE,
    STALE_MARKET_DATA,
    INVALID_REQUEST
}
