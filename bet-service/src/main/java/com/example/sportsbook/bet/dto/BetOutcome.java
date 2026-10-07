package com.example.sportsbook.bet.dto;

/** Every way POST /api/v1/bets can resolve. Rejections are synchronous HTTP, never Kafka events. */
public enum BetOutcome {
    ACCEPTED,
    ODDS_CHANGED,
    MARKET_SUSPENDED,
    EVENT_NOT_LIVE,
    STALE_MARKET_DATA,
    /** The stake is above the configured maximum for one bet. */
    STAKE_LIMIT_EXCEEDED,
    /** Settlement has already processed this market; Redis had not caught up yet. */
    MARKET_SETTLED,
    INVALID_REQUEST
}
