package com.example.sportsbook.common;

/** Event types carried on the {@code sports-events} topic. */
public enum SportsEventType {
    ODDS_UPDATED,
    MARKET_SUSPENDED,
    MARKET_OPENED,
    MATCH_STARTED,
    MATCH_FINISHED,
    MARKET_SETTLED
}
