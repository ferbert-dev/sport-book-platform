package com.example.sportsbook.common;

/** Lifecycle of a betting market. Bets are only accepted while ACTIVE. */
public enum MarketStatus {
    ACTIVE,
    SUSPENDED,
    CLOSED,
    SETTLED
}
