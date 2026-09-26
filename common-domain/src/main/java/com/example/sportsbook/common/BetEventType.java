package com.example.sportsbook.common;

/** Event types carried on the {@code bet-events} topic. */
public enum BetEventType {
    BET_PLACED,
    BET_SETTLED,
    PAYOUT_REQUIRED
}
