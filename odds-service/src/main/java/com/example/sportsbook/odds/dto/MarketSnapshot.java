package com.example.sportsbook.odds.dto;

import com.example.sportsbook.common.MarketStatus;

import java.time.Instant;
import java.util.List;

/**
 * @param version       the provider version this snapshot reflects. Clients use it to reconcile the
 *                      REST snapshot with the WebSocket stream: any streamed event with a lower or
 *                      equal version is already included here and can be discarded.
 * @param lastUpdatedAt lets a client judge freshness for itself
 * @param suspendReason {@code STALE_FEED} while the staleness sweep holds the market suspended, else
 *                      null. That suspension is written straight to Redis and never reaches the
 *                      stream, so this snapshot is the only place a client can see it.
 */
public record MarketSnapshot(
        String marketId,
        MarketStatus status,
        long version,
        Instant lastUpdatedAt,
        String suspendReason,
        List<SelectionSnapshot> selections
) {
}
