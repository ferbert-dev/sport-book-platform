package com.example.sportsbook.odds.dto;

import com.example.sportsbook.common.EventStatus;

import java.util.List;

public record EventSnapshotResponse(
        String eventId,
        EventStatus status,
        List<MarketSnapshot> markets
) {
}
