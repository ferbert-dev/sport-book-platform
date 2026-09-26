package com.example.sportsbook.odds.service;

import com.example.sportsbook.odds.dto.EventSnapshotResponse;
import com.example.sportsbook.odds.dto.MarketSnapshot;
import com.example.sportsbook.odds.dto.SelectionSnapshot;
import com.example.sportsbook.odds.exception.EventNotFoundException;
import com.example.sportsbook.odds.repository.EventStateRepository;
import org.springframework.stereotype.Service;

import java.util.List;

/** Assembles the REST snapshot half of the SNAPSHOT + STREAM pattern. */
@Service
public class EventSnapshotService {

    private final EventStateRepository repository;

    public EventSnapshotService(EventStateRepository repository) {
        this.repository = repository;
    }

    public EventSnapshotResponse snapshot(String eventId) {
        EventStateRepository.EventState event = repository.findEvent(eventId)
                .orElseThrow(() -> new EventNotFoundException(eventId));

        List<MarketSnapshot> markets = repository.findMarketIds(eventId).stream()
                .map(repository::findMarket)
                .flatMap(java.util.Optional::stream)
                .map(this::toMarketSnapshot)
                .toList();

        return new EventSnapshotResponse(event.eventId(), event.status(), markets);
    }

    private MarketSnapshot toMarketSnapshot(EventStateRepository.MarketState market) {
        List<SelectionSnapshot> selections = repository.findOdds(market.marketId()).entrySet().stream()
                .map(entry -> new SelectionSnapshot(entry.getKey(), entry.getValue()))
                .toList();

        return new MarketSnapshot(market.marketId(), market.status(), market.version(),
                market.lastUpdatedAt(), selections);
    }
}
