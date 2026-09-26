package com.example.sportsbook.settlement.repository;

import com.example.sportsbook.common.BetStatus;
import com.example.sportsbook.settlement.domain.Bet;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface BetRepository extends JpaRepository<Bet, UUID> {

    /** Served by idx_bet_market_status. Only OPEN bets are settleable. */
    List<Bet> findByEventIdAndMarketIdAndStatus(String eventId, String marketId, BetStatus status);
}
