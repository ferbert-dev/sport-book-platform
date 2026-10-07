package com.example.sportsbook.bet.controller;

import com.example.sportsbook.bet.dto.BetOutcome;
import com.example.sportsbook.bet.dto.PlaceBetRequest;
import com.example.sportsbook.bet.dto.PlaceBetResponse;
import com.example.sportsbook.bet.service.BetService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/bets")
public class BetController {

    private final BetService betService;

    public BetController(BetService betService) {
        this.betService = betService;
    }

    /**
     * The body always carries the machine-readable {@link BetOutcome}; the status says whether a
     * retry can ever help.
     *
     * <ul>
     *   <li>422 for a well-formed request the world is not ready for (market suspended or settled,
     *       odds moved, stale data, stake limit): it may succeed later, or with other values.</li>
     *   <li>400 for {@code INVALID_REQUEST}: the ids in the request do not fit together (unknown
     *       selection, a market of another event). It can never succeed as sent.</li>
     * </ul>
     */
    @PostMapping
    public ResponseEntity<PlaceBetResponse> placeBet(@Valid @RequestBody PlaceBetRequest request) {
        PlaceBetResponse response = betService.placeBet(request);
        HttpStatus status = switch (response.status()) {
            case ACCEPTED -> HttpStatus.CREATED;
            case INVALID_REQUEST -> HttpStatus.BAD_REQUEST;
            case ODDS_CHANGED, MARKET_SUSPENDED, EVENT_NOT_LIVE, STALE_MARKET_DATA,
                 STAKE_LIMIT_EXCEEDED, MARKET_SETTLED -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
        return ResponseEntity.status(status).body(response);
    }
}
