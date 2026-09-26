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
     * Rejections return 422 rather than 400: the request was well-formed, but the market moved.
     * The body always carries the machine-readable {@link BetOutcome}.
     */
    @PostMapping
    public ResponseEntity<PlaceBetResponse> placeBet(@Valid @RequestBody PlaceBetRequest request) {
        PlaceBetResponse response = betService.placeBet(request);
        HttpStatus status = response.status() == BetOutcome.ACCEPTED
                ? HttpStatus.CREATED
                : HttpStatus.UNPROCESSABLE_ENTITY;
        return ResponseEntity.status(status).body(response);
    }
}
