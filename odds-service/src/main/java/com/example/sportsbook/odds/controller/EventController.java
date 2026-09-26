package com.example.sportsbook.odds.controller;

import com.example.sportsbook.odds.dto.EventSnapshotResponse;
import com.example.sportsbook.odds.service.EventSnapshotService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final EventSnapshotService snapshotService;

    public EventController(EventSnapshotService snapshotService) {
        this.snapshotService = snapshotService;
    }

    @GetMapping("/{eventId}")
    public EventSnapshotResponse getEvent(@PathVariable String eventId) {
        return snapshotService.snapshot(eventId);
    }
}
