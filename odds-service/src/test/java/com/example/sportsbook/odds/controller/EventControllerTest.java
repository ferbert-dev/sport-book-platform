package com.example.sportsbook.odds.controller;

import com.example.sportsbook.common.EventStatus;
import com.example.sportsbook.common.MarketStatus;
import com.example.sportsbook.odds.dto.EventSnapshotResponse;
import com.example.sportsbook.odds.dto.MarketSnapshot;
import com.example.sportsbook.odds.dto.SelectionSnapshot;
import com.example.sportsbook.odds.exception.EventNotFoundException;
import com.example.sportsbook.odds.service.EventSnapshotService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventController.class)
class EventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private EventSnapshotService snapshotService;

    @Test
    void returnsTheSnapshotShapeTheApiContractPromises() throws Exception {
        when(snapshotService.snapshot("event-123")).thenReturn(new EventSnapshotResponse(
                "event-123", EventStatus.LIVE,
                List.of(new MarketSnapshot("market-456", MarketStatus.ACTIVE, 1003,
                        Instant.parse("2026-09-26T10:00:00Z"),
                        List.of(new SelectionSnapshot("real-madrid", new BigDecimal("2.10")),
                                new SelectionSnapshot("draw", new BigDecimal("3.40")))))));

        mockMvc.perform(get("/api/v1/events/event-123"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.eventId").value("event-123"))
                .andExpect(jsonPath("$.status").value("LIVE"))
                .andExpect(jsonPath("$.markets[0].marketId").value("market-456"))
                .andExpect(jsonPath("$.markets[0].status").value("ACTIVE"))
                .andExpect(jsonPath("$.markets[0].selections[0].selectionId").value("real-madrid"))
                .andExpect(jsonPath("$.markets[0].selections[0].odds").value(2.10))
                .andExpect(jsonPath("$.markets[0].selections[1].odds").value(3.40));
    }

    @Test
    void unknownEventReturns404WithAStructuredError() throws Exception {
        when(snapshotService.snapshot(any())).thenThrow(new EventNotFoundException("missing"));

        mockMvc.perform(get("/api/v1/events/missing"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("EVENT_NOT_FOUND"));
    }
}
