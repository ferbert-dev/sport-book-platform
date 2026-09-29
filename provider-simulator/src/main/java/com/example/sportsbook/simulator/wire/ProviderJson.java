package com.example.sportsbook.simulator.wire;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/** Wire encoding: one {@link ProviderMessage} per WebSocket text frame, ISO-8601 timestamps. */
public final class ProviderJson {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private ProviderJson() {
    }

    public static String encode(ProviderMessage message) {
        try {
            return MAPPER.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not encode provider message " + message.sequenceNumber(), e);
        }
    }
}
