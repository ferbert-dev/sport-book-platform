package com.example.sportsbook.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Shared Jackson configuration, so the non-Spring Vert.x modules serialize events
 * byte-for-byte the same way the Spring modules do.
 */
public final class SportsbookJson {

    private static final ObjectMapper MAPPER = create();

    private SportsbookJson() {
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static ObjectMapper create() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                // ISO-8601 strings, not epoch numbers, so events stay human-readable on the topic.
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }
}
