package com.example.sportsbook.feed.provider;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProviderStreamClientTest {

    @Test
    void withNothingDeliveredYetItAsksForEverythingTheProviderStillHolds() {
        assertThat(ProviderStreamClient.resumeUrl("ws://p:8086/provider/stream", -1))
                .isEqualTo("ws://p:8086/provider/stream?fromSequence=0");
    }

    @Test
    void reconnectAsksForTheSequenceAfterTheLastOneProcessed() {
        assertThat(ProviderStreamClient.resumeUrl("ws://p:8086/provider/stream", 1000))
                .isEqualTo("ws://p:8086/provider/stream?fromSequence=1001");
    }

    @Test
    void existingQueryParametersAreKept() {
        assertThat(ProviderStreamClient.resumeUrl("ws://p/stream?token=abc", 41))
                .isEqualTo("ws://p/stream?token=abc&fromSequence=42");
    }
}
