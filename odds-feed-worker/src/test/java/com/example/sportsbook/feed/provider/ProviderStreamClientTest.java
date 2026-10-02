package com.example.sportsbook.feed.provider;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProviderStreamClientTest {

    @Test
    void withNothingDeliveredYetItAsksForEverythingTheProviderStillHolds() {
        assertThat(ProviderStreamClient.resumeUrl("ws://p:8086/provider/stream", -1, -1))
                .isEqualTo("ws://p:8086/provider/stream?sessionEpoch=0&fromSequence=0");
    }

    @Test
    void reconnectAsksForTheSequenceAfterTheLastOneProcessedInItsSession() {
        assertThat(ProviderStreamClient.resumeUrl("ws://p:8086/provider/stream", 2, 1000))
                .isEqualTo("ws://p:8086/provider/stream?sessionEpoch=2&fromSequence=1001");
    }

    @Test
    void existingQueryParametersAreKept() {
        assertThat(ProviderStreamClient.resumeUrl("ws://p/stream?token=abc", 1, 41))
                .isEqualTo("ws://p/stream?token=abc&sessionEpoch=1&fromSequence=42");
    }

    @Test
    void cursorReadFromKafkaSplitsBackIntoEpochAndSequence() {
        // What OddsFeedVerticle does at startup with the highest version on sports-events.
        long lastPublishedVersion = ProviderVersion.compose(3, 77);

        assertThat(ProviderStreamClient.resumeUrl("ws://p/stream",
                ProviderVersion.epochOf(lastPublishedVersion), ProviderVersion.sequenceOf(lastPublishedVersion)))
                .isEqualTo("ws://p/stream?sessionEpoch=3&fromSequence=78");
    }
}
