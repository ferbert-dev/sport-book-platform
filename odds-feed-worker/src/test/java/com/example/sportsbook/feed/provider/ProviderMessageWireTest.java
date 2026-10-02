package com.example.sportsbook.feed.provider;

import com.example.sportsbook.common.SportsbookJson;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * FIX 4: the provider/worker contract is the JSON on the wire. These tests pin the worker's side of
 * it; the simulator's ProviderFeedTest pins the provider's side (the key it writes).
 */
class ProviderMessageWireTest {

    private static final String WITH_EPOCH = """
            {"sessionEpoch":2,"sequenceNumber":5,"messageType":"MATCH_START",
             "matchId":"e1","sentAt":"2026-10-01T10:00:00Z"}""";

    private static final String WITHOUT_EPOCH = """
            {"sequenceNumber":5,"messageType":"MATCH_START",
             "matchId":"e1","sentAt":"2026-10-01T10:00:00Z"}""";

    @Test
    void sessionEpochIsReadFromTheWire() throws Exception {
        ProviderMessage message = SportsbookJson.mapper().readValue(WITH_EPOCH, ProviderMessage.class);

        assertThat(message.sessionEpoch()).isEqualTo(2);
        assertThat(message.sequenceNumber()).isEqualTo(5);
    }

    @Test
    void missingSessionEpochReadsAsZero() throws Exception {
        ProviderMessage message = SportsbookJson.mapper().readValue(WITHOUT_EPOCH, ProviderMessage.class);

        assertThat(message.sessionEpoch()).isZero();
    }
}
