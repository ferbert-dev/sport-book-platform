package com.example.sportsbook.feed.provider;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SimulatedSportsProviderTest {

    private final SimulatedSportsProvider provider =
            new SimulatedSportsProvider("event-123", "market-456", Duration.ofMillis(1));

    @Test
    void scriptCoversTheFullMatchLifecycleEndingInSettlement() {
        List<String> types = provider.script().stream().map(ProviderMessage::messageType).toList();

        assertThat(types).startsWith("MATCH_START");
        assertThat(types).endsWith("MARKET_RESULT");
        assertThat(types).contains("MARKET_LOCK", "MARKET_UNLOCK", "MATCH_END");
    }

    @Test
    void scriptDeliberatelyContainsOneDuplicateSoTheDedupePathIsExercised() {
        SequenceValidator validator = new SequenceValidator();

        long duplicates = provider.script().stream()
                .map(message -> validator.evaluate(message.sequenceNumber()))
                .filter(decision -> decision == SequenceDecision.DUPLICATE)
                .count();

        assertThat(duplicates).isEqualTo(1);
    }

    @Test
    void scriptDeliberatelyContainsOneGapSoTheResyncPathIsExercised() {
        SequenceValidator validator = new SequenceValidator();

        long gaps = provider.script().stream()
                .map(message -> validator.evaluate(message.sequenceNumber()))
                .filter(decision -> decision == SequenceDecision.GAP)
                .count();

        assertThat(gaps).isEqualTo(1);
    }

    @Test
    void everyScriptedMessageIsStructurallyValid() {
        assertThat(provider.script()).allMatch(ProviderMessageValidator::isValid);
    }

    @Test
    void everyScriptedMessageNormalizesToADomainEvent() {
        assertThat(provider.script())
                .allSatisfy(message -> assertThat(MessageNormalizer.normalize(message)).isPresent());
    }

    @Test
    void streamEmitsTheScriptInOrderAndThenLoops() {
        List<ProviderMessage> emitted = provider.messages()
                .take(provider.script().size() + 2L)
                .toList()
                .blockingGet();

        assertThat(emitted).hasSize(provider.script().size() + 2);
        assertThat(emitted.get(0).messageType()).isEqualTo("MATCH_START");
        // Wrapped around to the start of the script.
        assertThat(emitted.get(provider.script().size()).messageType()).isEqualTo("MATCH_START");
    }
}
