package com.example.sportsbook.simulator.feed;

import com.example.sportsbook.simulator.wire.ProviderMessage;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class ScriptedMatchTest {

    private final ProviderFeed feed = new ProviderFeed(100L, Clock.systemUTC());
    private final ScriptedMatch scripted = new ScriptedMatch("event-123", "market-456");

    @Test
    void scriptCoversTheFullMatchLifecycleEndingInSettlement() {
        List<String> types = scripted.steps().stream()
                .filter(ScriptedMatch.Step.Send.class::isInstance)
                .map(step -> ((ScriptedMatch.Step.Send) step).draft().messageType())
                .toList();

        assertThat(types).startsWith("MATCH_START");
        assertThat(types).endsWith("MARKET_RESULT");
        assertThat(types).contains("PRICE_CHANGE", "MARKET_LOCK", "MARKET_UNLOCK", "MATCH_END");
    }

    @Test
    void everyCycleInjectsExactlyOneDuplicateAndOneGap() {
        List<ProviderMessage> sent = playCycles(2);

        long duplicates = IntStream.range(1, sent.size())
                .filter(i -> sent.get(i).sequenceNumber() <= sent.get(i - 1).sequenceNumber())
                .count();
        long gaps = IntStream.range(1, sent.size())
                .filter(i -> sent.get(i).sequenceNumber() > sent.get(i - 1).sequenceNumber() + 1)
                .count();

        assertThat(duplicates).isEqualTo(2);
        assertThat(gaps).isEqualTo(2);
    }

    @Test
    void laterCyclesKeepMovingForwardSoTheVersionGuardDoesNotFreeze() {
        int sendsPerCycle = sendsPerCycle();
        List<ProviderMessage> sent = playCycles(2);

        long firstCycleMax = sent.subList(0, sendsPerCycle).stream()
                .mapToLong(ProviderMessage::sequenceNumber).max().orElseThrow();
        long secondCycleMin = sent.subList(sendsPerCycle, sent.size()).stream()
                .mapToLong(ProviderMessage::sequenceNumber).min().orElseThrow();

        assertThat(secondCycleMin).isGreaterThan(firstCycleMax);
    }

    @Test
    void aStepTheFeedRefusesIsRetriedNotSkipped() {
        TestSubscriber<ProviderMessage> sent = feed.messages().test();
        scripted.playNext(feed);                       // MATCH_START
        feed.setEmitLimit(feed.lastSequence());        // no more capacity

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> scripted.playNext(feed))
                .isInstanceOf(IllegalStateException.class);

        feed.setEmitLimit(Long.MAX_VALUE);             // capacity back
        scripted.playNext(feed);

        assertThat(sent.values()).extracting(ProviderMessage::messageType)
                .containsExactly("MATCH_START", "PRICE_CHANGE");
    }

    /** Plays whole cycles; a gap step also plays the following step, so it is not a tick of its own. */
    private List<ProviderMessage> playCycles(int cycles) {
        TestSubscriber<ProviderMessage> subscriber = feed.messages().test();
        long ticksPerCycle = scripted.steps().stream()
                .filter(step -> !(step instanceof ScriptedMatch.Step.Gap)).count();
        for (int i = 0; i < ticksPerCycle * cycles; i++) {
            scripted.playNext(feed);
        }
        return subscriber.values();
    }

    private int sendsPerCycle() {
        return (int) scripted.steps().stream()
                .filter(step -> !(step instanceof ScriptedMatch.Step.Gap)).count();
    }
}
