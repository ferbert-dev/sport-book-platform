package com.example.sportsbook.simulator.feed;

import com.example.sportsbook.simulator.wire.ProviderJson;
import com.example.sportsbook.simulator.wire.ProviderMessage;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class ProviderFeedTest {

    private static final Instant NOW = Instant.parse("2026-09-28T12:00:00Z");

    private final ProviderFeed feed = new ProviderFeed(500L, Clock.fixed(NOW, ZoneOffset.UTC), 1L);

    @Test
    void emitStampsTheNextSequenceAndTheClockTime() {
        ProviderMessage sent = feed.emit(ProviderMessage.matchStart("event-1"));

        assertThat(sent.sequenceNumber()).isEqualTo(501L);
        assertThat(sent.sentAt()).isEqualTo(NOW);
        assertThat(feed.lastSequence()).isEqualTo(501L);
    }

    @Test
    void sequenceIsContiguousAcrossDifferentMatches() {
        TestSubscriber<ProviderMessage> subscriber = feed.messages().test();

        feed.emit(ProviderMessage.matchStart("event-1"));
        feed.emit(ProviderMessage.matchStart("event-2"));
        feed.emit(ProviderMessage.marketLock("event-1", "market-1"));

        assertThat(subscriber.values()).extracting(ProviderMessage::sequenceNumber)
                .containsExactly(501L, 502L, 503L);
    }

    @Test
    void resendLastRepeatsThePreviousMessageAsADuplicate() {
        TestSubscriber<ProviderMessage> subscriber = feed.messages().test();

        feed.emit(ProviderMessage.matchStart("event-1"));
        feed.resendLast();

        assertThat(subscriber.values()).hasSize(2);
        assertThat(subscriber.values().get(1)).isEqualTo(subscriber.values().get(0));
    }

    @Test
    void skipSequenceLeavesAGapInTheNumbering() {
        TestSubscriber<ProviderMessage> subscriber = feed.messages().test();

        feed.emit(ProviderMessage.matchStart("event-1"));
        feed.skipSequence();
        feed.emit(ProviderMessage.matchEnd("event-1"));

        assertThat(subscriber.values()).extracting(ProviderMessage::sequenceNumber)
                .containsExactly(501L, 503L);
    }

    @Test
    void everyEmittedMessageCarriesTheFeedsSessionEpoch() {
        ProviderFeed epochThree = new ProviderFeed(0L, Clock.fixed(NOW, ZoneOffset.UTC), 3L);

        assertThat(epochThree.emit(ProviderMessage.matchStart("event-1")).sessionEpoch()).isEqualTo(3L);
    }

    @Test
    void sessionEpochIsSentUnderTheKeyTheWorkerReads() {
        // The provider/worker contract is the JSON, not a shared class: if the key here ever differs
        // from the worker's ProviderMessage.sessionEpoch, the worker silently reads 0.
        String json = ProviderJson.encode(feed.emit(ProviderMessage.matchStart("event-1")));

        assertThat(json).contains("\"sessionEpoch\":");
    }

    @Test
    void failsClosedInsteadOfEmittingPastTheDurableReservation() {
        feed.setEmitLimit(502L);
        feed.emit(ProviderMessage.matchStart("event-1"));
        feed.emit(ProviderMessage.matchEnd("event-1"));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> feed.emit(ProviderMessage.matchStart("event-2")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SEQUENCE_RESERVATION_EXHAUSTED");
        assertThat(feed.lastSequence()).isEqualTo(502L);

        feed.setEmitLimit(600L);
        assertThat(feed.emit(ProviderMessage.matchStart("event-2")).sequenceNumber()).isEqualTo(503L);
    }

    @Test
    void replayReturnsEverythingFromTheCursorOnInOrder() {
        feed.emit(ProviderMessage.matchStart("event-1"));
        feed.emit(ProviderMessage.marketUnlock("event-1", "market-1"));
        feed.emit(ProviderMessage.marketResult("event-1", "market-1", "home"));

        ProviderFeed.Replay replay = feed.replayFrom(1L, 502L);

        assertThat(replay.complete()).isTrue();
        assertThat(replay.messages()).extracting(ProviderMessage::sequenceNumber).containsExactly(502L, 503L);
    }

    @Test
    void replayWindowIsBoundedAndReportsWhatItCanNoLongerReplay() {
        ProviderFeed small = new ProviderFeed(0L, Clock.fixed(NOW, ZoneOffset.UTC), 2, 1L);
        small.emit(ProviderMessage.matchStart("event-1"));
        small.emit(ProviderMessage.matchStart("event-2"));
        small.emit(ProviderMessage.matchStart("event-3"));

        ProviderFeed.Replay replay = small.replayFrom(1L, 1L);

        assertThat(replay.complete()).isFalse();
        assertThat(replay.messages()).extracting(ProviderMessage::sequenceNumber).containsExactly(2L, 3L);
    }

    @Test
    void cursorAtTheHeadReplaysNothingAndIsComplete() {
        feed.emit(ProviderMessage.matchStart("event-1"));

        ProviderFeed.Replay replay = feed.replayFrom(1L, 502L);

        assertThat(replay.complete()).isTrue();
        assertThat(replay.messages()).isEmpty();
    }

    @Test
    void messagesEmittedWithoutASubscriberAreNotPushedToLateSubscribers() {
        feed.emit(ProviderMessage.matchStart("event-1"));

        TestSubscriber<ProviderMessage> late = feed.messages().test();
        feed.emit(ProviderMessage.matchEnd("event-1"));

        assertThat(late.values()).extracting(ProviderMessage::sequenceNumber).containsExactly(502L);
    }

    // FIX 4: provider sessions

    @Test
    void newSessionRestartsNumberingAtOne() {
        feed.emit(ProviderMessage.matchStart("event-1"));

        feed.startNewSession(2);
        ProviderMessage first = feed.emit(ProviderMessage.matchStart("event-2"));

        assertThat(first.sessionEpoch()).isEqualTo(2);
        assertThat(first.sequenceNumber()).isEqualTo(1);
    }

    @Test
    void newSessionMustHaveAHigherEpoch() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> feed.startNewSession(1))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> feed.startNewSession(0))
                .isInstanceOf(IllegalArgumentException.class);

        // Still numbering the old session.
        ProviderMessage next = feed.emit(ProviderMessage.matchStart("event-1"));
        assertThat(next.sessionEpoch()).isEqualTo(1);
        assertThat(next.sequenceNumber()).isEqualTo(501L);
    }

    @Test
    void replayCrossesTheSessionBoundary() {
        ProviderFeed twoSessions = twoSessions(10_000);

        ProviderFeed.Replay replay = twoSessions.replayFrom(1, 2);

        assertThat(replay.complete()).isTrue();
        assertThat(positions(replay)).containsExactly("1:2", "1:3", "2:1", "2:2");
    }

    @Test
    void replayFromTheNewSessionSkipsTheOldOne() {
        assertThat(positions(twoSessions(10_000).replayFrom(2, 1))).containsExactly("2:1", "2:2");
    }

    @Test
    void replayOfAnOldSessionThatLeftTheBufferIsIncomplete() {
        // Capacity 2: of (1,1) (1,2) (1,3) (2,1) (2,2) only the last two are kept.
        ProviderFeed.Replay replay = twoSessions(2).replayFrom(1, 1);

        assertThat(replay.complete()).isFalse();
        assertThat(positions(replay)).containsExactly("2:1", "2:2");
    }

    @Test
    void connectedSubscribersReceiveTheNewSessionWithoutReconnecting() {
        TestSubscriber<ProviderMessage> subscriber = feed.messages().test();

        feed.emit(ProviderMessage.matchStart("event-1"));
        feed.startNewSession(2);
        feed.emit(ProviderMessage.matchStart("event-2"));

        assertThat(subscriber.values()).extracting(ProviderMessage::sessionEpoch).containsExactly(1L, 2L);
    }

    /** Session 1 emits sequences 1..3, then session 2 emits 1..2. */
    private static ProviderFeed twoSessions(int replayCapacity) {
        ProviderFeed twoSessions = new ProviderFeed(0L, Clock.fixed(NOW, ZoneOffset.UTC), replayCapacity, 1L);
        twoSessions.emit(ProviderMessage.matchStart("event-1"));
        twoSessions.emit(ProviderMessage.marketUnlock("event-1", "market-1"));
        twoSessions.emit(ProviderMessage.matchEnd("event-1"));
        twoSessions.startNewSession(2);
        twoSessions.emit(ProviderMessage.matchStart("event-2"));
        twoSessions.emit(ProviderMessage.marketUnlock("event-2", "market-2"));
        return twoSessions;
    }

    private static java.util.List<String> positions(ProviderFeed.Replay replay) {
        return replay.messages().stream()
                .map(message -> message.sessionEpoch() + ":" + message.sequenceNumber())
                .toList();
    }
}
