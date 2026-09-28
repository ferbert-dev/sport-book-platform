package com.example.sportsbook.simulator.feed;

import com.example.sportsbook.simulator.wire.ProviderMessage;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class ProviderFeedTest {

    private static final Instant NOW = Instant.parse("2026-09-28T12:00:00Z");

    private final ProviderFeed feed = new ProviderFeed(500L, Clock.fixed(NOW, ZoneOffset.UTC));

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

        ProviderFeed.Replay replay = feed.replayFrom(502L);

        assertThat(replay.complete()).isTrue();
        assertThat(replay.messages()).extracting(ProviderMessage::sequenceNumber).containsExactly(502L, 503L);
    }

    @Test
    void replayWindowIsBoundedAndReportsWhatItCanNoLongerReplay() {
        ProviderFeed small = new ProviderFeed(0L, Clock.fixed(NOW, ZoneOffset.UTC), 2);
        small.emit(ProviderMessage.matchStart("event-1"));
        small.emit(ProviderMessage.matchStart("event-2"));
        small.emit(ProviderMessage.matchStart("event-3"));

        ProviderFeed.Replay replay = small.replayFrom(1L);

        assertThat(replay.complete()).isFalse();
        assertThat(replay.messages()).extracting(ProviderMessage::sequenceNumber).containsExactly(2L, 3L);
    }

    @Test
    void cursorAtTheHeadReplaysNothingAndIsComplete() {
        feed.emit(ProviderMessage.matchStart("event-1"));

        ProviderFeed.Replay replay = feed.replayFrom(502L);

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
}
