package com.example.sportsbook.simulator.feed;

import org.junit.jupiter.api.Test;

import com.example.sportsbook.simulator.feed.SequenceReservation.Persisted;

import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SequenceReservationTest {

    @Test
    void firstRunStartsFromTheClock() {
        SequenceReservation reservation = SequenceReservation.startingAt(5_000, Optional.empty(), 100);

        assertThat(reservation.start()).isEqualTo(5_000);
        assertThat(reservation.reservedUpTo()).isEqualTo(5_100);
    }

    @Test
    void restartAfterAClockRollbackStillStartsAboveEverythingPreviouslyReserved() {
        // Previous run reserved up to 9_000; the clock has since stepped back to 5_000.
        SequenceReservation restarted = SequenceReservation.startingAt(5_000, Optional.of(new Persisted(1, 9_000)), 100);

        assertThat(restarted.start()).isEqualTo(9_000);
    }

    @Test
    void restartAfterMoreMessagesThanMillisecondsStillMovesForward() {
        SequenceReservation first = SequenceReservation.startingAt(1_000, Optional.empty(), 100);
        // A burst: 250 messages in the same millisecond, persisting reservations as it goes.
        long persisted = first.reservedUpTo();
        long base = first.start();
        for (long sequence = base + 1; sequence <= base + 250; sequence++) {
            OptionalLong extended = first.extendIfNeeded(sequence);
            if (extended.isPresent()) {
                persisted = extended.getAsLong();
            }
        }

        // Restarted in the same millisecond: the clock alone would replay 1_001..1_250.
        SequenceReservation restarted = SequenceReservation.startingAt(1_000, Optional.of(new Persisted(1, persisted)), 100);

        assertThat(restarted.start()).isGreaterThan(1_250);
    }

    @Test
    void nextBlockIsReservedOnlyOnceHalfTheCurrentOneIsUsed() {
        SequenceReservation reservation = SequenceReservation.startingAt(0, Optional.empty(), 100);

        assertThat(reservation.extendIfNeeded(49)).isEmpty();
        assertThat(reservation.extendIfNeeded(50)).hasValue(200);
        assertThat(reservation.extendIfNeeded(51)).isEmpty();
    }

    // FIX 4: the reservation carries the provider session epoch

    @Test
    void firstStartBeginsWithEpochOne() {
        assertThat(SequenceReservation.startingAt(5_000, Optional.empty(), 100).epoch()).isEqualTo(1);
    }

    @Test
    void restartKeepsThePersistedEpoch() {
        SequenceReservation restarted = SequenceReservation.startingAt(5_000, Optional.of(new Persisted(3, 9_000)), 100);

        assertThat(restarted.epoch()).isEqualTo(3);
        assertThat(restarted.start()).isEqualTo(9_000);
    }

    @Test
    void oldSingleNumberFormatReadsAsEpochOne() {
        assertThat(SequenceReservation.parse("9000")).isEqualTo(new Persisted(1, 9_000));
        // As the current Docker volume holds it, trailing newline and all.
        assertThat(SequenceReservation.parse("1790614709947\n")).isEqualTo(new Persisted(1, 1_790_614_709_947L));
    }

    @Test
    void formatAndParseRoundTrip() {
        assertThat(SequenceReservation.parse(SequenceReservation.format(4, 123))).isEqualTo(new Persisted(4, 123));
    }

    @Test
    void nextSessionIncrementsTheEpochAndRestartsTheSequence() {
        SequenceReservation next = SequenceReservation.startingAt(5_000, Optional.of(new Persisted(2, 9_000)), 100)
                .nextSession(100);

        assertThat(next.epoch()).isEqualTo(3);
        assertThat(next.start()).isZero();
        assertThat(next.reservedUpTo()).isEqualTo(100);
    }

    @Test
    void malformedFileIsRejected() {
        assertThatThrownBy(() -> SequenceReservation.parse("1:2:3")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SequenceReservation.parse("abc")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SequenceReservation.parse("")).isInstanceOf(IllegalArgumentException.class);
    }
}
