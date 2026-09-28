package com.example.sportsbook.simulator.feed;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

class SequenceReservationTest {

    @Test
    void firstRunStartsFromTheClock() {
        SequenceReservation reservation = SequenceReservation.startingAt(5_000, OptionalLong.empty(), 100);

        assertThat(reservation.start()).isEqualTo(5_000);
        assertThat(reservation.reservedUpTo()).isEqualTo(5_100);
    }

    @Test
    void restartAfterAClockRollbackStillStartsAboveEverythingPreviouslyReserved() {
        // Previous run reserved up to 9_000; the clock has since stepped back to 5_000.
        SequenceReservation restarted = SequenceReservation.startingAt(5_000, OptionalLong.of(9_000), 100);

        assertThat(restarted.start()).isEqualTo(9_000);
    }

    @Test
    void restartAfterMoreMessagesThanMillisecondsStillMovesForward() {
        SequenceReservation first = SequenceReservation.startingAt(1_000, OptionalLong.empty(), 100);
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
        SequenceReservation restarted = SequenceReservation.startingAt(1_000, OptionalLong.of(persisted), 100);

        assertThat(restarted.start()).isGreaterThan(1_250);
    }

    @Test
    void nextBlockIsReservedOnlyOnceHalfTheCurrentOneIsUsed() {
        SequenceReservation reservation = SequenceReservation.startingAt(0, OptionalLong.empty(), 100);

        assertThat(reservation.extendIfNeeded(49)).isEmpty();
        assertThat(reservation.extendIfNeeded(50)).hasValue(200);
        assertThat(reservation.extendIfNeeded(51)).isEmpty();
    }
}
