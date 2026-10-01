package com.example.sportsbook.feed.provider;

import org.junit.jupiter.api.Test;

import static com.example.sportsbook.feed.provider.ProviderVersion.EPOCH_FACTOR;
import static com.example.sportsbook.feed.provider.ProviderVersion.compose;
import static com.example.sportsbook.feed.provider.ProviderVersion.epochOf;
import static com.example.sportsbook.feed.provider.ProviderVersion.sequenceOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/*
 * FIX 4 — test plan for ProviderVersion.
 *
 * WHAT IS UNDER TEST
 *   ProviderVersion turns a provider (sessionEpoch, sequenceNumber) pair into ONE long version for the
 *   projection's VersionGuard, and back:
 *       compose(epoch, sequence) = epoch * EPOCH_FACTOR + sequence        (EPOCH_FACTOR = 10^15)
 *       epochOf(version)         = version / EPOCH_FACTOR
 *       sequenceOf(version)      = version % EPOCH_FACTOR
 *   The whole point: when the provider restarts its numbering at 1 in a new session, versions must still
 *   grow, or VersionGuard drops every new event as stale.
 *
 * Every test below checks exactly ONE behaviour, so a red test names the broken rule by itself.
 *
 * A. ORDERING — the property FIX 4 exists for
 *   A1 anyVersionOfANewSessionIsHigherThanEveryVersionOfTheOldOne
 *        compose(2, 1) > compose(1, EPOCH_FACTOR - 1)
 *        The FIRST message of the next session outranks the LAST possible message of the previous one.
 *   A2 laterMessageInTheSameSessionHasAHigherVersion
 *        compose(1, 6) > compose(1, 5)
 *        Inside one session the version follows the sequence, as it did before FIX 4.
 *   A3 sameEpochAndSequenceAlwaysGiveTheSameVersion
 *        compose(3, 77) == compose(3, 77)
 *        Deterministic: a replayed message gets the SAME version, so redelivery stays idempotent
 *        downstream (version guard, settlement fence). This is why the worker does not invent versions.
 *
 * B. ROUND TRIP — compose and decompose are inverses
 *   B1 decomposingReturnsWhatWasComposed
 *        epochOf(compose(7, 42)) == 7 and sequenceOf(compose(7, 42)) == 42
 *   B2 roundTripHoldsAtTheEdgesOfTheRange
 *        compose(0, 0) == 0                    -> epochOf 0, sequenceOf 0   (smallest valid version)
 *        compose(1, EPOCH_FACTOR - 1)          -> epochOf 1, sequenceOf EPOCH_FACTOR - 1
 *        compose(9_222, EPOCH_FACTOR - 1)      -> largest epoch whose whole sequence range fits in a long
 *                                                 (9_222_999_999_999_999_999 < Long.MAX_VALUE; 9_223 would not)
 *        Zero is valid, and the last sequence of a session does not spill into the next epoch.
 *
 * C. INVALID INPUT — rejected loudly, never turned into a wrong version
 *   C1 negativeSequenceIsRejected
 *        compose(1, -1) throws IllegalArgumentException
 *        Otherwise it would land in epoch 0 (compose(1, -1) == 999...999) and look older than it is.
 *   C2 sequenceAtOrAboveTheEpochFactorIsRejected
 *        compose(1, EPOCH_FACTOR) throws IllegalArgumentException, message mentions "sequence"
 *        Otherwise it would read as epoch 2, sequence 0 — a message of session 1 posing as session 2.
 *        Only the sequence is invalid here; the epoch is a normal 1.
 *   C3 negativeEpochIsRejected
 *        compose(-1, 5) throws IllegalArgumentException, message mentions "epoch"
 *   C4 epochTooLargeOverflowsLoudlyInsteadOfGoingNegative
 *        compose(10_000, 1) throws ArithmeticException
 *        10_000 * 10^15 does not fit in a long; Math.multiplyExact must stop it instead of
 *        silently producing a negative version.
 *   C5 negativeVersionCannotBeDecomposed
 *        epochOf(-1) and sequenceOf(-1) throw IllegalArgumentException
 *        No valid version is negative, so a negative one is a bug upstream, not data.
 */
class ProviderVersionTest {

    // A. Ordering

    @Test
    void anyVersionOfANewSessionIsHigherThanEveryVersionOfTheOldOne() {
        assertThat(compose(2, 1)).isGreaterThan(compose(1, EPOCH_FACTOR - 1));
    }

    @Test
    void laterMessageInTheSameSessionHasAHigherVersion() {
        assertThat(compose(1, 6)).isGreaterThan(compose(1, 5));
    }

    @Test
    void sameEpochAndSequenceAlwaysGiveTheSameVersion() {
        assertThat(compose(3, 77)).isEqualTo(compose(3, 77));
    }

    // B. Round trip

    @Test
    void decomposingReturnsWhatWasComposed() {
        long version = compose(7, 42);

        assertThat(epochOf(version)).isEqualTo(7);
        assertThat(sequenceOf(version)).isEqualTo(42);
    }

    @Test
    void roundTripHoldsAtTheEdgesOfTheRange() {
        assertThat(compose(0, 0)).isZero();

        long lastOfSessionOne = compose(1, EPOCH_FACTOR - 1);
        assertThat(epochOf(lastOfSessionOne)).isEqualTo(1);
        assertThat(sequenceOf(lastOfSessionOne)).isEqualTo(EPOCH_FACTOR - 1);

        long largest = compose(9_222, EPOCH_FACTOR - 1);
        assertThat(largest).isLessThan(Long.MAX_VALUE);
        assertThat(epochOf(largest)).isEqualTo(9_222);
        assertThat(sequenceOf(largest)).isEqualTo(EPOCH_FACTOR - 1);
    }

    // C. Invalid input

    @Test
    void negativeSequenceIsRejected() {
        assertThatThrownBy(() -> compose(1, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sequence");
    }

    @Test
    void sequenceAtOrAboveTheEpochFactorIsRejected() {
        assertThatThrownBy(() -> compose(1, EPOCH_FACTOR))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sequence");
    }

    @Test
    void negativeEpochIsRejected() {
        assertThatThrownBy(() -> compose(-1, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("epoch");
    }

    @Test
    void epochTooLargeOverflowsLoudlyInsteadOfGoingNegative() {
        assertThatThrownBy(() -> compose(10_000, 1)).isInstanceOf(ArithmeticException.class);
    }

    @Test
    void negativeVersionCannotBeDecomposed() {
        assertThatThrownBy(() -> epochOf(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sequenceOf(-1)).isInstanceOf(IllegalArgumentException.class);
    }
}
