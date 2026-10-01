package com.example.sportsbook.feed.provider;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SequenceValidatorTest {

    private final SequenceValidator validator = new SequenceValidator();

    @Test
    void firstMessageEstablishesTheBaselineWhateverItsSequence() {
        assertThat(validator.evaluate(0, 100)).isEqualTo(SequenceDecision.IN_ORDER);
        assertThat(validator.lastProcessedSequence()).isEqualTo(100);
    }

    @Test
    void consecutiveSequencesAreInOrder() {
        validator.evaluate(0, 100);

        assertThat(validator.evaluate(0, 101)).isEqualTo(SequenceDecision.IN_ORDER);
        assertThat(validator.evaluate(0, 102)).isEqualTo(SequenceDecision.IN_ORDER);
        assertThat(validator.evaluate(0, 103)).isEqualTo(SequenceDecision.IN_ORDER);
    }

    @Test
    void replayedSequenceIsReportedAsDuplicate() {
        validator.evaluate(0, 100);
        validator.evaluate(0, 101);

        assertThat(validator.evaluate(0, 101)).isEqualTo(SequenceDecision.DUPLICATE);
        assertThat(validator.evaluate(0, 100)).isEqualTo(SequenceDecision.DUPLICATE);
    }

    @Test
    void duplicateDoesNotRewindTheBaseline() {
        validator.evaluate(0, 100);
        validator.evaluate(0, 105);

        validator.evaluate(0, 101);

        assertThat(validator.lastProcessedSequence()).isEqualTo(105);
    }

    @Test
    void missingSequenceIsReportedAsGapAndStillAdvancesTheBaseline() {
        validator.evaluate(0, 100);
        validator.evaluate(0, 101);

        // 102 never arrived.
        assertThat(validator.evaluate(0, 103)).isEqualTo(SequenceDecision.GAP);
        assertThat(validator.lastProcessedSequence()).isEqualTo(103);
        assertThat(validator.evaluate(0, 104)).isEqualTo(SequenceDecision.IN_ORDER);
    }

    @Test
    void resetAfterSnapshotRecoveryRebaselinesTheStream() {
        validator.evaluate(0, 100);

        validator.resetTo(0, 500);

        assertThat(validator.evaluate(0, 501)).isEqualTo(SequenceDecision.IN_ORDER);
    }

    @Test
    void decideDoesNotRecordSoAFailedPublishIsAcceptedAgainOnReplay() {
        validator.markProcessed(0, 100);

        // 101 arrives, its publish fails: decided but never marked.
        assertThat(validator.decide(0, 101)).isEqualTo(SequenceDecision.IN_ORDER);

        // Replayed after reconnect: still in order, not a duplicate.
        assertThat(validator.decide(0, 101)).isEqualTo(SequenceDecision.IN_ORDER);
        assertThat(validator.lastProcessedSequence()).isEqualTo(100);
    }

    @Test
    void markProcessedNeverMovesTheCursorBackwards() {
        validator.markProcessed(0, 200);
        validator.markProcessed(0, 150);

        assertThat(validator.lastProcessedSequence()).isEqualTo(200);
    }

    // FIX 4: provider sessions (epoch)

    @Test
    void firstMessageOfAnyEpochIsInOrder() {
        assertThat(validator.decide(3, 50)).isEqualTo(SequenceDecision.IN_ORDER);
    }

    @Test
    void higherEpochIsANewSessionEvenWithALowerSequence() {
        validator.markProcessed(1, 5000);

        assertThat(validator.decide(2, 1)).isEqualTo(SequenceDecision.NEW_SESSION);
    }

    @Test
    void lowerEpochIsAStaleSession() {
        validator.markProcessed(2, 1);

        assertThat(validator.decide(1, 10_000)).isEqualTo(SequenceDecision.STALE_SESSION);
    }

    @Test
    void withinOneEpochDuplicatesAndGapsWorkAsBefore() {
        validator.markProcessed(2, 1);

        assertThat(validator.decide(2, 1)).isEqualTo(SequenceDecision.DUPLICATE);
        assertThat(validator.decide(2, 2)).isEqualTo(SequenceDecision.IN_ORDER);
        assertThat(validator.decide(2, 5)).isEqualTo(SequenceDecision.GAP);
    }

    @Test
    void markingANewEpochResetsTheSequence() {
        validator.markProcessed(1, 5000);
        validator.markProcessed(2, 1);

        assertThat(validator.decide(2, 2)).isEqualTo(SequenceDecision.IN_ORDER);
        assertThat(validator.lastProcessedEpoch()).isEqualTo(2);
    }

    @Test
    void markingAnOlderEpochIsIgnored() {
        validator.markProcessed(2, 10);
        validator.markProcessed(1, 1999);

        assertThat(validator.lastProcessedEpoch()).isEqualTo(2);
        assertThat(validator.lastProcessedSequence()).isEqualTo(10);
    }

    @Test
    void evaluateRecordsTheEpoch() {
        validator.evaluate(2, 10);

        assertThat(validator.lastProcessedEpoch()).isEqualTo(2);
    }

    @Test
    void staleMessageDoesNotMoveTheCursor() {
        validator.evaluate(2, 10);

        assertThat(validator.evaluate(1, 9999)).isEqualTo(SequenceDecision.STALE_SESSION);
        // The point of the test: a late message of an old session must not poison the cursor,
        // or real messages 11..9999 of session 2 would be dropped as duplicates.
        assertThat(validator.lastProcessedSequence()).isEqualTo(10);
    }

    @Test
    void decideNeverChangesState() {
        validator.markProcessed(2, 10);

        validator.decide(2, 100);
        validator.decide(2, 100);

        assertThat(validator.lastProcessedEpoch()).isEqualTo(2);
        assertThat(validator.lastProcessedSequence()).isEqualTo(10);
    }
}
