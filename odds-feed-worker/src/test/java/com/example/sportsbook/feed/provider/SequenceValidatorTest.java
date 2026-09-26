package com.example.sportsbook.feed.provider;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SequenceValidatorTest {

    private final SequenceValidator validator = new SequenceValidator();

    @Test
    void firstMessageEstablishesTheBaselineWhateverItsSequence() {
        assertThat(validator.evaluate(100)).isEqualTo(SequenceDecision.IN_ORDER);
        assertThat(validator.lastProcessedSequence()).isEqualTo(100);
    }

    @Test
    void consecutiveSequencesAreInOrder() {
        validator.evaluate(100);

        assertThat(validator.evaluate(101)).isEqualTo(SequenceDecision.IN_ORDER);
        assertThat(validator.evaluate(102)).isEqualTo(SequenceDecision.IN_ORDER);
        assertThat(validator.evaluate(103)).isEqualTo(SequenceDecision.IN_ORDER);
    }

    @Test
    void replayedSequenceIsReportedAsDuplicate() {
        validator.evaluate(100);
        validator.evaluate(101);

        assertThat(validator.evaluate(101)).isEqualTo(SequenceDecision.DUPLICATE);
        assertThat(validator.evaluate(100)).isEqualTo(SequenceDecision.DUPLICATE);
    }

    @Test
    void duplicateDoesNotRewindTheBaseline() {
        validator.evaluate(100);
        validator.evaluate(105);

        validator.evaluate(101);

        assertThat(validator.lastProcessedSequence()).isEqualTo(105);
    }

    @Test
    void missingSequenceIsReportedAsGapAndStillAdvancesTheBaseline() {
        validator.evaluate(100);
        validator.evaluate(101);

        // 102 never arrived.
        assertThat(validator.evaluate(103)).isEqualTo(SequenceDecision.GAP);
        assertThat(validator.lastProcessedSequence()).isEqualTo(103);
        assertThat(validator.evaluate(104)).isEqualTo(SequenceDecision.IN_ORDER);
    }

    @Test
    void resetAfterSnapshotRecoveryRebaselinesTheStream() {
        validator.evaluate(100);

        validator.resetTo(500);

        assertThat(validator.evaluate(501)).isEqualTo(SequenceDecision.IN_ORDER);
    }
}
