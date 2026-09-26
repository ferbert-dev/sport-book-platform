package com.example.sportsbook.feed.provider;

/**
 * Tracks the provider's sequence numbers to detect duplicates and gaps.
 *
 * <p>Not thread-safe by design: it is confined to a single Vert.x verticle, which is
 * single-threaded, so no synchronization is needed.
 */
public class SequenceValidator {

    private static final long NOT_STARTED = -1L;

    private long lastProcessedSequence = NOT_STARTED;

    public SequenceDecision evaluate(long incomingSequence) {
        if (lastProcessedSequence == NOT_STARTED) {
            lastProcessedSequence = incomingSequence;
            return SequenceDecision.IN_ORDER;
        }
        if (incomingSequence <= lastProcessedSequence) {
            return SequenceDecision.DUPLICATE;
        }
        boolean gap = incomingSequence > lastProcessedSequence + 1;
        lastProcessedSequence = incomingSequence;
        return gap ? SequenceDecision.GAP : SequenceDecision.IN_ORDER;
    }

    public long lastProcessedSequence() {
        return lastProcessedSequence;
    }

    /** Called after a simulated snapshot/replay so the validator accepts the recovered stream. */
    public void resetTo(long sequence) {
        this.lastProcessedSequence = sequence;
    }
}
