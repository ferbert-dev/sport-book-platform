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
    private long lastProcessedEpoch = NOT_STARTED;


    /** Decides and marks processed in one step. */
    public SequenceDecision evaluate(long incomingSequence) {
        return evaluate(0L, incomingSequence);
    }
    /** Decides and marks processed in one step. */
    public SequenceDecision evaluate(long epoch, long incomingSequence) {
        SequenceDecision decision = decide(epoch, incomingSequence);

        if (decision != SequenceDecision.DUPLICATE && decision != SequenceDecision.STALE_SESSION) {
            markProcessed(epoch, incomingSequence);
        }
        return decision;
    }

    /**
     * Classifies a sequence without recording it. The worker marks a sequence processed only once
     * Kafka has acknowledged it, so a failed publish is asked for again on reconnect instead of
     * being skipped.
     */
    public SequenceDecision decide(long incomingSequence) {
        if (lastProcessedSequence == NOT_STARTED) {
            return SequenceDecision.IN_ORDER;
        }
        if (incomingSequence <= lastProcessedSequence) {
            return SequenceDecision.DUPLICATE;
        }
        return incomingSequence > lastProcessedSequence + 1 ? SequenceDecision.GAP : SequenceDecision.IN_ORDER;
    }
    public SequenceDecision decide(long incomingEpoch, long incomingSequence) {
        if(lastProcessedEpoch == NOT_STARTED) {
            return SequenceDecision.IN_ORDER;
        }
        if (incomingEpoch > lastProcessedEpoch) {
            return SequenceDecision.NEW_SESSION;
        }
        else if (incomingEpoch < lastProcessedEpoch) {
            return SequenceDecision.STALE_SESSION;
        }
        else {
            return decide(incomingSequence);
        }
    }

    public void markProcessed(long sequence) {
        if (sequence > lastProcessedSequence) {
            lastProcessedSequence = sequence;
        }
    }

    public void markProcessed(long epoch, long sequence) {
        if (epoch > lastProcessedEpoch) {
            lastProcessedEpoch = epoch;
            lastProcessedSequence = sequence;
        }
        else if (epoch == lastProcessedEpoch) {
            markProcessed(sequence);
        }
    }

    public long lastProcessedSequence() {
        return lastProcessedSequence;
    }
    public long lastProcessedEpoch() {
        return lastProcessedEpoch;
    }
    /** Called after a simulated snapshot/replay so the validator accepts the recovered stream. */
    public void resetTo(long sequence) {
        this.lastProcessedSequence = sequence;
    }
}
