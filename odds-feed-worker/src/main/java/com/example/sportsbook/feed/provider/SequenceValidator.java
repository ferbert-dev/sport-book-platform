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

    /** Decides and, unless the message is a duplicate or from a stale session, marks it processed. */
    public SequenceDecision evaluate(long epoch, long incomingSequence) {
        SequenceDecision decision = decide(epoch, incomingSequence);
        if (decision != SequenceDecision.DUPLICATE && decision != SequenceDecision.STALE_SESSION) {
            markProcessed(epoch, incomingSequence);
        }
        return decision;
    }

    /**
     * Classifies a message without recording it: the epoch first, then the sequence within the
     * epoch. The worker marks a message processed only once Kafka has acknowledged it, so a failed
     * publish is asked for again on reconnect instead of being skipped.
     */
    public SequenceDecision decide(long incomingEpoch, long incomingSequence) {
        if (lastProcessedEpoch == NOT_STARTED) {
            return SequenceDecision.IN_ORDER;
        }
        if (incomingEpoch > lastProcessedEpoch) {
            return SequenceDecision.NEW_SESSION;
        }
        if (incomingEpoch < lastProcessedEpoch) {
            return SequenceDecision.STALE_SESSION;
        }
        return decideWithinEpoch(incomingSequence);
    }

    /** A higher epoch replaces both fields; the same epoch only moves forward; a lower one is ignored. */
    public void markProcessed(long epoch, long sequence) {
        if (epoch > lastProcessedEpoch) {
            lastProcessedEpoch = epoch;
            lastProcessedSequence = sequence;
        } else if (epoch == lastProcessedEpoch && sequence > lastProcessedSequence) {
            lastProcessedSequence = sequence;
        }
    }

    public long lastProcessedSequence() {
        return lastProcessedSequence;
    }

    public long lastProcessedEpoch() {
        return lastProcessedEpoch;
    }

    /** Called after a simulated snapshot/replay so the validator accepts the recovered stream. */
    public void resetTo(long epoch, long sequence) {
        this.lastProcessedEpoch = epoch;
        this.lastProcessedSequence = sequence;
    }

    private SequenceDecision decideWithinEpoch(long incomingSequence) {
        if (incomingSequence <= lastProcessedSequence) {
            return SequenceDecision.DUPLICATE;
        }
        return incomingSequence > lastProcessedSequence + 1 ? SequenceDecision.GAP : SequenceDecision.IN_ORDER;
    }
}
