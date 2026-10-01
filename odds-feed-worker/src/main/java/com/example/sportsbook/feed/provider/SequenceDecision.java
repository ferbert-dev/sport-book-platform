package com.example.sportsbook.feed.provider;

/** Outcome of validating a provider message's sequence number. */
public enum SequenceDecision {

    /** Exactly the next expected sequence number. */
    IN_ORDER,

    /** Already seen, or older than what we have processed. Drop it. */
    DUPLICATE,

    /** Newer than expected: at least one message was missed. Process, but resynchronize. */
    GAP,
    /** Higher epoch: the provider restarted its numbering. Process it, and treat the switch like a gap. */
    NEW_SESSION,
    /** Lower epoch: a late message from a session already left behind. Drop it. */
    STALE_SESSION
}
