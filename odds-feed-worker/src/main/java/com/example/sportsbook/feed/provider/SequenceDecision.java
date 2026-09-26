package com.example.sportsbook.feed.provider;

/** Outcome of validating a provider message's sequence number. */
public enum SequenceDecision {

    /** Exactly the next expected sequence number. */
    IN_ORDER,

    /** Already seen, or older than what we have processed. Drop it. */
    DUPLICATE,

    /** Newer than expected: at least one message was missed. Process, but resynchronize. */
    GAP
}
