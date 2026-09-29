package com.example.sportsbook.feed.provider;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class ReconnectBackoffTest {

    private final ReconnectBackoff backoff =
            new ReconnectBackoff(Duration.ofMillis(500), Duration.ofSeconds(30));

    @Test
    void delayDoublesAfterEachConsecutiveFailure() {
        assertThat(IntStream.range(0, 4).mapToLong(i -> backoff.nextDelayMillis()))
                .containsExactly(500L, 1000L, 2000L, 4000L);
    }

    @Test
    void delayIsCappedAtTheMaximum() {
        IntStream.range(0, 10).forEach(i -> backoff.nextDelayMillis());

        assertThat(backoff.nextDelayMillis()).isEqualTo(30_000L);
    }

    @Test
    void manyFailuresNeverOverflowTheDelay() {
        IntStream.range(0, 1_000).forEach(i -> backoff.nextDelayMillis());

        assertThat(backoff.nextDelayMillis()).isEqualTo(30_000L);
    }

    @Test
    void successfulConnectionStartsOverFromTheInitialDelay() {
        IntStream.range(0, 5).forEach(i -> backoff.nextDelayMillis());

        backoff.reset();

        assertThat(backoff.nextDelayMillis()).isEqualTo(500L);
        assertThat(backoff.consecutiveFailures()).isEqualTo(1);
    }
}
