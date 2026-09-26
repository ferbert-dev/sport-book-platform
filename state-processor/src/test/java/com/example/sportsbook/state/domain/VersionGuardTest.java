package com.example.sportsbook.state.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VersionGuardTest {

    @Test
    void firstEventForAKeyIsAlwaysApplied() {
        assertThat(VersionGuard.shouldApply(null, 1L)).isTrue();
    }

    @Test
    void newerVersionIsApplied() {
        assertThat(VersionGuard.shouldApply(100L, 101L)).isTrue();
    }

    @Test
    void sameVersionIsRejectedSoRedeliveryIsIdempotent() {
        assertThat(VersionGuard.shouldApply(100L, 100L)).isFalse();
    }

    @Test
    void olderVersionIsRejectedSoOutOfOrderDeliveryCannotRewindState() {
        assertThat(VersionGuard.shouldApply(100L, 99L)).isFalse();
    }
}
