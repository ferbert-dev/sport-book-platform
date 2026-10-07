package com.example.sportsbook.bet.config;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BetServicePropertiesTest {

    private static BetServiceProperties withMaxStake(String maxStake) {
        return new BetServiceProperties(Duration.ofSeconds(30), BigDecimal.ZERO,
                maxStake == null ? null : new BigDecimal(maxStake));
    }

    @Test
    void anUnsetLimitFallsBackToTheDefault() {
        assertThat(withMaxStake(null).maxStake()).isEqualByComparingTo("10000");
    }

    @Test
    void aLimitWhosePayoutCouldNotBeStoredStopsTheServiceAtStartup() {
        // 10^9 x the largest odds (below 10^6) reaches 10^15, one digit more than NUMERIC(19,4) holds.
        assertThatThrownBy(() -> withMaxStake("1000000000"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-stake");
    }

    @Test
    void theLargestPayableLimitIsAllowed() {
        assertThat(withMaxStake("999999999.9999").maxStake()).isEqualByComparingTo("999999999.9999");
    }

    @Test
    void aLimitOfZeroOrLessIsRefused() {
        assertThatThrownBy(() -> withMaxStake("0")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> withMaxStake("-1")).isInstanceOf(IllegalArgumentException.class);
    }
}
