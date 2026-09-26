package com.example.sportsbook.settlement.service;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class PayoutCalculatorTest {

    @Test
    void decimalOddsIncludeTheStakeSoPayoutIsStakeTimesOdds() {
        assertThat(PayoutCalculator.winningPayout(new BigDecimal("100.00"), new BigDecimal("2.10")))
                .isEqualByComparingTo("210.00");
    }

    @Test
    void payoutIsRoundedToTwoDecimalPlacesHalfUp() {
        // 100 * 1.005 = 100.50 exactly; 33.33 * 1.115 = 37.163... -> 37.16
        assertThat(PayoutCalculator.winningPayout(new BigDecimal("33.33"), new BigDecimal("1.115")))
                .isEqualByComparingTo("37.16");
        assertThat(PayoutCalculator.winningPayout(new BigDecimal("10.00"), new BigDecimal("1.005")))
                .isEqualByComparingTo("10.05");
    }

    @Test
    void payoutAlwaysCarriesCurrencyScale() {
        assertThat(PayoutCalculator.winningPayout(new BigDecimal("100"), new BigDecimal("2")).scale())
                .isEqualTo(2);
        assertThat(PayoutCalculator.losingPayout().scale()).isEqualTo(2);
    }

    @Test
    void losingBetPaysNothing() {
        assertThat(PayoutCalculator.losingPayout()).isEqualByComparingTo("0.00");
    }
}
