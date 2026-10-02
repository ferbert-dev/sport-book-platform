package com.example.sportsbook.feed.provider;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ProviderMessageValidatorTest {

    private final Instant sentAt = Instant.now();

    @Test
    void wellFormedPriceChangeIsValid() {
        assertThat(ProviderMessageValidator.isValid(priceChange(new BigDecimal("2.10"), "real-madrid")))
                .isTrue();
    }

    @Test
    void oddsAtOrBelowOneAreRejectedBecauseTheyImplyNoOrNegativeReturn() {
        assertThat(ProviderMessageValidator.isValid(priceChange(BigDecimal.ONE, "real-madrid"))).isFalse();
        assertThat(ProviderMessageValidator.isValid(priceChange(new BigDecimal("0.5"), "real-madrid"))).isFalse();
    }

    @Test
    void priceChangeWithoutSelectionOrPriceIsRejected() {
        assertThat(ProviderMessageValidator.isValid(priceChange(new BigDecimal("2.10"), null))).isFalse();
        assertThat(ProviderMessageValidator.isValid(priceChange(null, "real-madrid"))).isFalse();
    }

    @Test
    final void marketResultWithoutAWinnerIsRejected() {
        ProviderMessage noWinner = new ProviderMessage(1, 1, "MARKET_RESULT", "event-123",
                "market-456", null, null, null, sentAt);

        assertThat(ProviderMessageValidator.isValid(noWinner)).isFalse();
    }

    @Test
    void missingMatchIdOrNegativeSequenceIsRejected() {
        assertThat(ProviderMessageValidator.isValid(new ProviderMessage(1, 1, "MATCH_START", null,
                null, null, null, null, sentAt))).isFalse();
        assertThat(ProviderMessageValidator.isValid(new ProviderMessage(1, -1, "MATCH_START", "event-123",
                null, null, null, null, sentAt))).isFalse();
    }

    @Test
    void nullMessageIsRejectedRatherThanThrowing() {
        assertThat(ProviderMessageValidator.isValid(null)).isFalse();
    }

    private ProviderMessage priceChange(BigDecimal price, String selection) {
        return new ProviderMessage(1, 1, "PRICE_CHANGE", "event-123", "market-456", selection,
                price, null, sentAt);
    }

    // FIX 4: a position that cannot become an event version is an invalid frame, not a crash

    @Test
    void negativeEpochIsInvalid() {
        assertThat(ProviderMessageValidator.isValid(matchStartAt(-1, 1))).isFalse();
    }

    @Test
    void sequenceAtOrAboveTheVersionRangeIsInvalid() {
        assertThat(ProviderMessageValidator.isValid(matchStartAt(1, 1_000_000_000_000_000L))).isFalse();
    }

    @Test
    void epochTooLargeForALongVersionIsInvalid() {
        assertThat(ProviderMessageValidator.isValid(matchStartAt(10_000, 1))).isFalse();
    }

    @Test
    void largestComposablePositionIsStillValid() {
        assertThat(ProviderMessageValidator.isValid(matchStartAt(9_222, 999_999_999_999_999L))).isTrue();
    }

    private ProviderMessage matchStartAt(long epoch, long sequence) {
        return new ProviderMessage(epoch, sequence, "MATCH_START", "event-123", null, null, null, null, sentAt);
    }
}
