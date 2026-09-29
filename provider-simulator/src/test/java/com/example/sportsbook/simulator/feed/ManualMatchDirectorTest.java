package com.example.sportsbook.simulator.feed;

import com.example.sportsbook.simulator.wire.ProviderMessage;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ManualMatchDirectorTest {

    private final ProviderFeed feed = new ProviderFeed(0L, Clock.systemUTC());
    private final ManualMatchDirector director = new ManualMatchDirector(feed);

    @Test
    void startsTheMatchUnderTheRequestedIdAndDerivesTheMarketId() {
        TestSubscriber<ProviderMessage> sent = feed.messages().test();

        ManualMatch match = director.startMatch("derby-1");

        assertThat(match.eventId()).isEqualTo("derby-1");
        assertThat(match.marketId()).isEqualTo("market-derby-1");
        assertThat(sent.values()).allSatisfy(message -> assertThat(message.matchId()).isEqualTo("derby-1"));
        assertThat(sent.values()).extracting(ProviderMessage::messageType)
                .startsWith("MATCH_START", "MARKET_UNLOCK").contains("PRICE_CHANGE");
    }

    @Test
    void eventPrefixIsNotRepeatedInTheMarketId() {
        assertThat(director.startMatch("event-777").marketId()).isEqualTo("market-777");
    }

    @Test
    void reusingAnIdThatIsAlreadyRunningIsRejected() {
        director.startMatch("derby-1");

        assertThatThrownBy(() -> director.startMatch("derby-1")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void differentEventIdsThatDeriveTheSameMarketIdAreRejected() {
        director.startMatch("event-777");

        assertThatThrownBy(() -> director.startMatch("777"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("market-777");
    }

    @Test
    void theScriptedMatchesMarketCannotBeTakenByADevMatch() {
        ManualMatchDirector withScripted = new ManualMatchDirector(feed, java.util.Set.of("market-456"));
        TestSubscriber<ProviderMessage> sent = feed.messages().test();

        assertThatThrownBy(() -> withScripted.startMatch("event-456")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> withScripted.startMatch("456")).isInstanceOf(IllegalStateException.class);
        assertThat(sent.values()).isEmpty();
    }

    @Test
    void idsThatAreNotPlainAreRejectedBeforeAnythingIsSent() {
        TestSubscriber<ProviderMessage> sent = feed.messages().test();

        for (String bad : new String[] {"", "has space", "slash/id", "x".repeat(65)}) {
            assertThatThrownBy(() -> director.startMatch(bad)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(sent.values()).isEmpty();
    }

    @Test
    void randomMatchesGetDistinctGeneratedIds() {
        ManualMatch first = director.startRandomMatch();
        ManualMatch second = director.startRandomMatch();

        assertThat(first.eventId()).startsWith("event-").isNotEqualTo(second.eventId());
    }
}
