package com.example.sportsbook.simulator.feed;

import com.example.sportsbook.simulator.wire.ProviderMessage;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * The looping scripted match: prices drift, the market locks and reopens, then it settles.
 *
 * <p>Deliberately injects one duplicate and one gap per cycle, so the worker's sequence-validation
 * and resync paths run at runtime, not only in tests. Because the {@link ProviderFeed} numbers
 * everything, the loop can never replay old sequences and needs no stride arithmetic.
 */
public final class ScriptedMatch {

    /** One step of the script: send a message, repeat the last one, or skip a sequence. */
    public sealed interface Step {
        record Send(ProviderMessage draft) implements Step { }
        record Duplicate() implements Step { }
        record Gap() implements Step { }
    }

    private final List<Step> script;
    private int position;

    public ScriptedMatch(String matchId, String marketId) {
        this.script = script(matchId, marketId);
    }

    /** Plays the next step into the feed, wrapping around at the end of the script. */
    public void playNext(ProviderFeed feed) {
        Step step = script.get(position);
        position = (position + 1) % script.size();
        switch (step) {
            case Step.Send send -> feed.emit(send.draft());
            case Step.Duplicate ignored -> feed.resendLast();
            // A gap alone sends nothing, so play the next step now to keep the pace steady.
            case Step.Gap ignored -> {
                feed.skipSequence();
                playNext(feed);
            }
        }
    }

    List<Step> steps() {
        return script;
    }

    private static List<Step> script(String matchId, String marketId) {
        List<Step> steps = new ArrayList<>();
        steps.add(send(ProviderMessage.matchStart(matchId)));
        steps.add(send(price(matchId, marketId, "2.10")));
        steps.add(send(price(matchId, marketId, "2.05")));
        steps.add(new Step.Duplicate());
        steps.add(send(price(matchId, marketId, "1.95")));
        steps.add(send(ProviderMessage.marketLock(matchId, marketId)));
        steps.add(send(ProviderMessage.marketUnlock(matchId, marketId)));
        steps.add(new Step.Gap());
        steps.add(send(price(matchId, marketId, "1.85")));
        steps.add(send(ProviderMessage.matchEnd(matchId)));
        steps.add(send(ProviderMessage.marketResult(matchId, marketId, "real-madrid")));
        return List.copyOf(steps);
    }

    private static Step send(ProviderMessage draft) {
        return new Step.Send(draft);
    }

    private static ProviderMessage price(String matchId, String marketId, String price) {
        return ProviderMessage.priceChange(matchId, marketId, "real-madrid", new BigDecimal(price));
    }
}
