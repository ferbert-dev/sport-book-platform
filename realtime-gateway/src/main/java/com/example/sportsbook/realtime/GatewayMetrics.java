package com.example.sportsbook.realtime;

import java.util.concurrent.atomic.AtomicLong;

/** Plain counters; this module has no Actuator, so values are logged periodically. */
public class GatewayMetrics {

    public final AtomicLong websocketConnections = new AtomicLong();
    public final AtomicLong websocketMessagesSentTotal = new AtomicLong();
    public final AtomicLong websocketBadFramesTotal = new AtomicLong();
    public final AtomicLong sportsEventsConsumedTotal = new AtomicLong();

    public String snapshot() {
        return "websocket_connections=" + websocketConnections.get()
                + " websocket_messages_sent_total=" + websocketMessagesSentTotal.get()
                + " websocket_bad_frames_total=" + websocketBadFramesTotal.get()
                + " sports_events_consumed_total=" + sportsEventsConsumedTotal.get();
    }
}
