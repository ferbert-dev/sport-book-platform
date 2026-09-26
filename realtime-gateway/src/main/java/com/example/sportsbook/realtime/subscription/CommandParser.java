package com.example.sportsbook.realtime.subscription;

import com.fasterxml.jackson.databind.JsonNode;
import com.example.sportsbook.common.SportsbookJson;

import java.util.Optional;

/** Parses inbound client frames defensively: a bad frame must not kill the socket. */
public final class CommandParser {

    private CommandParser() {
    }

    public static Optional<ClientCommand> parse(String frame) {
        if (frame == null || frame.isBlank()) {
            return Optional.empty();
        }
        try {
            JsonNode node = SportsbookJson.mapper().readTree(frame);
            String action = text(node, "action");
            String eventId = text(node, "eventId");
            if (action == null || eventId == null || eventId.isBlank()) {
                return Optional.empty();
            }
            return Optional.of(new ClientCommand(ClientAction.valueOf(action.toUpperCase()), eventId));
        } catch (IllegalArgumentException unknownAction) {
            return Optional.empty();
        } catch (Exception malformedJson) {
            return Optional.empty();
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }
}
