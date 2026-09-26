package com.example.sportsbook.realtime.subscription;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CommandParserTest {

    @Test
    void parsesTheDocumentedSubscribeFrame() {
        ClientCommand command = CommandParser
                .parse("{\"action\":\"SUBSCRIBE\",\"eventId\":\"event-123\"}").orElseThrow();

        assertThat(command.action()).isEqualTo(ClientAction.SUBSCRIBE);
        assertThat(command.eventId()).isEqualTo("event-123");
    }

    @Test
    void parsesUnsubscribe() {
        assertThat(CommandParser.parse("{\"action\":\"UNSUBSCRIBE\",\"eventId\":\"event-1\"}"))
                .get()
                .extracting(ClientCommand::action)
                .isEqualTo(ClientAction.UNSUBSCRIBE);
    }

    @Test
    void actionIsCaseInsensitiveForClientConvenience() {
        assertThat(CommandParser.parse("{\"action\":\"subscribe\",\"eventId\":\"event-1\"}")).isPresent();
    }

    @Test
    void malformedJsonIsRejectedWithoutThrowing() {
        assertThat(CommandParser.parse("not json at all")).isEmpty();
        assertThat(CommandParser.parse("{\"action\":")).isEmpty();
    }

    @Test
    void unknownActionIsRejected() {
        assertThat(CommandParser.parse("{\"action\":\"DROP_TABLE\",\"eventId\":\"event-1\"}")).isEmpty();
    }

    @Test
    void missingOrBlankFieldsAreRejected() {
        assertThat(CommandParser.parse("{\"action\":\"SUBSCRIBE\"}")).isEmpty();
        assertThat(CommandParser.parse("{\"eventId\":\"event-1\"}")).isEmpty();
        assertThat(CommandParser.parse("{\"action\":\"SUBSCRIBE\",\"eventId\":\"\"}")).isEmpty();
    }

    @Test
    void nullAndBlankFramesAreRejected() {
        assertThat(CommandParser.parse(null)).isEmpty();
        assertThat(CommandParser.parse("   ")).isEmpty();
    }
}
