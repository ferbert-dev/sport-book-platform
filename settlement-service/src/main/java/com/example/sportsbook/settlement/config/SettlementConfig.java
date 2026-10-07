package com.example.sportsbook.settlement.config;

import com.example.sportsbook.common.SportsbookJson;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class SettlementConfig {

    private static final Logger log = LoggerFactory.getLogger(SettlementConfig.class);

    @Bean
    public ObjectMapper objectMapper() {
        return SportsbookJson.create();
    }

    /**
     * What happens to a MARKET_SETTLED record whose settlement throws.
     *
     * <p>Without this bean Spring Kafka's default applies: ten attempts with no pause, then the
     * record is logged and skipped. A database outage of a few seconds would then drop the
     * settlement for good and leave every bet on that market OPEN.
     *
     * <ul>
     *   <li><b>Transient failures</b> (database unreachable, lock timeout, ...) are retried until
     *       they succeed, with a growing pause (1s up to 30s).</li>
     *   <li><b>Data errors</b> ({@link DataIntegrityViolationException}: a value the schema cannot
     *       store, a broken constraint) are not retried. They fail the same way every time, so
     *       retrying forever would block the partition, and with it the settlement of every other
     *       market on it. That would let one bad bet, placed by anyone, stall settlement. The record
     *       is abandoned with SETTLEMENT_ABANDONED: its transaction rolled back, so the market's
     *       bets stay OPEN and it has no fence row, which means it can be settled later by
     *       replaying the record once the cause is fixed.</li>
     * </ul>
     *
     * <p>A production system would park an abandoned record on a dead-letter topic with an alert;
     * this project keeps to its two topics, so the log line is the alert.
     */
    @Bean
    public DefaultErrorHandler settlementErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(30_000L);
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (record, failure) -> log.error("SETTLEMENT_ABANDONED topic={} partition={} offset={} key={} "
                                + "reason={} note=bets-stay-open-replay-after-fix",
                        record.topic(), record.partition(), record.offset(), record.key(), failure.getMessage()),
                backOff);
        handler.addNotRetryableExceptions(DataIntegrityViolationException.class);
        handler.setRetryListeners((record, failure, attempt) ->
                log.error("SETTLEMENT_DELIVERY_FAILED topic={} partition={} offset={} attempt={} reason={}",
                        record.topic(), record.partition(), record.offset(), attempt, failure.getMessage()));
        return handler;
    }
}
