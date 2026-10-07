# What was fixed, and why

A record of the correctness problems found in this project and how each was resolved. Every entry
says what could go wrong, what changed, and how the change was verified. Problems that are known
and still open are listed at the end.

`README.md` holds the design rationale; this file is the history of where the design was wrong.

## Summary

| # | Problem | Resolution | Where |
| --- | --- | --- | --- |
| 1 | A lost provider message could leave a locked market taking bets | Suspend every active market on a sequence gap | `main` |
| 2 | A provider restarting its numbering froze the feed | Stream position is (session epoch, sequence) | `main`, PR #3 |
| 3 | A market suspended for a silent feed stayed closed and looked open | Suspension carries a reason; the next price ends it; clients see it | `main`, PR #4 |
| 4 | `sports-events` came back with one partition | Broker auto-creation off; clients wait for topic creation | `main`, PR #6 |
| 5 | A bet accepted after settlement stayed `OPEN` forever | Placement and settlement serialised per market | `3f57f07` |
| 6 | Settlement could publish a payout for a bet it then failed to settle | Settlement writes its events to the transactional outbox | `0e1c158` |
| 7 | One bad record could stall settlement for a whole partition | Data errors are not retried; the record is abandoned | `3a062b1` |
| 8 | A bet could be accepted whose payout could not be stored | Stake limited in two layers | `6984419` |

---

## 1. Sequence gap: a lost message could leave a locked market open

**Problem.** The provider numbers its messages. If one is lost, nothing says which market it was
for. The dangerous case is a lost `MARKET_LOCK`: prices keep arriving, so the staleness sweep never
fires, the market stays `ACTIVE`, and bets are accepted on a market the provider has closed.

**Fix.** On a gap the worker publishes `MARKET_SUSPENDED` for every active market before the
message that revealed the gap, versioned `receivedVersion - 1`: above everything stored for the
market, below the provider's next message, so the provider's own `MARKET_UNLOCK` still reopens it.
A price never reopens such a market. A restarted worker has an empty list of active markets, so
after any gap a market it has not published yet is suspended just before its first price. `SETTLED`
is terminal in the projection, so a late suspension cannot move a settled market.

**Where.** `SequenceValidator`, `GapSuspension`, `ActiveMarkets`, `OddsFeedVerticle`,
`MarketStateProjection`.

## 2. Provider session epoch: a restarted provider froze the feed

**Problem.** A provider that restarts sends sequence 1 again. Every message is then lower than the
last one processed, looks like a duplicate and is dropped. The feed stops with no error.

**Fix.** A stream position is the pair (session epoch, sequence), and the event version is
`epoch × 10¹⁵ + sequence`. A higher epoch is a new session and is handled like a gap; a lower one is
stale and dropped. Versions keep growing, so the projection's version guard is unchanged. The worker
resumes with `?sessionEpoch=<e>&fromSequence=<s+1>` and, after its own restart, derives that cursor
from the highest version on `sports-events`.

**Limit.** The version is one `long`: epochs up to 9222 have the full sequence range, and from 9224
a message is rejected as invalid. That stops ingestion; it cannot corrupt a version.

**Where.** `ProviderVersion`, `SequenceValidator`, `ProviderStreamClient`; in the simulator
`ProviderFeed` and `SequenceReservation`.

## 3. Stale-feed suspension: invisible, and permanent

**Problem.** When no price arrives for 30 seconds, `StalenessDetector` suspends the market by
writing Redis directly, with no Kafka event. Two consequences were seen on a running stack: the
demo page kept showing `ACTIVE` while bets were rejected with `MARKET_SUSPENDED`, and the market
stayed closed after prices resumed, because a price never changes status.

**Fix.** The sweep stores `suspendReason=STALE_FEED` next to `SUSPENDED`. The next applied price
reopens a market with that reason and only that reason; every status event from the feed clears it,
so a provider or gap suspension still needs `MARKET_OPENED`. The REST snapshot exposes
`suspendReason`, and the demo page reconciles status against the snapshot.

**Where.** `StalenessDetector`, `MarketStateProjection`, `EventStateRepository`, `ui/index.html`.

## 4. Kafka topic created with one partition

**Problem.** After a full restart of the broker, a client asked for `sports-events` before the
`kafka-init` job had run. The broker auto-created it with its default of one partition, and
`kafka-init`'s `--create --if-not-exists --partitions 3` then left it that way.

**Fix.** `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, and every Kafka client in `docker-compose.yml`
waits for `kafka-init` to complete.

## 5. A bet accepted after settlement stayed OPEN forever

**Problem.** `bet-service` validates against Redis, which lags Kafka. `settlement-service` reads
Kafka directly and selects a market's `OPEN` bets exactly once. A bet that commits after that select
is never settled: a redelivered `MARKET_SETTLED` is skipped by the idempotency fence. There are two
ways in: Redis still says `ACTIVE` after settlement has finished, or the two transactions simply
interleave (the bet inserts after the select and before the commit).

**Fix.** Both sides take a PostgreSQL advisory lock keyed by the market id, scoped to the
transaction.

- `BetService`, last step before the insert: lock **shared** → is there a `processed_settlements`
  row for this market? → insert, otherwise reject with `MARKET_SETTLED`.
- `SettlementService.settle`, first statement: lock **exclusive** → fence row → select `OPEN` bets.

Either the bet commits first and settlement, once it holds the lock, selects it; or settlement
commits first and the bet's check finds the fence row.

**Three rules this depends on.**

- The bet's lock is shared. Exclusive would make bets on one market queue behind each other.
- The check runs after the lock is taken. Checking first decides on stale information.
- The isolation level is `READ COMMITTED`: after waiting for the lock, the check needs a fresh
  snapshot. Under `REPEATABLE READ` it would look at the transaction's first snapshot and the bug
  would return without any error.

**Verified by.** `PlaceBetIT` (a bet racing a settlement that has not committed; a bet arriving
after it) and `SettlementIT` (a settlement waiting for a bet that holds the shared lock), all on
real PostgreSQL.

**Not covered.** Redis lagging a *suspension* rather than a settlement: a bet can still be accepted
in the moment between a provider lock and Redis showing it. That needs a bet delay, not a lock.

## 6. Settlement published to Kafka inside its database transaction

**Problem.** `SettlementService` sent `BET_SETTLED` and `PAYOUT_REQUIRED` from inside its database
transaction. Kafka and PostgreSQL share no commit, so a commit that failed after the sends left a
payout in Kafka for a bet that was still `OPEN`, and a retry sent it again. Sending after the commit
has the opposite failure: the fence row is committed, the event is lost, and the retry is skipped.

**Fix.** Settlement inserts its events into `outbox_event` in the same transaction as the bet rows
and the fence row. `settlement-service` no longer has a Kafka producer. `bet-service`'s
`OutboxPublisher` is the only relay and delivers those rows to `bet-events`.

**Found while testing.** The relay ordered rows by `created_at`, which each writer stamps with its
own clock; with two writers a bet's `BET_SETTLED` was published before its `BET_PLACED`. The relay
now orders by `seq`, an identity column assigned by the database (`V2__outbox_relay_sequence.sql`).

**What it guarantees.** No event without a committed settlement and no committed settlement without
its events. Delivery is at-least-once: the relay sends a row and then marks it, so a crash between
the two sends it again. `PAYOUT_REQUIRED` carries a deterministic `paymentId` so the payments side
can dedupe.

**What it costs.** Up to one polling interval of delay; settlement's events are delivered only while
`bet-service` runs; the table is never pruned; the relay is a single poller that stops at the first
row it cannot send.

**Verified by.** `SettlementIT`: bets, fence and outbox rows commit together, and a settlement that
fails in the database (a payout too large for its column) leaves no queued event behind.
`PlaceBetIT`: rows queued by settlement are relayed after the bet's own `BET_PLACED`. The chain was
also exercised on the Docker stack: a settled bet produced `BET_PLACED`, `BET_SETTLED` and
`PAYOUT_REQUIRED` on one partition, in that order.

## 7. A deterministic failure could stall settlement for a whole partition

**Problem.** Without an error handler, Spring Kafka retries a failing record ten times with no
pause and then skips it, so a short database outage would lose a settlement. The first fix retried
without limit, which has a worse failure: a record that can never succeed blocks its partition and
every other market on it. One bet, placed by anyone, could do that.

**Fix.** Transient failures are retried with backoff (1s up to 30s) until they succeed. A
`DataIntegrityViolationException` is not retried: the record is abandoned with
`SETTLEMENT_ABANDONED`. Its transaction rolled back, so the market's bets stay `OPEN` and it has no
fence row; it can be settled by replaying the record once the cause is fixed.

**Not verified.** The handler has not run against a failing record: the listener is switched off in
`SettlementIT`. That test does pin the exception type the handler classifies on.

## 8. A bet could be accepted whose payout could not be stored

**Problem.** A stake of up to 15 integer digits passed validation, but `stake × odds` could exceed
`bet.payout`, `NUMERIC(19,4)`. The failure appeared at settlement, long after the bet was accepted.

**Fix.** Two layers.

- Technical ceiling: the request allows at most 8 integer digits for the stake (400
  `INVALID_REQUEST`). With 6 digits of odds the worst payout is below 10¹⁴ and always fits.
- Business limit: `sportsbook.bet.max-stake` (default 10000), checked in `BetService` right after
  the idempotency replay and before any Redis read (422 `STAKE_LIMIT_EXCEEDED`). A retry of a bet
  accepted under a higher limit still returns that bet.

`BetServiceProperties` refuses to start with a limit of zero or less, or of 10⁹ or more.

---

## Known and still open

| Problem | Effect | Direction |
| --- | --- | --- |
| The loser of a concurrent idempotency-key race re-reads inside an aborted transaction | It gets HTTP 500 instead of the winner's bet; data stays correct. A failing test exists and is not committed | `INSERT … ON CONFLICT DO NOTHING`, or resolve in a new transaction |
| Market ownership is not checked | A bet with a live event id and another event's market is accepted and never settled | Compare the market's stored event id |
| The outbox relay is a single poller | A second instance would send every row twice | Claim rows with `FOR UPDATE SKIP LOCKED`, or CDC |
| `StalenessDetector` reads status and then writes it | It can overwrite `SETTLED` | One atomic conditional write |
| The projection's version guard is read-then-write | Not atomic with the write it guards | One atomic conditional write |
| Freshness is the projection's clock, not the provider's | A Kafka backlog looks fresh when it is finally processed | Keep source time and processing time apart |
| A result that never arrives | Bets stay `OPEN`; nothing voids them | Reconciliation, void and manual review |
| The feed worker crashing while it suspends markets on a gap | The quarantine obligation is lost with its memory | Persist the recovery obligation |
| An idempotency key reused with a different request | The original bet is returned silently | Bind the key to a request fingerprint |
| No authentication | `userId` comes from the request body; `/dev` is open | Stated demo limitation |

Outside the scope of this project: live bet delay, exposure and liability limits, wallet
reservation, void and resettlement, multiples, cash-out, responsible-gambling controls.
