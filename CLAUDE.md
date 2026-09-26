# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`live-sportsbook` — a Java 21 Maven multi-module live sports betting platform built as a **system
design reference project**, not a production sportsbook. It exists to demonstrate event-driven
architecture honestly, including its failure modes. `README.md` carries the full design rationale
and trade-off discussion; this file covers what you need to work in the code.

Maven only. **Never add Gradle files.**

## Commands

```bash
./mvnw clean verify                   # canonical build: compile + 92 unit + 7 integration tests
./mvnw test                           # unit tests only (no Docker needed)
./mvnw -pl bet-service -am test       # one module plus its dependencies

# Everything in Docker (images run pre-built JARs, so package first):
./mvnw clean package && docker compose up -d --build   # demo UI at localhost:8080

# Infrastructure only, services on the host:
docker compose up -d kafka kafka-init redis postgres
```

Run a single test class or method. `-Dsurefire.failIfNoSpecifiedTests=false` is **required** with
`-am`: the reactor also runs surefire on `common-domain`, which matches nothing and would otherwise
fail the build.

```bash
./mvnw -pl bet-service -am test -Dtest=BetServiceTest -Dsurefire.failIfNoSpecifiedTests=false
./mvnw -pl bet-service -am test -Dtest='BetServiceTest#validBetIsAccepted' -Dsurefire.failIfNoSpecifiedTests=false

# Integration tests use failsafe's -Dit.test
./mvnw -pl state-processor -am verify -Dit.test=SportsEventProjectionIT \
  -Dtest=skipUnit -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false
```

After `./mvnw clean install -DskipTests`, the flag is unnecessary because `-am` can be dropped:

```bash
./mvnw -pl bet-service test -Dtest=BetServiceTest
```

There is no linter or formatter configured. Follow the surrounding style.

### Running the services

All six run from their built JARs — Boot JARs and shaded Vert.x JARs alike:

```bash
java -jar state-processor/target/state-processor-1.0.0-SNAPSHOT.jar          # :8081
java -jar odds-service/target/odds-service-1.0.0-SNAPSHOT.jar                # :8082
java -jar realtime-gateway/target/realtime-gateway-1.0.0-SNAPSHOT.jar        # :8083
java -jar bet-service/target/bet-service-1.0.0-SNAPSHOT.jar                  # :8084
java -jar settlement-service/target/settlement-service-1.0.0-SNAPSHOT.jar    # :8085
java -jar odds-feed-worker/target/odds-feed-worker-1.0.0-SNAPSHOT.jar        # no HTTP port
```

Order matters: **`bet-service` before `settlement-service`** (it owns the Flyway migrations, and
settlement runs with Flyway disabled), and **`odds-feed-worker` last** so consumers are listening.

`./mvnw -pl <module> -am spring-boot:run` **fails** with "Unable to find a suitable main class" — a
CLI-invoked goal runs against every reactor project, and `-am` pulls in the root aggregator and
`common-domain`, neither of which has a main class. Use `./mvnw clean install -DskipTests` once,
then `./mvnw -pl <module> spring-boot:run` without `-am`.

## Architecture

The design rests on one distinction, and most invariants below follow from it:

- **Kafka** = what happened. Durable log, the source of truth, replayable.
- **Redis** = what is true now. A rebuildable projection; losing it is an availability problem only.
- **PostgreSQL** = durable accepted bets. *Not* rebuildable from Kafka or Redis.

```
Provider → odds-feed-worker → Kafka(sports-events) → state-processor → Redis
                                       ├→ realtime-gateway → WebSocket clients
                                       └→ settlement-service ─┐
                                                              ├→ Postgres
Client → bet-service → Redis (validate) → Postgres (bet + outbox) → Kafka(bet-events)
```

| Module | Stack | Notes |
| --- | --- | --- |
| `common-domain` | **plain Java + Jackson** | Events, enums, `RedisKeys`, `SportsbookJson` |
| `odds-feed-worker` | Vert.x + RxJava 3, **no Spring** | Shaded executable JAR |
| `realtime-gateway` | Vert.x Web, **no Spring** | Shaded executable JAR |
| `state-processor` | Spring Boot | Kafka → Redis projection |
| `odds-service` | Spring Boot Web | Redis read path only |
| `bet-service` | Spring Boot + JPA | Owns all Flyway migrations |
| `settlement-service` | Spring Boot + JPA | Flyway disabled |

### Invariants — breaking these breaks the design

1. **`odds-feed-worker` must never write Redis.** The path is always
   `Provider → Worker → Kafka → state-processor → Redis`. Writing Redis directly would make Redis
   the only record of the feed, so it could no longer be rebuilt and no other consumer could see it.
2. **`common-domain` stays plain Java.** No Spring, no Vert.x, no Spring Data. The pure-Vert.x
   modules depend on it. `RedisKeys` lives here because it is a *contract* between the writer
   (state-processor) and readers (odds-service, bet-service) — but each service keeps its own thin
   Redis reader, since a shared one would drag Spring Data into `common-domain`.
3. **Every projection write is version-guarded.** `VersionGuard.shouldApply` — apply only if
   `incoming.version > stored.version`. This is what makes the projection idempotent under Kafka's
   at-least-once delivery, and it is what makes correctness independent of arrival order. New event
   handling in `MarketStateProjection` must go through it.
4. **`ODDS_UPDATED` must not touch market status.** A price may move while a market is suspended;
   reopening must not resurrect a stale price.
5. **Exactly two topics**: `sports-events` and `bet-events`. Do not add a third. `BET_REJECTED` is
   deliberately not published — rejections return synchronously over HTTP.
6. **Partition keys**: `marketId` for market-scoped events, `eventId` for match-level, `betId` for
   bet events. `SportsEvent.partitionKey()` encodes this; don't bypass it.
7. **At-least-once, never exactly-once.** Do not add code comments, docs, or log lines claiming
   end-to-end exactly-once semantics.
8. **Money and odds are `BigDecimal`** (`NUMERIC` in SQL), timestamps are `Instant`, internal ids
   are `UUID`. No doubles anywhere near money. Payout uses decimal odds that *include* the stake:
   `stake * acceptedOdds`, scale 2, `HALF_UP`.
9. **Constructor injection only.** No field injection.

### Idempotency: the unique constraint is the fence, not the read

Both idempotency paths use the same two-step shape, and the second step is the one that actually
guarantees correctness:

- **Bets** — read `(userId, idempotencyKey)` first, *before any validation*, so a client retrying
  after a timeout is never re-judged against newer odds. Concurrent retries can both pass that
  read; the loser catches `DataIntegrityViolationException` on `uk_bet_user_idempotency` and
  resolves by re-reading the winner.
- **Settlement** — register `(marketId, settlementVersion)` in `processed_settlements` first,
  with the same read-then-insert-then-catch shape on `uk_settlement_market_version`.

`settlement-service` publishes to Kafka **inside** the transaction on purpose. If the broker is
down, the transaction rolls back and Kafka redelivers `MARKET_SETTLED`. Publishing after commit
would leave the fence row committed, so the retry would be skipped as a duplicate and
`BET_SETTLED` would be lost permanently. Do not "fix" this by moving the publish out.

### Adding a domain event

`SportsEvent` and `BetEvent` are **sealed interfaces** over records. Adding a type means updating
the `permits` clause, `@JsonSubTypes`, and every exhaustive `switch`. Those switches failing to
compile is the intended safety net — resolve them rather than adding a `default` branch.

Jackson uses `@JsonTypeInfo(include = As.PROPERTY, property = "type")`. Do **not** switch to
`EXISTING_PROPERTY`: Jackson only treats record *components* as properties, so a `type()` accessor
is never serialized and the discriminator silently vanishes.

### Schema ownership

`bet-service` owns **every** migration in `bet-service/src/main/resources/db/migration`, including
`processed_settlements`, which `settlement-service` reads and writes but must never migrate
(`spring.flyway.enabled: false`). One migration history, no race between two services.

Both services map the `bet` table — a deliberate simplification documented in the README's
limitations, not a pattern to extend.

## Build configuration that is load-bearing

Three root-POM settings exist for specific, non-obvious reasons; removing them breaks the build in
confusing ways:

- **Custom parent, not `spring-boot-starter-parent`.** Not about dependencies: a Maven parent never
  transmits dependencies through `dependencyManagement`, and `spring-boot-starter-parent` declares
  no `<dependencies>` of its own, so Spring jars would not leak into the Vert.x modules either way
  (verified — `dependency:tree` shows zero `org.springframework` artifacts there). What a Boot
  parent *does* impose is build convention: `spring-boot-maven-plugin` defaults, `@`-delimiter
  resource filtering, and compiler settings driven by its own properties. A custom parent keeps
  those out of the two non-Boot modules, which want `maven-shade-plugin` instead. Spring Boot and
  Vert.x are then *imported* as BOMs purely for version management.
- **`docker.api.version` (default `1.44`)**, passed to surefire/failsafe as the `api.version`
  system property. Docker Engine 29 requires API ≥ 1.40, while the docker-java client bundled with
  Testcontainers 1.21.3 negotiates v1.32 and is rejected with HTTP 400 — Testcontainers then
  reports "Could not find a valid Docker environment". Override on an older daemon.
- **Failsafe `<classesDirectory>${project.build.outputDirectory}</classesDirectory>`.** Failsafe
  runs after `package`, so it would otherwise put the repackaged Boot fat JAR on the test classpath,
  where classes sit under `BOOT-INF/` and are invisible to component scanning.

Pinned versions: Spring Boot 3.5.3, Vert.x **4.5.14** (5.x is still release-candidate only),
RxJava 3.1.10, Testcontainers 1.21.3.

**Kafka image: use `apache/kafka:4.0.0`.** `apache/kafka:3.9.0` fails to start under Testcontainers
1.21.3 (container exits 1 with no output). Keep the compose file and the ITs on the same tag.

## Testing notes

Integration tests are `*IT.java`, run by Failsafe in `verify`, and annotated
`@Testcontainers(disabledWithoutDocker = true)` — so `./mvnw clean verify` stays green without a
Docker daemon by skipping them. Keep that annotation on new ITs; the build must never require Docker.

Vert.x modules use `SubscriptionRegistry<C>` generically so the registry is unit-testable with
`String` stand-ins instead of real sockets. Prefer extracting pure logic (`SequenceValidator`,
`MessageNormalizer`, `PayoutCalculator`, `VersionGuard`) over testing through the event loop.

`BetService` takes an injected `Clock` so freshness checks are testable without sleeping.

## Demo UI

`ui/index.html` is a single static page with no build step, served by an nginx container that also
reverse-proxies `/api/*` to odds-service, bet-service and the gateway. That single origin is why no
Spring service needs CORS config — keep it that way rather than adding CORS to make a direct-to-
service page work.

The page re-implements the server's version guard client-side on purpose: it logs streamed events
that are not strictly newer as `ignored`. That mirrors `VersionGuard` and is the point of the page,
so don't "simplify" it into applying every frame.

## Simulated provider

`SimulatedSportsProvider` scripts a match lifecycle and loops it, deliberately injecting one
duplicate and one sequence gap per cycle so those paths run at runtime, not only in tests.

Its sequence stride is **derived from the script** (`max - min + 1`) so cycles are contiguous. A
larger stride injects a spurious gap at every cycle boundary; replaying the same sequences makes the
version guard discard everything and freezes the projection after one cycle. Don't hardcode it.

## Logging

Stable, greppable event names with identifiers attached — `SEQUENCE_GAP_DETECTED`,
`MARKET_SUSPENDED_STALE_FEED`, `BET_ACCEPTED`, `BET_REJECTED_ODDS_CHANGED`, `DUPLICATE_BET_REQUEST`,
`SETTLEMENT_COMPLETED`, `DUPLICATE_SETTLEMENT_SKIPPED`. Match this convention in new code.

The Vert.x modules ship a `logback.xml` because Logback's default is DEBUG, which buries
application events under Kafka and Netty internals. They have no Actuator, so they keep plain
counters and log a snapshot periodically; Spring services use Micrometer via Actuator.
