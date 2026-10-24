# Changelog

All notable changes to MasoApps.Messaging.Nats. Versions follow semantic versioning.

## 6.0.0 - 2026-10-24

First public release, on nuget.org as `MasoApps.Messaging.Nats` (the
namespace stays `Messaging.NATS`). A rewrite of the consumer engine, the publisher and the hosting. Most 4.x and
5.x code keeps compiling, but publish, retry and ack behaviour changed. Read
[MIGRATION-v6.md](MIGRATION-v6.md) before upgrading.

Requirements: .NET 10, NATS.Net 2.7.3, nats-server 2.11 or later (2.12 or later
for schedules).

### Breaking changes

- Immediate publishes are JetStream publishes that wait for the stream's ack,
  with a 5 s budget and 2 retries under one message id. A subject no stream
  captures throws `NatsPublishException(NoStream)` instead of being dropped.
  `PublishOptions.FireAndForget` keeps the old core publish.
- Failed publishes and deferred publishes throw. Await every `PublishAsync`.
- Deferred publishes are JetStream message schedules (ADR-51). They need
  nats-server 2.12 and a stream with `AllowMsgSchedules`. Each deferred publish
  is its own schedule.
- Handlers that return normally are acked (both `INatsHandler<T>` and the 4.x
  `INatsConsumer<T>`).
- `MaxDeliveryAttempts` is honoured (default 3). The server MaxDeliver is
  unlimited and the library decides when a message is dead-lettered.
- Consumers never get a server Backoff. Retry delays are NAK delays,
  `RetryDelaysSeconds`, default `[1, 5, 30, 120, 600]` s.
- New durables on Limits and Interest streams start at `New` (was `All`).
  Existing durables keep their DeliverPolicy and DeliverGroup.
- Terminal failures are stored in a `DLQ` stream (created if missing) instead of
  a core publish to `dlq.{filter}`. The subject is `dlq.{actual subject}`.
- `DefaultQueueGroup` is null (was `"default-worker"`). A service with
  consumers needs `ServiceName`, `DefaultQueueGroup` or `QueueGroup`.
- Invalid attributes and options throw at registration (they used to log a
  warning and be skipped).
- A permanent setup error stops the application by default
  (`OnComponentFaulted = StopApplication`).
- The default AckWait is 30 s (5.x: 5 min), with an in-progress heartbeat.
- `IPublisher` is a singleton. The 5.x generic
  `PublishAsync<T>(subject, message, TimeSpan? deferredBy = null)` is replaced by
  explicit overloads; the 4.x `PublishAsync(string, object, TimeSpan?)` stays.
- 5.x attribute properties `AckWait` and `BackOffSeconds` are replaced by
  `AckWaitSeconds` and `RetryDelaysSeconds`. `DeadLetterSubject` is nullable.
  `NatsConsumerAttribute` is sealed.
- `NatsManager`, `NatsSubscriptionHostedService`, `CreateDurableSubscriber` and
  `CreateReplyHandler` are no longer public. The `NatsPublisher` constructor
  takes `NatsMessagingOptions`.
- `Nats-*` headers, the library's own `X-*` headers, `traceparent` and
  `tracestate` are dropped from user headers on publish.
- New package dependency: `Microsoft.Extensions.Diagnostics.HealthChecks`.

### Added

- `INatsHandler<T>` with `MessageContext<T>`: message, subject, headers, a
  stable `MessageId`, correlation id, sent and enqueued times, delivery count,
  and the schedule fields of a fired schedule.
- Multi-type handler classes: one `[NatsConsumer]` per binding, one durable per
  class and stream, dispatch by subject.
- Attribute settings `MessageType`, `Durable`, `MaxConcurrency`, `MaxMsgs`,
  `MaxAckPending`, `DeliverFrom`, `InactiveThresholdSeconds` and
  `MaxHandlerSeconds`.
- A pull loop that fetches only for free handler slots (no prefetch), a
  heartbeat for long handlers, and a handler time limit (`MaxHandlerTime`).
- `PublishOptions` (message id, correlation id, headers, timeout, retries,
  fire-and-forget), `PublishResult` and `NatsPublishException` with
  `OutcomeUnknown`.
- `ScheduleAsync` with keyed schedules (`Replace`, `CreateIfAbsent`),
  `CancelScheduleAsync`, `ScheduleResult.ScheduleId`, and
  `StreamDefinition.Scheduled(...)` for a timer stream.
- Stream provisioning modes `CreateIfMissing`, `VerifyOnly` and
  `CreateOrUpdate`. Updates keep the settings the definition does not model.
  New `StreamDefinition` properties: `MaxBytes`, `MaxMsgs`, `Replicas`,
  `Storage`, `Discard`, `DuplicateWindow`.
- A dead-letter stream with idempotent writes, full failure headers, and
  `NatsDeadLetters.ReplayAsync` (targeted or to every consumer).
- `NatsMessagingStatus`, the `AddNatsMessagingCheck` health check with
  `NatsHealthFilters`, `WaitUntilStarted` and `StartTimeout`.
- Graceful drain within `ShutdownTimeout`, with confirmed acks and hand-off
  NAKs.
- OpenTelemetry spans and metrics on the `Messaging.NATS` source and meter.
- Orphaned consumer reports (log and gauge, never a delete).
- `HandlerTypes`, `ServiceName` and `RetryOnInitialConnect` options.
- `Messaging.NATS.Testing`: `InMemoryPublisher` and `TestMessageContext`.

### Fixed

- Several deferred publishes to one subject overwrote each other; a consumer
  without a filter received pending schedules as well as their fires.
- Publishes to a subject without a stream were lost silently.
- Prefetched messages aged past AckWait while another handler ran, and were
  redelivered.
- A failed dead-letter write could drop the message.
- A consumer that hit a setup error stopped for good without notice, and an
  error loop could spin without a delay.
- A changed stream definition crashed startup (err_code 10058).
- A request handler subscription that failed was not retried.
- NATS client logs were hidden because the hosting LoggerFactory was replaced.

## 5.1.0 (2026-04-25)

- Added a dead-letter subject for messages that exhaust their retries, and
  `PoisonMessageException` to dead-letter at once.
- Added per-attempt redelivery delays (`BackOffSeconds`) and
  `DeadLetterSubject` on `NatsConsumerAttribute`.
- Exhausted messages are published to `dlq.<subject>` instead of being dropped
  by JetStream.

## 5.0.0 (2026-03-14)

- Deferred publishes use native JetStream message schedules (nats-server 2.12);
  `DeferredMessageHandler` is removed.
- `StreamDefinition.AllowMsgSchedules`.
- Code moved into `Abstractions`, `Attributes`, `Caching` and `Configuration`
  folders; `NatsCacheFactory` takes `NatsJSContext`.
- The consumer AckWait defaults to 5 minutes and MaxDeliver follows
  `MaxDeliveryAttempts`.
- Integration tests for pub/sub, request/reply and deferred messages.

## 4.2.0 (2026-03-14)

- NATS.Net 2.7.3. The library's own tracing is removed; listen on the
  `NATS.Net` activity source instead.
- Reflection-based JSON serialisation by default, with `SerializerContext` or
  `JsonSerializerOptions` to change it.
- Published as the `Messaging.NATS` package.

## 4.1.0 (2025-10-18)

- First version in this repository: attribute-based JetStream consumers,
  request/reply handlers, a publisher with deferred delivery, and a KV cache.
