# Migrating to Messaging.NATS 6.0

This guide covers upgrades from 4.x and 5.x. Most code keeps compiling, but v6
changes how publishes, retries and acks behave. Read the whole guide, not only
the compile errors.

Requirements:

- .NET 10 and NATS.Net 2.7.3 (both unchanged).
- nats-server 2.11 or later. Schedules and deferred publishes need nats-server
  2.12 or later.
- A new package dependency: `Microsoft.Extensions.Diagnostics.HealthChecks`.

## Mechanical steps

Do these in order. Each step says what breaks if you skip it.

### 1. Package and namespace

v6 ships on nuget.org as `MasoApps.Messaging.Nats`. Replace any reference to the
older `Messaging.NATS` or `MasoApps.Messaging` package ids with it. Early 4.x
builds also used the `MasoApps.Messaging` namespace; change the usings:

```csharp
using Messaging.NATS;          // was: using MasoApps.Messaging;
using Messaging.NATS.Testing;  // InMemoryPublisher, TestMessageContext
```

### 2. Give every service with consumers a queue group

The `"default-worker"` fallback is gone. `DefaultQueueGroup` is now null, and a
service with consumers and no queue group fails at registration.

```csharp
builder.Services.AddNatsMessaging(options =>
{
    options.NatsUrl = "nats://localhost:4222";
    options.ServiceName = "orders-worker";      // new: connection name, durable owner, queue group fallback
    options.DefaultQueueGroup = "default-worker"; // keep the 4.x/5.x durable names, if you relied on the default
    options.Assemblies = [typeof(Program).Assembly];
});
```

Durable names are `{queue group}_{ClassName}`, exactly as in 4.x. If your
services used the old default, set `DefaultQueueGroup = "default-worker"` (or
the value you had), or the durables get new names and start again (see
"DeliverPolicy for new durables" below). Set a distinct `ServiceName` on every
service in any case: it marks which service owns a durable.

### 3. Await every publish

Publishes now throw on failure (step 4 and "Acked publishes" below). A call that
is not awaited loses that error, and the message with it.

```csharp
// Before: the task is dropped, a failure goes unnoticed.
_publisher.PublishAsync("orders.reminder", reminder, TimeSpan.FromHours(1));

// After
await _publisher.PublishAsync("orders.reminder", reminder, TimeSpan.FromHours(1));
```

Search for `PublishAsync(` calls that are not awaited or returned. This is
required, not a style fix.

### 4. Make sure a stream captures every subject you publish to

Every publish is a JetStream publish. A subject that no stream captures used to
be dropped silently. It now throws `NatsPublishException` with
`Failure = NoStream` after the publish budget (5 s by default). For messages you
really can lose, opt out per call with `PublishOptions.FireAndForget`.

### 5. Turn on schedules where you defer messages

`PublishAsync(subject, message, deferredBy)` with a positive delay is now a
JetStream message schedule. It needs nats-server 2.12 or later, and the stream
that captures the subject must allow schedules:

```csharp
options.StreamDefinitions.Add(new StreamDefinition
{
    Name = "QUEUE",
    Subjects = ["queue.>"],
    Retention = StreamConfigRetention.Workqueue,
    AllowMsgSchedules = true   // new
});
```

Under the default `CreateIfMissing` provisioning this is enough: the next start
turns schedules on for the existing stream and adds the holder subject
`_sched.{Name}.>`. Nothing else on the stream changes. The schedule rules
(no Interest retention, no `MaxMsgs`/`MaxBytes` limit, `Discard` not New, no
unfiltered consumer) are checked at registration and on the server.

A deferred publish can now also throw `NatsMessagingConfigurationException` (no
stream with schedules captures the subject) and `ArgumentOutOfRangeException`
(the delay is at or beyond the stream's `MaxAge`). A caller that catches only
`NatsPublishException` misses these.

For new code, use `ScheduleAsync`, which also supports keys (Replace,
CreateIfAbsent) and `CancelScheduleAsync`. See "Schedules" in the README.

### 6. Rename the 5.x attribute properties

```csharp
// 5.x
[NatsConsumer(Stream = "ORDERS", Subject = "orders.placed",
    AckWait = ..., BackOffSeconds = [1, 5, 30])]

// 6.0
[NatsConsumer(Stream = "ORDERS", Subject = "orders.placed",
    AckWaitSeconds = 30, RetryDelaysSeconds = [1, 5, 30])]
```

- `AckWait` (TimeSpan) becomes `AckWaitSeconds` (int, 0 = 30 s).
- `BackOffSeconds` becomes `RetryDelaysSeconds`. The delays are NAK delays, not a
  server Backoff (see "No default backoff" below).
- `DeadLetterSubject` is nullable. Null means `{DeadLetterSubjectPrefix}.{subject}`.

### 7. Add `MessageType` on classes that handle several types

A class may now implement several handler interfaces and carry one
`[NatsConsumer]` per binding. When it implements more than one, every attribute
needs `MessageType`, and the consumer-level settings (`Stream`, `QueueGroup`,
`Durable`, `AckWaitSeconds`, `MaxConcurrency`, `MaxMsgs`, `MaxAckPending`,
`DeliverFrom`, `InactiveThresholdSeconds`, `MaxHandlerSeconds`) must agree
across its attributes on one stream. Invalid attributes now throw at
registration. 4.x logged a warning and skipped them.

### 8. Remove uses of the low-level API

`NatsManager`, `NatsSubscriptionHostedService`, `CreateDurableSubscriber`,
`CreateReplyHandler` and the pre-3.0 APIs are no longer public. Use attributes
for consumers and request handlers. Tests that created consumers through
`NatsManager` must be rewritten (see "Testing" below). If you construct
`NatsPublisher` yourself, its constructor is now
`(INatsConnection, NatsJSContext, NatsMessagingOptions, ILogger<NatsPublisher>)`;
resolve `IPublisher` from DI instead. `IPublisher` is now a singleton (it was
scoped).

### 9. Fix the publish calls that no longer bind

| Call | v6 |
| --- | --- |
| 5.x `PublishAsync<T>(subject, message, deferredBy)` | Removed. Use `PublishAsync<T>(subject, message)` (with `PublishOptions` or a `CancellationToken`), or `ScheduleAsync`. The 4.x `PublishAsync(string, object, TimeSpan?)` overload stays. |
| `PublishAsync(s, x, null)` with `x` typed `object` | Ambiguous (CS0121). Write `(TimeSpan?)null`. |
| `PublishAsync(s, x, null)` or `PublishAsync(s, x)` with `x` typed as a base class or interface | Compiles, but the generic overloads serialise by the static type `T`, not the runtime type. Derived properties are dropped (a `SerializerContext` without an entry for the interface throws). Pass the concrete type, or write `(TimeSpan?)null` to keep the 4.x overload, which serialises the runtime type. |

### 10. Add the health check

```csharp
builder.Services.AddHealthChecks().AddNatsMessagingCheck(tags: ["ready"]);
```

Map it to your readiness probe. Startup now runs in the background, so readiness
comes from this check, not from host start (see "Startup" below).

### 11. Optional: move handlers to `INatsHandler<T>`

`INatsConsumer<T>` still works with the same signature. New handlers should use
`INatsHandler<T>`: the library settles the message, and `MessageContext<T>` has
a stable `MessageId` for idempotency, the correlation id, the send time and the
schedule fields.

```csharp
// Before
public sealed class OrderPlacedConsumer : INatsConsumer<OrderPlaced>
{
    public async Task HandleMessageAsync(INatsJSMsg<OrderPlaced> message, CancellationToken cancellationToken)
    {
        await _orders.ReserveStockAsync(message.Data!, cancellationToken);
        await message.AckAsync(cancellationToken: cancellationToken);
    }
}

// After
public sealed class OrderPlacedConsumer : INatsHandler<OrderPlaced>
{
    public Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken cancellationToken) =>
        _orders.ReserveStockAsync(context.Message, cancellationToken);
}
```

Return to ack. Throw to retry. Throw `PoisonMessageException` to dead-letter at
once. The class name and attribute stay the same, so the durable keeps its name.

### 12. Testing

- Moq setups and verifies on `IPublisher` keep compiling: every publish shape is
  its own overload with no optional parameters, and the 4.x overload still
  returns `Task`.
- `Messaging.NATS.Testing.InMemoryPublisher` is the supported fake. It applies
  Replace, CreateIfAbsent and Cancel, dedupes message ids, and can be told to
  fail calls.
- `TestMessageContext.Create(...)` builds a `MessageContext<T>` for handler
  unit tests.

## Behaviour changes that could surprise

### The four to read first

**Acked publishes.** `PublishAsync` now waits for the stream's
acknowledgement. Each call costs one round trip to the stream leader (on R3,
also the replica writes). 4.x and 5.x sent immediate publishes as core NATS
publishes, which return once the message is queued on the connection.

- A loop that awaits publishes one by one is now as slow as the round trip
  times the count. Run independent publishes at once (`Task.WhenAll`), or use
  `FireAndForget` for messages you can lose.
- Each call has a budget (`PublishTimeout`, 5 s) and retries inside it
  (`PublishRetryAttempts`, 2) with the same message id. A call that cannot reach
  the stream blocks its caller for up to the budget, then throws. Lower
  `PublishOptions.Timeout` on latency-sensitive paths.
- With retries on and no `MessageId`, the library generates one, so the stream
  keeps a dedupe entry per message for its `DuplicateWindow`. Pass a stable
  business id for publishes that must not be stored twice, or
  `RetryAttempts = 0` to send none.
- `NatsPublishException.OutcomeUnknown` (a timeout) means the message may be
  stored. Retry with the same `MessageId`.

**DeliverPolicy for new durables.** A durable created by v6 on a Limits or
Interest stream starts at `New`: it receives only messages stored after it was
created. 4.x and 5.x started every durable at `All`. WorkQueue streams still
start at `All`.

- Existing durables are not affected: their DeliverPolicy and DeliverGroup are
  kept, and the library only updates the fields the server lets it change.
- You are affected when a durable is created for the first time under v6: a new
  handler class, a renamed class, a changed `QueueGroup` or `Durable`, a lost
  `"default-worker"` default (step 2), or a durable deleted by hand while no
  process ran. On a Limits stream such a durable skips the history that a 4.x
  durable would have replayed.
- Set `DeliverFrom = DeliverFrom.All` on the attribute when a new consumer
  should process what is already in the stream. It applies only when the durable
  is created.
- A durable that disappears while the service runs is recreated from the last
  ack floor the library saw, so nothing is skipped (some messages may be handled
  twice).

**No default backoff.** v6 never sets a server `Backoff` on a consumer. 5.1 set
one by default. Retry delays are now NAK delays that the library sends after a
handler failure: `RetryDelaysSeconds`, default `[1, 5, 30, 120, 600]` s, the last
entry repeating.

- A message that is never settled (the process crashed or was killed, or the
  handler ran past its time limit) comes back after `AckWait` (30 s by default),
  not after a retry delay. In 5.x the server Backoff also spaced these.
- An existing 5.x durable has its `Backoff` removed, `MaxDeliver` set to -1 and
  `AckWait` updated on the first start. If the server rejects that update, the
  consumer runs with the old config, the heartbeat follows the shortest Backoff
  entry, and the component shows a Warning.
- 4.x NAKed failures with no delay; v6 waits 1 s before the first retry.
- `RetryDelaysSeconds = []` retries at once.

**Auto-ack.** A handler that returns normally is acked, for `INatsHandler<T>` and
for `INatsConsumer<T>`.

- A 4.x handler that forgot to ack used to be redelivered after AckWait, up to
  MaxDeliver times. Now it is acked once. If a handler relied on that to retry,
  make it throw instead.
- A legacy handler that settles itself (Ack, Nak, AckTerminate) is not settled
  again. `AckProgress` does not count as a settle.
- A legacy handler that NAKs and returns (instead of throwing) is not retried
  with the delays: every NAK is a delivery, and the message is dead-lettered
  after `MaxDeliveryAttempts + MaxDeliverySlack` deliveries. Throw to get the
  retry delays.
- A legacy handler that throws without settling follows the retry table (retry
  with delays, then the dead-letter stream).

### Retries and dead letters

- **`MaxDeliveryAttempts` is honoured** (default 3). Released 4.x builds ignored
  it and used a server MaxDeliver of 10. The server cap is now unlimited (-1),
  and the library dead-letters after `MaxDeliveryAttempts` deliveries, or after
  `MaxDeliveryAttempts + MaxDeliverySlack` deliveries without a handler result
  (a crash-loop guard). Deliveries where the handler did not finish (a crash, a
  shutdown) count too, so the handler may run fewer times. To keep the 4.x retry
  count, set `MaxDeliveryAttempts = 10`. `MaxDeliveryAttempts = 1` now really
  means one failure, then the dead letter.
- **Dead letters go to a stream.** Terminal failures are stored in the `DLQ`
  stream (subjects `dlq.>`, 30 days, created if missing). 4.x let the server
  drop them; 5.x sent a core publish to `dlq.{filter}`. The subject is now
  `{DeadLetterSubjectPrefix}.{actual subject}`. The source message is acked only
  after the stream has stored the dead letter. The stored entry carries
  `Nats-Msg-Id = {stream}:{sequence}:{stored}:{durable}` (`stored` is the source
  message's stored time in UTC ticks). `NatsDeadLetters.ReplayAsync` replays
  entries.
- **In-flight 4.x deferrals keep working.** The `Msg-DeferredUntil` header is no
  longer written, but messages that carry it are still honoured on consume until
  v7. The delivery such a message spent waiting does not count against
  `MaxDeliveryAttempts`. Each deferred publish is now its own schedule (5.x kept
  one per subject).

### Consumers

- **AckWait and the heartbeat.** The default AckWait is 30 s (5.x: 5 min; 4.x:
  30 s). A running handler keeps its message with an in-progress heartbeat every
  third of the server's effective AckWait, so long handlers are not redelivered.
- **Handlers have a time limit.** A handler may run for `MaxHandlerSeconds`
  (default `MaxHandlerTime`, 30 minutes) on one message. After that its token is
  cancelled, its heartbeat stops (the message is redelivered after AckWait) and
  the consumer shows a Warning. Raise it on consumers whose handlers
  legitimately run longer.
- **No prefetch.** The worker pulls only for free handler slots
  (`MaxConcurrency`, default 1). A message no longer waits in a buffer, without a
  heartbeat, while another one is handled.
- **An empty `Subject` consumes the whole stream.** 4.x logged a warning and
  skipped a `[NatsConsumer]` without a `Subject`. v6 binds it with no filter, so
  a dormant class such as `[NatsConsumer(Stream = "x")]` starts taking every
  message on the stream after the upgrade (on WorkQueue it can also clash with
  other consumers, on Interest it adds a durable that holds messages). Remove
  the attribute, or set the subject you want. An empty `Subject` is rejected on
  a stream with schedules.
- **A durable of another service is not taken over.** When an existing
  durable's `messaging-nats.owner` metadata names another `ServiceName`, the
  consumer is Faulted instead of rewriting that service's filters. Two services
  that share a queue group and a handler class name must give one of them its
  own `Durable` or `QueueGroup`. A durable written before `ServiceName` was set
  (owner = the queue group) is still accepted.
- **Rolling deploys that add a binding.** The shared durable's filters change for
  every pod as soon as the first new pod starts. Old pods NAK the new subject
  back (1 s) instead of dead-lettering it, and pause their pulls for 3 s so a new
  pod gets it. A message is handed back only while its delivery count is at most
  `MaxDeliverySlack` (5); after that it goes to the dead-letter stream as
  `UnroutableSubject`. Do not restart old pods during such a rollout: an old pod
  writes the old filters back, and messages on the new subject published until a
  new pod starts again are passed over.
- **`InactiveThresholdSeconds` is rejected on Interest streams.** The server
  removes the messages only a deleted durable had not acked, so it would lose
  messages there.
- **Orphan reports need `ServiceName`.** Only a consumer whose owner metadata is
  this service's `ServiceName` is reported as an orphan (Warning, gauge, delete
  command). One owned by a queue group, or a 4.x durable matched by its
  `{queueGroup}_` prefix, is logged at Information as possibly unused, because
  another service may share the queue group. The library never deletes a
  consumer.

### Startup, faults and provisioning

- **Startup is a background pipeline.** Host start returns at once unless
  `WaitUntilStarted = true`. Readiness comes from the health check.
  `RetryOnInitialConnect` defaults to true; set it to false in short-lived jobs,
  so a down server fails fast.
- **A permanent setup error stops the application by default**
  (`OnComponentFaulted = StopApplication`). 4.x logged once and ran on without the
  consumer. Use `FaultAction.Report` in services where a broken consumer must not
  stop the process, together with a health check filter such as
  `NatsHealthFilters.ExcludeConsumers`.
- **Stream provisioning.** A changed definition no longer crashes startup:
  `CreateIfMissing` logs the drift as a Warning. Schedule-enabled streams gain the
  subject `_sched.{Name}.>`. `MaxAge` is unchanged (int seconds). The new modes
  are `VerifyOnly` and `CreateOrUpdate`. Every update keeps the settings
  `StreamDefinition` does not model.

### Headers, logs and telemetry

- **Headers named `Nats-*`, or the library's own `X-*` names, are dropped on
  publish** (see "Headers" in the README), and so are `traceparent` and
  `tracestate`. Forwarding `context.Headers` is now safe. The message carries
  the trace context of its own publish span, so the next consumer's span is a
  child of the handler's publish, not of the first producer.
- **NATS client logs appear.** The hosting LoggerFactory is now kept. The
  per-message debug log of the payload JSON is gone.
- **New telemetry.** The library has its own `ActivitySource` and `Meter`
  (`Messaging.NATS`). Spans, metrics and outcome values are listed in the README.

### Schedules

- **Retrying a schedule after `OutcomeUnknown`.** Pass `ScheduleOptions.ScheduleId`
  and retry with the same id, or use a key with Replace. The 4.x `PublishAsync`
  with a delay cannot pass an id, so a retry of it can leave two pending
  schedules.

## API changes

| 4.x / 5.x | 6.0 |
| --- | --- |
| `IPublisher` registered scoped | Singleton. |
| 5.x `PublishAsync<T>(subject, message, TimeSpan? deferredBy = null)` | Removed. Use the `PublishAsync<T>` overloads, or `ScheduleAsync`. |
| 4.x `PublishAsync(string, object, TimeSpan?)` | Kept, returns `Task`. Moq `It.IsAny<object>()` setups still match. |
| `NatsPublisher(connection, js, logger)` | `NatsPublisher(connection, js, options, logger)`. Resolve `IPublisher` from DI instead. |
| `NatsConsumerAttribute.AckWait` (TimeSpan, 5.x) | `AckWaitSeconds` (int, 0 = 30). |
| `NatsConsumerAttribute.BackOffSeconds` (5.x) | `RetryDelaysSeconds`, applied as NAK delays. No consumer gets a server Backoff. |
| `NatsConsumerAttribute.DeadLetterSubject` (string, empty default) | Nullable; null uses `{prefix}.{subject}`. |
| `NatsConsumerAttribute` (unsealed, one per class) | Sealed, `AllowMultiple = true`. |
| `NatsManager`, `NatsSubscriptionHostedService`, `CreateDurableSubscriber`, `CreateReplyHandler`, the pre-3.0 APIs | Removed. The hosted service is internal. |
| `DefaultQueueGroup = "default-worker"` | `null`. |
| `Assemblies` required | Optional. `HandlerTypes` lists handler classes explicitly. A publish-only service needs neither. |
| 4.x `INatsCache.CreateAsync(key, value, TimeSpan? expiry, ct)` and `UpdateAsync(key, value, revision, TimeSpan? expiry, ct)` | `expiry` removed (5.x already; 4.x ignored it). The TTL is set per bucket in `NatsCacheFactory.CreateAsync(name, defaultTtl)`. A positional token in the expiry slot, or a hand-written `INatsCache` fake, no longer compiles. |
| 4.x `new NatsCacheFactory(INatsConnection)` | `NatsCacheFactory(NatsJSContext)`. Resolve it from DI. |

New options: `ServiceName`, `HandlerTypes`, `StreamProvisioning`,
`DeadLetterStream`, `DeadLetterSubjectPrefix`, `RetryOnInitialConnect`,
`WaitUntilStarted`, `StartTimeout`, `ShutdownTimeout`, `OnComponentFaulted`,
`FaultedRetryInterval`, `PublishTimeout`, `PublishRetryAttempts`,
`MaxDeliverySlack`, `MaxHandlerTime`. New `StreamDefinition` properties:
`MaxBytes`, `MaxMsgs`, `Replicas`, `Storage`, `Discard`, `DuplicateWindow`, and
the `Scheduled(...)` and `DeadLetter(...)` factories. New attribute properties:
`MessageType`, `Durable`, `AckWaitSeconds`, `MaxHandlerSeconds`,
`MaxConcurrency`, `MaxMsgs`, `MaxAckPending`, `DeliverFrom`,
`InactiveThresholdSeconds`, `RetryDelaysSeconds`.

New public types: `INatsHandler<T>`, `MessageContext<T>`, `DeliverFrom`,
`PublishOptions`, `PublishResult`, `ScheduleOptions`, `ScheduleMode`,
`ScheduleResult`, `PublishFailure`, `NatsPublishException`,
`NatsMessagingConfigurationException`, `StreamProvisioning`, `FaultAction`,
`NatsMessagingStatus`, `ComponentStatus`, `ComponentKind`, `ComponentState`,
`NatsHealthFilters`, `NatsDeadLetters`, `ReplayOptions`, `ReplayResult`,
`ReplaySkip`, `NatsMessagingTelemetry`, and in `Messaging.NATS.Testing`:
`InMemoryPublisher`, `PublishedMessage`, `CancelledSchedule`, `RecordedRequest`,
`TestMessageContext`.

Unchanged: the `INatsConsumer<T>` signature, `INatsRequestHandler`,
`[NatsRequestHandler]`, `RequestAsync`, the reply JSON format, `INatsCache` and
`NatsCacheFactory` since 5.x, `INatsConnection` / `NatsConnection` /
`NatsJSContext` in DI, `SerializerContext` behaviour, `StreamDefinition.MaxAge`,
the message wire format, and existing durable names.

## Known limitations

- Request handlers subscribe without a queue group, as in 4.x: every instance
  answers each request.
- A keyed schedule gives at most one pending schedule per key, not one delivery
  per key. Near the fire time a Replace or Cancel can lose the race, and after a
  stream leader change one schedule can fire twice with the same `ScheduleId`.
  See "Schedules" in the README for the guard.
- `MessageContext.EnqueuedTime` has millisecond precision (NATS.Net truncates
  the server timestamp).
