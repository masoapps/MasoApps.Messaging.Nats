# MasoApps.Messaging.Nats

NATS JetStream messaging for .NET hosts. You write handler classes and mark them
with an attribute. The library creates the durable consumers, pulls and settles
messages, retries failures and dead-letters what cannot be handled. It also gives
you an acked publisher, message schedules, health checks and OpenTelemetry.

- .NET 10, NATS.Net 2.7.3.
- Namespace: `Messaging.NATS`.
- nats-server 2.11 or later. Message schedules need 2.12 or later.
- Tested on one server and on a 3-node R3 cluster (see "Running the tests").

Upgrading from 4.x or 5.x? Read [MIGRATION-v6.md](MIGRATION-v6.md) first. v6
changes publish, retry and ack behaviour. Release notes are in
[CHANGELOG.md](CHANGELOG.md).

## Contents

- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Streams and provisioning](#streams-and-provisioning)
- [Consumers](#consumers)
- [Concurrency, pulling and the heartbeat](#concurrency-pulling-and-the-heartbeat)
- [Publishing](#publishing)
- [Schedules](#schedules)
- [Dead letters and replay](#dead-letters-and-replay)
- [Health, readiness and faults](#health-readiness-and-faults)
- [Graceful shutdown](#graceful-shutdown)
- [OpenTelemetry](#opentelemetry)
- [Orphaned consumers](#orphaned-consumers)
- [Request/reply and the cache](#requestreply-and-the-cache)
- [Testing your code](#testing-your-code)
- [Running the tests](#running-the-tests)

## Quick start

```bash
dotnet add package MasoApps.Messaging.Nats
```

```csharp
builder.Services.AddNatsMessaging(options =>
{
    options.NatsUrl = "nats://localhost:4222";
    options.ServiceName = "orders-worker";
    options.HandlerTypes = [typeof(OrderPlacedHandler)]; // or Assemblies = [typeof(Program).Assembly]
    options.StreamDefinitions.Add(new StreamDefinition
    {
        Name = "ORDERS",
        Subjects = ["orders.>"],
        Replicas = 3
    });
});

builder.Services.AddHealthChecks().AddNatsMessagingCheck(tags: ["ready"]);
```

A handler:

```csharp
[NatsConsumer(Stream = "ORDERS", Subject = "orders.placed")]
public sealed class OrderPlacedHandler(IOrderService orders) : INatsHandler<OrderPlaced>
{
    public Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken cancellationToken) =>
        orders.ReserveStockAsync(context.Message, context.MessageId, cancellationToken);
}
```

- Return normally: the message is acked.
- Throw: the message is retried after a delay, then dead-lettered.
- The durable is `orders-worker_OrderPlacedHandler`: `{queue group}_{class}`,
  where the queue group falls back to `ServiceName`.

Publish with `IPublisher` (a singleton):

```csharp
var result = await publisher.PublishAsync("orders.placed", order,
    new PublishOptions { MessageId = order.OrderId });
```

The call returns when the stream has stored the message, or throws
`NatsPublishException`.

## Configuration

| Option | Default | Meaning |
| --- | --- | --- |
| `NatsUrl` | required | Server URL. Several URLs are comma separated. |
| `CredsFilePath` | null | NATS credentials file. |
| `ServiceName` | null | Connection name, consumer owner metadata, and the queue group fallback. Set it: without it the owner is the queue group, so two services that share a queue group and a class name share one durable. |
| `DefaultQueueGroup` | null | Queue group for attributes that do not set `QueueGroup`. |
| `HandlerTypes` | empty | Handler classes to register. |
| `Assemblies` | empty | Assemblies to scan for classes with `[NatsConsumer]` or `[NatsRequestHandler]`. |
| `StreamDefinitions` | empty | Streams this service declares (see "Streams and provisioning"). |
| `StreamProvisioning` | `CreateIfMissing` | How declared streams are provisioned. |
| `DeadLetterStream` | `DLQ`, `dlq.>`, 30 days | The dead-letter stream. Null: another service owns it. |
| `DeadLetterSubjectPrefix` | `dlq` | Default dead-letter subject is `{prefix}.{subject}`. |
| `SerializerContext` | null | Source-generated JSON context. |
| `JsonSerializerOptions` | null | Reflection JSON options, when there is no `SerializerContext`. |
| `PublishTimeout` | 5 s | The whole time budget of one publish, retries included. |
| `PublishRetryAttempts` | 2 | Extra publish attempts inside that budget, with the same message id. |
| `MaxDeliverySlack` | 5 | Deliveries above `MaxDeliveryAttempts` before a forced dead letter. Also the most hand-backs of a message no binding matches. |
| `MaxHandlerTime` | 30 min | The longest a handler may run on one message, unless the attribute sets `MaxHandlerSeconds`. Zero: no limit. |
| `RetryOnInitialConnect` | true | Keep retrying the first connection. Set false for short-lived jobs. |
| `ConfigureNatsOpts` | null | Runs last on the NATS.Net `NatsOpts`, for settings the library does not expose, such as `ReconnectWaitMin`. |
| `WaitUntilStarted` | false | Host start waits until every component is running. |
| `StartTimeout` | 60 s | How long `WaitUntilStarted` waits. |
| `ShutdownTimeout` | 25 s | How long shutdown waits for running handlers. |
| `OnComponentFaulted` | `StopApplication` | What a permanent error does (see "Health, readiness and faults"). |
| `FaultedRetryInterval` | 2 min | How often a faulted component retries. |

`AddNatsMessaging` checks the options and every attribute, and throws before the
host starts. Among other things it rejects:

- a missing `NatsUrl`, or a service with consumers and no queue group
- an invalid stream definition, or two declared streams whose subjects overlap
- a dead-letter subject that `DeadLetterStream` does not capture
- conflicting consumer-level settings across one class's attributes
- overlapping filter subjects within one durable, or two classes with the same durable name on one stream
- a filter that would receive schedule holders (see "Schedules")
- `DeliverFrom.New` on a WorkQueue stream
- `InactiveThresholdSeconds` on a declared Interest stream

JSON serialisation works with no setup. To change it:

```csharp
// Reflection-based, with your options
options.JsonSerializerOptions = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

// Source-generated
[JsonSerializable(typeof(OrderPlaced))]
public partial class AppJsonContext : JsonSerializerContext;

options.SerializerContext = AppJsonContext.Default;
```

## Streams and provisioning

A `StreamDefinition` describes a stream this service declares:

| Property | Default | Notes |
| --- | --- | --- |
| `Name`, `Subjects` | required | |
| `Retention` | `Limits` | `Limits`, `Interest` or `Workqueue`. Cannot change once created. |
| `MaxAge` | -1 | Seconds. 0 or less means no limit. |
| `MaxBytes`, `MaxMsgs` | -1 | No limit. |
| `Replicas` | 1 | Use 3 in production. |
| `Storage` | `File` | Cannot change once created. |
| `Discard` | `Old` | |
| `DuplicateWindow` | server default (2 min) | How long a repeated message id is dropped. |
| `AllowMsgSchedules` | false | See "Schedules". Can be turned on, never off. |

`StreamDefinition.Scheduled(name, subjects)` (see "The SCHEDULED stream
pattern") and `StreamDefinition.DeadLetter(...)` build the two standard shapes:

```csharp
options.StreamProvisioning = StreamProvisioning.CreateOrUpdate;

var timers = StreamDefinition.Scheduled("TIMERS", "timers.>");
timers.Replicas = 3;
options.StreamDefinitions.Add(timers);

options.StreamDefinitions.Add(new StreamDefinition
{
    Name = "EVENTS",
    Subjects = ["events.>"],
    Retention = StreamConfigRetention.Limits,
    MaxAge = (int)TimeSpan.FromDays(7).TotalSeconds,
    Replicas = 3,
    DuplicateWindow = TimeSpan.FromMinutes(10)
});

options.DeadLetterStream!.Replicas = 3;
```

The library copies `StreamDefinitions` at registration. It never changes your
list or its items.

### Provisioning modes

| Mode | Stream missing | Stream exists |
| --- | --- | --- |
| `CreateIfMissing` (default) | Create it. | Compare. Differences are logged as a Warning and shown as a component Warning. Only one change is applied: when the definition turns schedules on, the library turns them on and adds the holder subject. |
| `VerifyOnly` | Wait (Retrying) until the owner creates it. | Compare and warn, as above. Declared schedules that the stream lacks: Faulted. |
| `CreateOrUpdate` | Create it. | Update it to match the definition. An immutable change (Retention, Storage) is Faulted. |

- Every update starts from the server's current config and changes only the
  fields `StreamDefinition` models. Settings it does not model (AllowDirect,
  AllowMsgTTL, MaxMsgsPerSubject, sources, metadata and so on) are kept.
- An update never turns off `AllowMsgSchedules` and never removes the holder
  subject.
- An update never lowers the `MaxAge` of a stream that holds schedules, because
  the server deletes every pending schedule older than the new MaxAge. The
  server keeps its MaxAge, and the component shows a Warning with the number of
  pending schedules. Lower it by hand (`nats stream edit`) once no schedule longer
  than the new MaxAge is pending.
- Turning schedules on (either mode) is refused (Faulted) while a consumer of the
  stream has no filter, or a filter that matches `_sched.{Name}.>`, because it
  would take every pending schedule as soon as it is stored. The error names
  those consumers. This includes consumers of other services.
- A declared stream that disappears while the service runs (a Memory stream
  after a server restart, a delete by hand) is provisioned again by the next
  consumer attempt on it. The stream component shows Retrying until then.
  `VerifyOnly` only waits for the owner. A service that only publishes to the
  stream does not notice.
- Compared fields: Subjects (as a set), Retention, MaxAge, MaxBytes, MaxMsgs,
  Replicas, Storage, Discard, DuplicateWindow, AllowMsgSchedules.
- The dead-letter stream is always `CreateIfMissing`, whatever the mode.

Use `CreateOrUpdate` only in the service that owns the stream.

### Undeclared streams

A consumer may use a stream that is not in `StreamDefinitions`. The library never
creates or changes such a stream. The consumer waits (Retrying) until it exists.
This is how a service consumes a stream that another service owns. The attribute
rules for schedule and WorkQueue streams are then checked when the consumer
starts, and a broken rule makes the consumer Faulted.

## Consumers

### Handlers

Implement `INatsHandler<T>` and add `[NatsConsumer]`. Handler classes are
transient. Each message gets its own DI scope.

`MessageContext<T>` carries the message and its delivery:

| Property | Meaning |
| --- | --- |
| `Message` | The payload. Never null: a null or unreadable payload goes to the dead-letter stream without calling the handler. |
| `Subject` | The subject the message was published to. |
| `Headers` | The headers; empty when there are none. |
| `MessageId` | A stable id for idempotency (below). |
| `CorrelationId` | `X-Correlation-Id`. |
| `SentTime` | When the publisher sent it (`X-Sent-Time`), else `EnqueuedTime`. |
| `EnqueuedTime` | When the stream stored it. For a fired schedule this is the fire time. Millisecond precision. |
| `DeliveryCount`, `IsRedelivery` | 1 on the first delivery. |
| `Stream`, `Consumer`, `StreamSequence` | Where it came from. |
| `ScheduleId`, `ScheduleKey`, `ScheduledFor`, `IsScheduled` | Fired schedules only. |

`MessageId` is the first of:

1. `X-Original-Msg-Id`: a replayed dead letter keeps the id it had when it failed.
2. `Nats-Msg-Id`: the id of an immediate publish.
3. `X-Schedule-Id`: a fired schedule. Two fires of one schedule share it.
4. `{stream}:{sequence}`.

Make handlers idempotent on `MessageId`. A message can be delivered more than
once: after a crash, a lost ack, a shutdown hand-off, or a duplicate schedule
fire.

### The attribute

```csharp
[NatsConsumer(Stream = "ORDERS", Subject = "orders.placed", MessageType = typeof(OrderPlaced), MaxConcurrency = 8)]
[NatsConsumer(Stream = "ORDERS", Subject = "orders.paid", MessageType = typeof(PaymentCaptured),
    MaxDeliveryAttempts = 10, RetryDelaysSeconds = [5, 30, 300])]
public sealed class OrderActivity(IOrderService orders) : INatsHandler<OrderPlaced>, INatsHandler<PaymentCaptured>
{
    public Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken cancellationToken) =>
        orders.ReserveStockAsync(context.Message, context.MessageId, cancellationToken);

    public Task HandleAsync(MessageContext<PaymentCaptured> context, CancellationToken cancellationToken) =>
        orders.CaptureAsync(context.Message, cancellationToken);
}
```

Binding-level properties may differ between the attributes of a class:

| Property | Default | Meaning |
| --- | --- | --- |
| `Subject` | whole stream | Filter subject; wildcards allowed. |
| `MessageType` | the only handled type | Required when the class handles more than one type. |
| `MaxDeliveryAttempts` | 3 | Deliveries before the dead letter (see "Retries and dead letters"). |
| `RetryDelaysSeconds` | `[1, 5, 30, 120, 600]` | NAK delay after failure N (the last entry repeats). |
| `DeadLetterSubject` | `{prefix}.{subject}` | Where failures go. |

Consumer-level properties must agree across the class's attributes on one
stream. Set each one on one attribute, or to the same value on several. Integers
use 0 for "not set".

| Property | Default (0 / empty) | Meaning |
| --- | --- | --- |
| `Stream` | required | |
| `QueueGroup` | `DefaultQueueGroup`, then `ServiceName` | Prefix of the durable name. |
| `Durable` | `{QueueGroup}_{ClassName}` | An explicit durable name. |
| `AckWaitSeconds` | 30 | Server redelivery timeout. A running handler holds its message with a heartbeat, so this does not limit handler time. |
| `MaxHandlerSeconds` | `MaxHandlerTime` (30 min) | The longest a handler may run on one message (see "Handler time limit"). |
| `MaxConcurrency` | 1 | Handlers running at once, per process, shared by the class's bindings on the stream. |
| `MaxMsgs` | `MaxConcurrency` | Most messages per pull. A pull asks only for free slots, so a value above `MaxConcurrency` changes nothing: there is no prefetch. |
| `MaxAckPending` | 1000 | Unacked messages across all processes. |
| `DeliverFrom` | `Default` | Where a new durable starts (below). |
| `InactiveThresholdSeconds` | never | The server deletes an idle durable. Rejected on Interest streams. |

### Durables and multi-type classes

- One durable per (class, stream). All bindings of a class on a stream share it,
  with one filter subject each, and the worker dispatches by subject. Adding or
  removing a message type changes the durable's filters in place; the name stays.
- The name is `Durable`, or `{queue group}_{ClassName}`, the same as in 4.x, so
  live durables keep their names. `. * > \ /` and spaces become `_`.
- Concurrency is per durable, like a receive endpoint: size `MaxConcurrency`
  for the whole class.
- Filters within one durable must not overlap.

New durables get MaxDeliver -1, no server Backoff, explicit acks, and owner
metadata (`messaging-nats.owner`, `messaging-nats.handler`). Where they start:

| `DeliverFrom` | Limits / Interest stream | WorkQueue stream |
| --- | --- | --- |
| `Default` | New | All |
| `All` | All | All |
| `New` | New | Rejected (the server requires All) |

`DeliverFrom` only applies when the durable is first created. An existing
durable keeps its DeliverPolicy and DeliverGroup. On start the library updates
only the fields the server lets it change (AckWait, MaxDeliver, MaxAckPending,
filters, InactiveThreshold, metadata). If the server rejects the update, the
consumer runs with the server's config and shows a component Warning.

A durable whose `messaging-nats.owner` metadata names another service (another
`ServiceName`, not this service's queue group) is never updated: the consumer is
Faulted with both owners in the error. Two services with the same queue group and
class name would otherwise share one durable and overwrite each other's filters.
Give one of them its own `Durable` or `QueueGroup`, or delete the durable if it
really moved.

If a durable disappears while the service runs (deleted by hand, or by
`InactiveThreshold`), the library recreates it from the last ack floor it saw,
or from where a new durable started when nothing was acked yet. Some messages may
be handled twice; none are skipped on a Limits or WorkQueue stream. If the stream
itself was deleted and created again, the durable starts at the beginning of the
new stream. A durable deleted while no process ran is recreated as a new one, so
use `InactiveThresholdSeconds` only on WorkQueue streams, or on Limits streams
with `DeliverFrom.All`. It is rejected on Interest streams (at registration for a
declared stream, as a Faulted consumer otherwise): when the server deletes the
durable there, it also removes every message only that durable had not acked,
and stores none for it until it exists again.

**Rolling deploys.** All pods of a service share one durable, so the first new pod
that starts changes its filters for every pod. When a release adds a binding, old
pods receive the new subject too. They do not dead-letter it: a message no binding
matches is NAKed back (1 s) for another pod, and the pod that handed it back
pauses its pulls for 3 s, so the redelivery goes to a pod that is pulling. Every
hand-back is a delivery the server counts, so a message is handed back only while
its delivery count is at most `MaxDeliverySlack` (5); a pod with the binding then
always runs its handler (the crash-loop guard only acts past `A + S`). After that
it is dead-lettered as `UnroutableSubject`. An old pod that restarts during the
rollout writes the old filters back; messages on the new subject published until
a new pod starts again are passed over by the durable (on an Interest stream they
are removed). Do not restart old pods mid-rollout, or ship the new binding in a
release before the code that publishes to it.

### Retries and dead letters

For each delivery `n` (the server's delivery count), with `A = MaxDeliveryAttempts`
and `S = MaxDeliverySlack`:

| Situation | Result |
| --- | --- |
| No binding matches the subject (a rolling deploy) | NAK for 1 s, for another pod, and pause pulls for 3 s; dead letter (`UnroutableSubject`) once `n > S`. |
| A targeted replay for another durable | Ack; the handler is not called. |
| A 4.x `Msg-DeferredUntil` header in the future | NAK until that time. |
| `n > A + S` | Dead letter without calling the handler (crash-loop guard). |
| Null or unreadable payload | Dead letter without calling the handler. |
| Handler returns | Ack. |
| Handler throws `PoisonMessageException` | Dead letter now. |
| Handler throws `OperationCanceledException` during shutdown | NAK with no delay, for another process. |
| Handler returns without settling after shutdown cancelled its token | NAK with no delay, for another process (it may have stopped half way). |
| Handler runs longer than `MaxHandlerSeconds` | Its token is cancelled and its heartbeat stops (see "Handler time limit"); a throw or return after that is a failure. |
| Handler throws, and `n >= A` | Dead letter. |
| Handler throws, otherwise | NAK with `RetryDelaysSeconds[n - 1]` (the last entry repeats). |

`A` counts deliveries, not only handler failures. A delivery where the handler
did not finish (a crash, a shutdown) still counts, so the handler can run fewer
than `A` times. It never runs more than `A + S` times. The server cap is
unlimited, so the server never drops a message on its own. For a 4.x
`Msg-DeferredUntil` message, the delivery spent waiting does not count, so it
gets its `A` attempts once it is due.

The retry delays are NAK delays the library sends. No consumer has a server
Backoff. So a message that was never settled (its process died, or the
handler time limit passed) comes back after AckWait, not after a retry delay.
An empty `RetryDelaysSeconds` retries at once.

```csharp
if (string.IsNullOrEmpty(context.Message.OrderId))
{
    throw new PoisonMessageException("OrderId is missing");
}
```

### 4.x handlers (`INatsConsumer<T>`)

`INatsConsumer<T>` still works, with the same signature:

```csharp
[NatsConsumer(Stream = "ORDERS", Subject = "orders.legacy", QueueGroup = "order-worker")]
public sealed class LegacyOrderConsumer : INatsConsumer<LegacyOrder>
{
    public async Task HandleMessageAsync(INatsJSMsg<LegacyOrder> message, CancellationToken cancellationToken)
    {
        if (message.Data is null)
        {
            await message.AckTerminateAsync(cancellationToken: cancellationToken);
            return;
        }

        // Returning without a settle acks the message.
    }
}
```

- If the handler settles (Ack, Nak, Term), the library does nothing more.
- If it returns without settling, the library acks.
- If it throws without settling, the retry table above applies.
- A null or unreadable payload reaches the handler as `Data == null`, as in 4.x.

## Concurrency, pulling and the heartbeat

| Setting | Scope | Default |
| --- | --- | --- |
| `MaxConcurrency` | Handlers running at once for one durable, in one process. All bindings of the class on that stream share them. | 1 |
| `MaxAckPending` | Unacked messages for one durable, across every process. | 1000 |
| `MaxMsgs` | Most messages per pull request. | `MaxConcurrency` |

- To scale out, run more processes. They share the durable, and the server
  spreads its messages across them.
- `MaxConcurrency = 1` keeps stream order within one process. With several
  processes, or a higher `MaxConcurrency`, handlers run in parallel and order
  is not kept.
- `MaxAckPending` must be at least `MaxConcurrency` (checked at registration).
  Size it for every process together: 4 processes with `MaxConcurrency = 50`
  need at least 200.
- `IPublisher` is a thread-safe singleton. Handlers may publish in parallel.

### Pulling

The worker pulls only for free handler slots. There is no prefetch buffer, so a
message delivered to the process always has a handler running for it, and no
message ages past its AckWait while it waits for a slot. A slot freed while a
pull is open is used by the next pull, at most 5 s later. An idle durable costs
one pull request every 5 s per process.

### The heartbeat

While the handler runs, the library sends an in-progress ack every third of the
server's effective AckWait (the durable's AckWait, or its shortest Backoff entry
when a rejected update left one in place), so a long handler is not
redelivered. The interval follows the server config the worker reads, and a
component Warning shows when the server waits less than `AckWaitSeconds`.
`AckWaitSeconds` therefore sets how fast a message comes back after a process
dies, not how long a handler may run.

### Handler time limit

A handler that never returns (a deadlock, an HTTP call with no timeout) would
hold its message and its slot for good. So a handler may run for
`MaxHandlerSeconds` (default `MaxHandlerTime`, 30 minutes) on one message. After
that the library cancels the handler's token, stops the heartbeat (the server
redelivers the message after AckWait), logs an Error and shows a component
Warning (health Degraded) until the handler returns. A throw or a return after
that counts as a failed attempt. A handler that ignores its token keeps its slot
until it returns. Raise the limit on consumers whose handlers legitimately run
longer (long syncs, report generation).

## Publishing

### Acked by default

`PublishAsync` sends to the stream that captures the subject and waits for the
stream's acknowledgement (one round trip). It returns
`PublishResult(Stream, Sequence, Duplicate, MessageId)`. `Duplicate = true`
means the stream already had this message id; that is a success.

```csharp
try
{
    var result = await publisher.PublishAsync("orders.placed", order,
        new PublishOptions { MessageId = order.OrderId });
    logger.LogInformation("Stored as {Stream}:{Sequence}", result.Stream, result.Sequence);
}
catch (NatsPublishException e) when (e.OutcomeUnknown)
{
    // The message may be stored. Retrying with the same MessageId is safe: the stream drops a duplicate.
    throw;
}
```

**Latency.** Every acked publish waits for one round trip to the stream leader
(on a 3-node cluster, also for the replicas to store it). A loop that awaits
publishes one by one pays that round trip per message. When the publishes are
independent, run them at once. They are then stored in no fixed order:

```csharp
await Task.WhenAll(orders.Select(order =>
    publisher.PublishAsync("orders.placed", order, new PublishOptions { MessageId = order.OrderId })));
```

Await every publish, or return its task to your caller. A failed publish
throws, and a task nobody awaits loses that error and the message with it.

### Budget and retries

- `Timeout` (default `PublishTimeout`, 5 s) bounds the whole call: every attempt
  and the wait for a reconnect.
- `RetryAttempts` (default `PublishRetryAttempts`, 2) extra attempts run inside
  that budget, all with the same message id.
- "No responders" (no stream, or a leader election) and HTTP 503 answers are
  retried after 250 ms, doubling. A reply timeout is retried at once. The same
  holds for the JetStream API calls of a schedule or cancel (the stream lookup,
  the CreateIfAbsent lookup, the purge): what is left after the budget is a
  `NatsPublishException` too.
- With retries on and no `MessageId`, the library generates one, so its own
  retries are deduplicated. With `RetryAttempts = 0` and no `MessageId`, no id is
  sent.

| `NatsPublishException.Failure` | Meaning | Stored? |
| --- | --- | --- |
| `NoStream` | No stream answered within the budget. | No |
| `Rejected` | The stream refused it; see `ErrorCode`. | No |
| `Timeout` (`OutcomeUnknown`) | No answer within the budget. | Maybe |

A stream that cannot hold a schedule (err_code 10188 or 10190) throws
`NatsMessagingConfigurationException` instead. Cancelling the call throws
`OperationCanceledException`; the outcome is then unknown too.

### Must-deliver publishes

Pass a stable business id as `MessageId` (an upload id, an order id). Then a
caller retry after `Timeout`, a library retry, or a retry after a crash stores
the message once. This holds only inside the stream's `DuplicateWindow`, so keep
that window longer than your own retry horizon. On a 3-node cluster, a publish
during a stream leader change finishes or throws within its `Timeout`, and a
retry with the same id leaves exactly one stored message (tested).

### Fire and forget

`FireAndForget = true` is a core NATS publish: no ack and no retry. Use it only
for messages you can lose. No message id is generated, but a `MessageId` you set
is still sent, and the stream still deduplicates on it: a later acked publish
with the same id returns `Duplicate = true`. Await it anyway: it throws
`NatsPublishException` (`Timeout`) when the connection cannot take the message
within the budget.

```csharp
await publisher.PublishAsync("orders.metrics", order, new PublishOptions { FireAndForget = true });
```

### Headers

`PublishOptions.Headers` is copied, never changed. The copy drops every `Nats-*`
header and the library's own `X-*` headers (`X-Sent-Time`, `X-Schedule-*`,
`X-Scheduled-For`, `X-Original-*`, `X-Replay*`, `X-Failure-*`, `X-Failed-At`,
`X-Stream`, `X-Stream-Sequence`, `X-Consumer`, `X-Delivery-Count`). So a handler
can forward `context.Headers` without purging a schedule, rolling up a subject or
being dropped as a duplicate. `traceparent` and `tracestate` are dropped too, so a
forwarded trace context never hides the current span. The library adds
`X-Correlation-Id` (`CorrelationId`, else a forwarded one), `X-Sent-Time`, and
the `traceparent` of the publish span (else of `Activity.Current`).

### The 4.x overload

`PublishAsync(string subject, object message, TimeSpan? deferredBy)` stays:

- null, zero or negative `deferredBy`: an immediate acked publish
- positive: an unkeyed schedule `deferredBy` from now
- it serialises with the message's runtime type and uses `PublishTimeout`

The generic overloads serialise with the static type `T`. A message typed as a
base class or interface loses its derived properties there (or throws, with a
`SerializerContext` that has no entry for the interface). Pass the concrete
type, or keep such calls on the 4.x overload with `(TimeSpan?)null`.

A positive `deferredBy` is a schedule, so it can also throw what `ScheduleAsync`
throws: `NatsMessagingConfigurationException` when no stream that allows schedules
captures the subject (set `AllowMsgSchedules` on its stream), and
`ArgumentOutOfRangeException` when the delay is at or beyond the stream's
`MaxAge`. A 4.x caller that catches only `NatsPublishException` misses these.

## Schedules

A schedule stores the message now and delivers it at `deliverAt`, never earlier.
It uses JetStream message schedules (ADR-51) and needs nats-server 2.12 or later.

### The SCHEDULED stream pattern

Give timers a stream of their own, declared by the service that owns it (with
`CreateOrUpdate`). Other services that schedule onto it or consume from it do
not declare it: they find it on the server.

```csharp
var timers = StreamDefinition.Scheduled("TIMERS", "timers.>");
timers.Replicas = 3;
options.StreamDefinitions.Add(timers);
```

`Scheduled` gives WorkQueue retention, schedules on, no age, count or byte
limits. The stream also captures `_sched.TIMERS.>`, where pending schedules
(holders) wait. A schedule on any subject under `timers.>` waits there and is
then stored on its own subject in the same stream, where one consumer per
subject takes it.

You can also turn schedules on for an existing stream with
`AllowMsgSchedules = true`. Under `CreateIfMissing` the next start turns them on
and adds the holder subject, and changes nothing else. Rules the library
enforces on any stream with schedules:

- Retention is WorkQueue or Limits, never Interest (Interest drops holders).
- No `MaxMsgs` or `MaxBytes` limit, and `Discard` is not New (eviction would
  delete pending holders).
- `MaxAge`, if set, is longer than every delay. A delay at or beyond `MaxAge`
  throws `ArgumentOutOfRangeException`. `CreateOrUpdate` never lowers it (see
  "Provisioning modes").
- No consumer filter may match `_sched.{stream}.>`, and no consumer on the stream
  may be unfiltered. Otherwise it would receive the holders.
- Timer subjects live in this stream only: the target must be in the same stream
  as its holder. WorkQueue allows one consumer per subject. To fan a timer out,
  use Limits retention with a `MaxAge` longer than the longest delay.

### Scheduling

```csharp
// One pending check per journey: a new call replaces the pending one.
var check = await publisher.ScheduleAsync("timers.journey.check", new JourneyCheck(journeyId),
    DateTimeOffset.UtcNow.AddMinutes(5), new ScheduleOptions { Key = journeyId });

// The first schedule wins; later calls return Created = false and the pending ScheduleId.
var arriving = await publisher.ScheduleAsync("timers.session.arriving", new SessionArriving(sessionId),
    DateTimeOffset.UtcNow.AddSeconds(90),
    new ScheduleOptions { Key = sessionId, Mode = ScheduleMode.CreateIfAbsent });

// Removes the pending schedule. True when one was removed.
var cancelled = await publisher.CancelScheduleAsync("timers.notification.due", notificationId);
```

- No `Key`: a unique key, so any number of schedules can be pending per subject.
- A key is one subject token of `[A-Za-z0-9_-]`, up to 128 characters. The
  target subject must be literal.
- `Replace` (the default): the new schedule replaces the pending one.
- `CreateIfAbsent`: a pending schedule is kept. Use it only when the first
  schedule's payload stays valid until it fires. If the pending schedule fires
  while the call reads it, the key is free and the call creates its own
  schedule (`Created = true`).
- A `deliverAt` in the past fires at once (about 250 ms later).
- `ScheduleResult.ScheduleId` is unique per call. The fired message carries it.
- **Retrying after `OutcomeUnknown`.** The schedule may be stored. A keyed
  Replace is safe to call again. For anything else, pass your own
  `ScheduleOptions.ScheduleId` (a stable business id) and retry with the same
  one: the stream drops the retry as a duplicate within its `DuplicateWindow`.
  An unkeyed schedule retried without its id (the 4.x `PublishAsync` with a
  delay cannot pass one) can leave two pending schedules that both fire.

A fired message is a normal stream message with `X-Schedule-Id`,
`X-Scheduled-For`, `X-Schedule-Key` (keyed only), your headers, and
`Nats-Scheduler` naming its holder. The handler sees `ScheduleId`, `ScheduleKey`
and `ScheduledFor`, and `MessageId` is the schedule id. Fired messages reach
JetStream consumers only, not core subscribers.

### Guarantees

A key gives **at most one pending schedule per key**, not at most one delivery
per key. Each schedule has its own holder subject
(`_sched.{stream}.{subject}.{key}.{scheduleId}`). A Replace stores the new holder
first, then purges the key's older holders, and a Cancel purges every holder of
the key. Storing a fire purges only its own holder, so a Replace is never lost.
Near the fire time:

- A Replace sent while the old schedule fires gives two deliveries for one key
  (old, then new). The same happens for a Replace sent after the fire, before
  the fired message is handled.
- A Cancel can return true while the fire it raced is still delivered.
- If a Replace stores the new schedule but cannot purge the old one, it throws
  `NatsPublishException` (`Timeout`, outcome unknown): both may fire, and a call
  again replaces both.
- After a stream leader change, one holder can fire twice. The new leader starts
  with an empty in-flight guard (nats-server 2.12.1 clears it on every leader
  change). Both fires carry the same `X-Schedule-Id`, so handlers that are
  idempotent on `MessageId` handle it once.

What we measured on a local 3-node cluster with nats-server 2.12.1:

| Scenario | Result |
| --- | --- |
| Stream leader stepdown before the fire time | Each schedule fired once, on time. |
| Leader killed before the fire time, back after it | Each fired once, 2-5 s late (leader election). |
| 20 stepdowns from 200 ms before to 200 ms after the fire time | Each fired once. |
| 100 schedules over 10 s with a stepdown every 2 s | Each fired once. |
| 2,000 schedules due at once, leader killed while they fire | Each fired once, up to 9 s late. |
| Every node stopped while schedules were due, then restarted | Each fired once after the restart. |
| Replace or Cancel from 300 ms before to 300 ms after the fire time | Always one of the documented outcomes above. |

On a single server, 1,000 keys each replaced once from 250 ms before to 250 ms
after their fire time kept every replacement pending (tested).

No schedule fired early and no duplicate fire was seen. The duplicate case above
stays documented: it follows from the server code, and a slower network widens
its window.

**The ScheduleId guard.** If a timer can be replaced or cancelled from outside
its own handler (an API call, a cancel), keep the latest `ScheduleId` with the
entity, and ignore a fire whose `ScheduleId` differs:

```csharp
[NatsConsumer(Stream = "TIMERS", Subject = "timers.notification.due")]
public sealed class NotificationDueHandler(INotificationStore store, INotificationSender sender)
    : INatsHandler<NotificationDue>
{
    public async Task HandleAsync(MessageContext<NotificationDue> context, CancellationToken cancellationToken)
    {
        var id = context.Message.NotificationId;
        if (context.ScheduleId != await store.CurrentScheduleIdAsync(id, cancellationToken))
        {
            return; // Replaced or cancelled after this fire started: not the latest schedule.
        }

        await sender.SendAsync(id, cancellationToken);
    }
}
```

with the schedule side:

```csharp
var result = await publisher.ScheduleAsync("timers.notification.due", new NotificationDue(notificationId), at,
    new ScheduleOptions { Key = notificationId }, cancellationToken);
await store.SaveScheduleIdAsync(notificationId, result.ScheduleId, cancellationToken);
```

A timer that reschedules itself from its own handler (with a Replace on its own
key) needs no guard: by then the fire has removed its holder.

### Server checks

The publisher checks that `AllowMsgSchedules` is on and the stream passes the
rules above, and reads its `MaxAge`. It reuses the check for 15 s, then reads the
stream again, so a changed MaxAge or rule is seen. It logs a Warning once per
stream when the stream's `_nats.level` metadata is below 2. A failed check throws
`NatsMessagingConfigurationException` and is tried again on the next call.

## Dead letters and replay

### The dead-letter stream

Dead letters are stored in `DeadLetterStream` (default `DLQ`, subjects `dlq.>`,
30 days, a 1 hour DuplicateWindow, created if missing). Set `Replicas = 3` in
production. The subject is the attribute's `DeadLetterSubject`, or
`{DeadLetterSubjectPrefix}.{original subject}`.

- The write waits for the stream's acknowledgement. Only then is the source
  message acked, with a confirmed ack.
- If the write fails, it is retried after 1 s, 2 s and 4 s while the message is
  held. Then the message is NAKed for 30 s, an Error is logged and
  `messaging.nats.dead_letter.failures` is counted. The message stays in its
  stream and comes back until a write succeeds. Nothing is dropped.
- The write's message id is `{stream}:{sequence}:{stored}:{durable}`, where
  `stored` is the source message's stored time in UTC ticks. A repeated write of
  the same message by the same durable is stored once (inside the DLQ stream's
  DuplicateWindow). Two durables that fail the same message get one entry each,
  and a stream deleted and created again (sequences restart at 1) does not make
  a new failure look like a repeat.
  Startup logs a Warning when a consumer's AckWait is longer than that window.

The entry has the original payload bytes, the original headers without `Nats-*`,
and these headers:

| Header | Description |
| --- | --- |
| `X-Original-Subject` | The subject the message was published to |
| `X-Original-Msg-Id` | The `MessageContext.MessageId` the handler saw |
| `X-Stream` | Source stream |
| `X-Consumer` | The durable that failed |
| `X-Stream-Sequence` | Sequence in the source stream |
| `X-Delivery-Count` | Deliveries before the dead letter |
| `X-Failure-Type` | Exception type, or `MaxDeliveriesExceeded`, `NullPayload`, `UnroutableSubject` |
| `X-Failure-Reason` | Exception message, one line, at most 1,024 characters |
| `X-Failed-At` | UTC time of the failure (RFC 3339) |

The stored entry also carries `Nats-Msg-Id` (the write's id above). Inspect the
stream with `nats stream view DLQ`.

With `DeadLetterStream = null`, another service owns the stream. The `dlq`
component stays Retrying until a stream captures `{DeadLetterSubjectPrefix}.>`;
failing messages wait in their source stream until then.

The dead-letter stream must not have Interest retention: nothing consumes the
dead-letter subjects, so such a stream acknowledges each write and keeps
nothing. A `DeadLetterStream` with Interest retention fails registration, and a
server stream with Interest retention (found, or already there under the
`DeadLetterStream` name) makes the `dlq` component Faulted. A write acknowledged
by an Interest stream counts as failed, so the source message stays in its
stream. Startup logs a Warning for WorkQueue retention (a consumer's ack deletes
an entry) and for `MaxMsgs`, `MaxBytes` or `MaxMsgsPerSubject` limits.

### Replay

Resolve `NatsDeadLetters` from DI:

```csharp
var result = await deadLetters.ReplayAsync(new ReplayOptions
{
    SubjectFilter = "orders.>",   // relative to the prefix; null = everything
    Consumer = null,              // only entries of this durable
    Targeted = true,              // default
    Max = 100
}, cancellationToken);

foreach (var skip in result.Skipped)
{
    Console.WriteLine($"kept {skip.DeadLetterSequence}: {skip.Reason}");
}
```

- Each entry is republished to `X-Original-Subject` with its payload and
  original headers, `X-Original-Msg-Id` included, so `MessageContext.MessageId`
  is the id the handler saw when it failed. The replay also carries
  `X-Replayed-From: {dlqStream}:{sequence}` and the message id
  `replay:{dlqStream}:{sequence}`, so a repeated replay is deduplicated.
- The entry is deleted only after the replay is acknowledged.
- **Targeted** (the default): the replay carries `X-Replay-Consumer`, so only the
  durable that failed runs its handler. Other library durables on the subject ack
  it without calling their handler. This matters on fan-out subjects. An entry
  is skipped and kept when no handler would run it: its durable no longer
  exists, has no pull request waiting for about 2 s (no running handler, as for
  the orphan durable of a renamed handler class, or every handler slot is busy),
  no longer filters the subject, or another stream than `X-Stream` captures the
  subject now. Start the owning service and replay again, or replay with
  `Targeted = false`.
- `Targeted = false` delivers the message to every consumer on the subject again.
  On an Interest stream an entry is skipped and kept when no consumer filters
  the subject: the stream would acknowledge the replay and keep nothing.
- Replay covers `{DeadLetterSubjectPrefix}.{filter}` only. Entries written to an
  explicit `DeadLetterSubject` outside the prefix are not replayed.

## Health, readiness and faults

### Components

The hosted service runs each part in its own retry loop and reports it in
`NatsMessagingStatus` (a singleton):

| Component | Kind |
| --- | --- |
| `connection` | Connection |
| `stream:{name}` | Stream (declared streams) |
| `dlq` | DeadLetter (services with consumers) |
| `consumer:{stream}/{durable}` | Consumer |
| `reply:{subject}` | ReplyHandler |

States: Starting, Running, Retrying, Faulted, Stopped. Each `ComponentStatus` has
the last `Error` and any `Warning` (drift, a rejected consumer update).

- Transient errors (timeouts, no responders, lost connection, HTTP 503 codes, a
  missing stream or consumer) retry after 1 s, doubling to 30 s, with jitter.
  The state is Retrying.
- A consumer that is up but receives nothing because JetStream does not answer
  (an R3 stream or durable without quorum, no responders) is stalled: three
  failed ConsumerInfo reads in a row, or 30 s of fetches that end with no
  responders or a protocol error. It logs a Warning and shows Retrying (health
  Degraded) with an `Error` starting "Stalled", and is Running again once a read
  or a fetch works.
- Permanent errors (configuration errors, by JetStream err_code) make the
  component Faulted, then `OnComponentFaulted` runs once:
  - `StopApplication` (default): log Critical and stop the host. Your
    orchestrator restarts it, so a broken consumer shows as a crash loop instead
    of a silent gap.
  - `Report`: keep running. The component retries every
    `FaultedRetryInterval` and stays Faulted until a retry succeeds.
- No loop ever stops for good.

### The health check

```csharp
services.AddHealthChecks()
    .AddNatsMessagingCheck(tags: ["ready"], componentFilter: NatsHealthFilters.ExcludeConsumers);
```

| Result | When |
| --- | --- |
| Healthy | The connection is open and every selected component is Running. |
| Degraded | Started, but a component is Retrying or has a Warning, or the connection is reconnecting. |
| Unhealthy | A component is Faulted, or has not reached Running yet, or the connection is Failed. |

NATS.Net gives up on a connection for good (the Failed state) when the server
rejects its credentials twice in a row, for example after a credentials
rotation. It never reconnects, even once the credentials are valid again. The
library then marks the `connection` component Faulted and runs
`OnComponentFaulted`, so with `StopApplication` the process restarts with a new
connection. This holds for the first connect too.

The check's `data` has one entry per selected component. Degraded returns HTTP
200 by default, so readiness still passes. Filters:

- default (null): every component. Use it for worker services.
- `NatsHealthFilters.ExcludeConsumers`: the connection, streams, dlq and reply
  handlers.
- `NatsHealthFilters.ConnectionOnly`.

A service that mostly publishes but also owns consumers (an API) should use
`ExcludeConsumers` or `ConnectionOnly` with `OnComponentFaulted = Report`, so a
broken consumer does not take its endpoints down.

### Waiting for startup

Startup is a background pipeline: connect, provision streams, check the
dead-letter stream, start consumers and reply handlers. By default host start
returns at once and readiness comes from the health check.

A worker that should not report itself started before it consumes can gate host
start instead:

```csharp
options.WaitUntilStarted = true;
options.StartTimeout = TimeSpan.FromSeconds(30);
```

Host start then waits until every component has reached Running. It fails at
once when a component is Faulted, and with a `TimeoutException` after
`StartTimeout`. In code, `await status.WaitUntilStartedAsync(ct)` does the same
wait, and `status.Components` (or `status.Get("consumer:ORDERS/orders-worker_OrderPlacedHandler")`)
shows each component's state, last error and warning.

`RetryOnInitialConnect = false` makes a failed first connection permanent: the
connection goes Faulted and `OnComponentFaulted` runs. Use it for short-lived
jobs that should fail fast.

## Graceful shutdown

On stop, the consumers stop pulling at once, so no new message is delivered.
Running handlers get up to `ShutdownTimeout` (default 25 s) to finish; their
acks and NAKs wait for the server's confirmation. Handlers still running then see
their `CancellationToken` cancelled. When they throw `OperationCanceledException`,
or return without settling once their token is cancelled (a catch-all, or a loop
on the token that exits early), the message is NAKed with no delay, so another
instance takes it at once. A handler that really finished at that moment runs
again on the other instance, which at-least-once delivery allows. Keep
`ShutdownTimeout` below the host's own shutdown timeout (30 s by default) and the
Kubernetes termination grace period. If the host's timeout ends first, the
running handlers are cancelled at once and the library waits up to 3 s more for
their NAKs, so they reach the server before the connection is disposed.

A request handler that is running when stop starts finishes, and its reply is
still sent (within 5 s) before the connection closes. Requests that arrived but
have not started yet are not handled: their callers time out and retry against
another instance.

## OpenTelemetry

The library has one `ActivitySource` and one `Meter`, both named
`NatsMessagingTelemetry.SourceName` (`"Messaging.NATS"`). Register them together
with NATS.Net's own source:

```csharp
services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(NatsMessagingTelemetry.SourceName, "NATS.Net"))
    .WithMetrics(m => m.AddMeter(NatsMessagingTelemetry.SourceName));
```

### Spans

| Span | Kind | Parent |
| --- | --- | --- |
| `publish {subject}` | Producer | The caller's current activity |
| `schedule {subject}` | Producer | The caller's current activity |
| `process {filter subject}` | Consumer | The `traceparent` header of the message |

- Publish and schedule write their span's context as `traceparent` (and
  `tracestate`), else the context of `Activity.Current`. A `traceparent` or
  `tracestate` the caller passes in `Headers` is dropped, so forwarding
  `context.Headers` from a handler links the next consumer to the handler's
  publish, not to the first producer. The scheduler copies the header onto the
  fired message, so a scheduled message's handler joins the trace of the
  `ScheduleAsync` call.
- The handler runs inside its `process` span (`Activity.Current`), so publishes
  made in the handler join the trace.
- Dead-letter writes and replays are `publish` spans too.

Attributes on `process`:

| Attribute | Value |
| --- | --- |
| `messaging.system` | `nats` |
| `messaging.operation.name`, `messaging.operation.type` | `process` |
| `messaging.destination.name` | The message subject |
| `messaging.consumer.group.name` | The durable |
| `messaging.message.id` | `MessageContext.MessageId` |
| `messaging.message.conversation_id` | `X-Correlation-Id` |
| `messaging.message.body.size` | Payload bytes |
| `messaging.nats.stream`, `messaging.nats.stream_sequence`, `messaging.nats.delivery_count` | From the delivery |
| `messaging.nats.handler` | Handler class |
| `messaging.nats.schedule.id` | Fired schedules only |
| `messaging.nats.outcome` | `ack`, `retry`, `dead_letter`, `skipped` (a targeted replay for another durable), `deferred` (4.x `Msg-DeferredUntil`), `handoff` (shutdown), `terminated` (a legacy handler called `AckTerminateAsync`) |
| `error.type` | On failure: the exception type (or the dead-letter failure type). The span status is Error and the exception is recorded as an event. |

Attributes on `publish` and `schedule`: `messaging.system`,
`messaging.operation.name`, `messaging.destination.name`, `messaging.message.id`,
`messaging.message.body.size`, `messaging.nats.publish.attempts`,
`messaging.nats.outcome` and, on failure, `error.type`. `schedule` also has
`messaging.nats.schedule.id`, `.key`, `.mode` (keyed only) and `.deliver_at`.

### Metrics

| Metric | Type | Tags |
| --- | --- | --- |
| `messaging.process.duration` (s) | Histogram | `messaging.consumer.group.name`, `messaging.nats.outcome` |
| `messaging.client.consumed.messages` | Counter | `messaging.consumer.group.name`, `messaging.nats.outcome` |
| `messaging.client.operation.duration` (s) | Histogram | `messaging.operation.name` (`publish`, `schedule`), `messaging.destination.name`, `messaging.nats.outcome` (`ok`, `duplicate`, `no_stream`, `rejected`, `timeout`, `cancelled`, `error`), `error.type` |
| `messaging.nats.inflight` | UpDownCounter | `messaging.consumer.group.name` |
| `messaging.nats.dead_letter.failures` | Counter | `messaging.consumer.group.name`, `messaging.nats.stream` |
| `messaging.nats.consumer.recreated` | Counter | `messaging.consumer.group.name`, `messaging.nats.stream` |
| `messaging.nats.consumer.config_rejected` | Counter | `messaging.consumer.group.name`, `messaging.nats.stream` |
| `messaging.nats.consumer.orphans` | Gauge | `messaging.nats.stream` |

`messaging.client.operation.duration` is tagged with the full subject. If your
subjects contain ids, drop or rewrite that tag with an OpenTelemetry View.
For a schedule, `duplicate` means a `CreateIfAbsent` that found a pending one.

## Orphaned consumers

After the consumers on a stream start, the library lists that stream's
consumers. Only `ServiceName` proves that a consumer belongs to this service:

- An orphan: no handler uses it and its `messaging-nats.owner` metadata is this
  service's `ServiceName`. It is logged as a Warning with its pending count and
  the `nats consumer rm` command, and counted in
  `messaging.nats.consumer.orphans`. On an Interest stream an orphan keeps
  messages from being removed, and the log says so.
- Possibly unused: no handler uses it, and its owner metadata is one of this
  service's queue groups (written when `ServiceName` was unset), or it has no
  owner metadata and its name starts with `{queueGroup}_` (a 4.x durable).
  Several services can share a queue group, so this may be another service's live
  durable. It is logged at Information, without a delete command, and not counted.

Set a distinct `ServiceName` on every service to get exact reports. The library
never deletes a consumer.

## Request/reply and the cache

A request handler (core NATS, unchanged from 4.x):

```csharp
[NatsRequestHandler(Subject = "prices.get")]
public sealed class PriceHandler : INatsRequestHandler<PriceRequest, PriceResponse>
{
    public Task<PriceResponse> HandleRequestAsync(string subject, PriceRequest request) =>
        Task.FromResult(new PriceResponse(request.Sku, 9.99m));
}
```

```csharp
var price = await publisher.RequestAsync<PriceRequest, PriceResponse>("prices.get", new PriceRequest(sku),
    timeout: TimeSpan.FromSeconds(2));
```

Request handlers do not use a queue group: every instance of the service answers
each request, and the caller takes the first reply.

`NatsCacheFactory` (a singleton) gives a key-value cache on a JetStream KV bucket:

```csharp
var cache = await caches.CreateAsync("stock", TimeSpan.FromMinutes(10), cancellationToken);
var entry = await cache.GetAsync<Stock>(sku, cancellationToken);
```

## Testing your code

`Messaging.NATS.Testing.InMemoryPublisher` implements `IPublisher`. It records
every publish and schedule in `Published`, every cancel (with its result) in
`Cancellations` and every request in `Requests`. It applies Replace,
CreateIfAbsent and Cancel to a pending list, treats a repeated `MessageId` (or
`ScheduleId`) as a duplicate, and answers requests from responders you register
with `OnRequest`. Nothing is delivered. A schedule fires (it moves from
`PendingSchedules` to `FiredSchedules`) once its `DeliverAt` has passed on the
fake's `Time` (a `TimeProvider`, the system clock by default), or when you call
`Fire(scheduleId)`, so CreateIfAbsent and Cancel after the fire behave as on the
server. Pass your `NatsMessagingOptions` (or a retry count) to the constructor so
a publish without options follows your `PublishRetryAttempts`, as the real
publisher does.

`FailWhen` (publishes and schedules), `FailCancelWhen` (cancels) and `FailOn`
(all three, by subject) make calls fail, so you can test the error paths. A
failed call is not recorded:

```csharp
publisher.FailOn("orders.placed",
    new NatsPublishException(PublishFailure.NoStream, "orders.placed", null, null, "No stream"));
publisher.FailWhen(call => call.IsScheduled ? new NatsMessagingConfigurationException("no schedules") : null);
```

```csharp
var publisher = new InMemoryPublisher();
var checkout = new Checkout(publisher, NullLogger<Checkout>.Instance);

await checkout.PlaceAsync(new OrderPlaced("o-1", 10m));

var order = publisher.MessagesOf<OrderPlaced>("orders.placed").Single();
Assert.AreEqual("o-1", order.OrderId);
Assert.AreEqual("o-1", publisher.Published.Single().Options!.MessageId);
```

`TestMessageContext.Create` builds a `MessageContext<T>` for handler tests:

```csharp
await handler.HandleAsync(TestMessageContext.Create(new NotificationDue("n-1"), scheduleId: "schedule-1"),
    CancellationToken.None);
```

Each context gets the next stream sequence, so contexts without a `messageId` get
different `MessageId`s, as two stored messages would. `stream`, `consumer`,
`streamSequence`, `enqueuedTime` and `scheduledFor` can be set.

Moq setups and verifies keep working: every publish shape is its own overload
with no optional parameters.

## Running the tests

The integration tests need a nats-server with JetStream.

| Variable | Meaning |
| --- | --- |
| `NATS_TEST_URL` | An existing server, for example `nats://127.0.0.1:4222`. Unset: the tests start a `nats:<NATS_SERVER_VERSION>-alpine` container (Docker). |
| `NATS_TEST_SPARE_PORT` | A free port. Tests that start and stop their own server use it. |
| `NATS_CLUSTER_BASE_PORT` | The first of three free client ports for the R3 cluster tests. |
| `NATS_CLUSTER_ROUTE_PORT` | The first of three free route ports (default base + 10). |
| `NATS_SERVER_BIN` | The nats-server binary (default `nats-server` on PATH). |
| `NATS_SERVER_VERSION` | The container image version when `NATS_TEST_URL` is unset (default `2.12`). |
| `NATS_REQUIRE_SERVER_TESTS` | `1`: a test that cannot start its own servers fails instead of ending Inconclusive. CI sets it. |

When a port variable is unset and nats-server can be found, the tests pick free
ports themselves, so a plain `dotnet test` also runs the R3 cluster tests. Without
nats-server those tests end Inconclusive (or fail with `NATS_REQUIRE_SERVER_TESTS=1`).
The publish workflow runs every test before it packs.

```bash
nats-server -js -sd "$(mktemp -d)" &
NATS_TEST_URL=nats://127.0.0.1:4222 NATS_TEST_SPARE_PORT=4300 NATS_CLUSTER_BASE_PORT=4310 dotnet test

# The R3 cluster tests only
NATS_TEST_URL=nats://127.0.0.1:4222 NATS_CLUSTER_BASE_PORT=4310 dotnet test --filter TestCategory=Cluster
```

The cluster tests start three nats-server processes (`ClusterTests`,
`NatsClusterFixture`), run one at a time, and stop nodes on purpose. They take
about four minutes on a laptop.

The code samples in this README are copied in `ReadmeExamples.cs` in the test
project, so they keep compiling.
