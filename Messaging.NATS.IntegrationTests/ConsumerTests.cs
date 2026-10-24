using System.Text;
using Messaging.NATS.IntegrationTests.Handlers;
using Messaging.NATS.Testing;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class ConsumerTests
{
    private static readonly byte[] Unreadable = Encoding.UTF8.GetBytes("{not json");

    [TestMethod]
    public async Task Handler_ThatReturns_IsAutoAcked()
    {
        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", AckWaitSeconds = 1 });

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));
        await probe.WaitForCallsAsync(1);

        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()).NumAckPending == 0,
            TimeSpan.FromSeconds(5), "The message is still pending");
        await Task.Delay(2500);
        Assert.AreEqual(1, probe.Count, "An acked message is not redelivered after AckWait");
        Assert.AreEqual(1UL, (await scenario.InfoAsync()).AckFloor.StreamSeq);
    }

    [TestMethod]
    public async Task Handler_ThatThrowsThenSucceeds_IsRetriedAfterTheRetryDelays()
    {
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, call, _) => call < 3 ? throw new InvalidOperationException($"call {call}") : Task.CompletedTask
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", RetryDelaysSeconds = [1, 2] });

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));
        var calls = await probe.WaitForCallsAsync(3, TimeSpan.FromSeconds(15));

        Assert.IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(900), calls[1].At - calls[0].At);
        Assert.IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(1900), calls[2].At - calls[1].At);
        Assert.AreEqual(3, calls[2].Arg.DeliveryCount);
        Assert.IsTrue(calls[2].Arg.IsRedelivery);

        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()).NumAckPending == 0,
            TimeSpan.FromSeconds(5), "The successful attempt is acked");
        Assert.AreEqual(3, probe.Count);
    }

    [TestMethod]
    public async Task Handler_ThatAlwaysThrows_RunsExactlyMaxDeliveryAttemptsTimes_ThenDeadLetters()
    {
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, call, _) => throw new InvalidOperationException($"call {call}")
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", MaxDeliveryAttempts = 3, RetryDelaysSeconds = [0] });
        await using var deadLetters = await scenario.Host.CollectDeadLettersAsync();

        var subject = $"{scenario.Root}.created";
        var published = await scenario.Publisher.PublishAsync(subject, new OrderEvent("ORD-DEAD", 42m),
            new PublishOptions { Headers = new NatsHeaders { ["x-custom"] = "kept" } });

        var dead = (await deadLetters.WaitForAsync(1, TimeSpan.FromSeconds(15))).Single();
        await Task.Delay(1000);

        Assert.AreEqual(3, probe.Count);
        Assert.AreEqual(scenario.Host.DeadLetterSubject(subject), dead.Subject);
        Assert.AreEqual("ORD-DEAD", dead.Json<OrderEvent>().OrderId);
        Assert.AreEqual("kept", dead.Header("x-custom"));
        Assert.AreEqual(subject, dead.Header("X-Original-Subject"));
        Assert.AreEqual(published.MessageId, dead.Header("X-Original-Msg-Id"));
        Assert.AreEqual(scenario.Stream.Name, dead.Header("X-Stream"));
        Assert.AreEqual(scenario.Durable, dead.Header("X-Consumer"));
        Assert.AreEqual(published.Sequence.ToString(), dead.Header("X-Stream-Sequence"));
        Assert.AreEqual("3", dead.Header("X-Delivery-Count"));
        Assert.AreEqual("System.InvalidOperationException", dead.Header("X-Failure-Type"));
        Assert.AreEqual("call 3", dead.Header("X-Failure-Reason"));
        Assert.IsNotNull(dead.Header("X-Failed-At"));
        // The stored entry's only Nats-* header is the dead-letter write's own message id.
        CollectionAssert.AreEqual(new[] { "Nats-Msg-Id" },
            dead.Headers.Keys.Where(k => k.StartsWith("Nats-", StringComparison.OrdinalIgnoreCase)).ToArray(),
            string.Join(", ", dead.Headers.Keys));
        DeadLetterTests.AssertDeadLetterId(scenario.Stream.Name, published.Sequence, scenario.Durable, dead.Header("Nats-Msg-Id"));
        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()).NumAckPending == 0,
            TimeSpan.FromSeconds(5), "The source message is acked after the dead-letter write");
    }

    [TestMethod]
    public async Task CrashLoopGuard_DeliveryPastAttemptsPlusSlack_IsDeadLetteredWithoutRunningTheHandler()
    {
        // A hanging handler with no heartbeat looks like a process that dies on the message: the server
        // redelivers it after every AckWait, and the handler never fails.
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, _, ct) => Task.Delay(Timeout.Infinite, ct)
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], o =>
            {
                o.MaxDeliverySlack = 2;
                o.ShutdownTimeout = TimeSpan.FromSeconds(1);
                o.ConsumerTestHooks.DisableHeartbeat = true;
            },
            new NatsConsumerAttribute { Subject = ".created", MaxDeliveryAttempts = 1, AckWaitSeconds = 1, MaxConcurrency = 8 });
        await using var deadLetters = await scenario.Host.CollectDeadLettersAsync();

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-OOM", 1m));

        var dead = (await deadLetters.WaitForAsync(1, TimeSpan.FromSeconds(20))).Single();
        await Task.Delay(1500);

        Assert.AreEqual(3, probe.Count, "The handler runs for deliveries 1..A+S only");
        CollectionAssert.AreEqual(new[] { 1, 2, 3 }, probe.Calls.Select(c => c.Arg.DeliveryCount).ToArray());
        Assert.AreEqual("4", dead.Header("X-Delivery-Count"));
        Assert.AreEqual("MaxDeliveriesExceeded", dead.Header("X-Failure-Type"));
    }

    [TestMethod]
    public async Task HandlerThatNeverReturns_LosesItsHeartbeatAfterMaxHandlerSeconds_SoTheMessageIsRedelivered()
    {
        var logs = new LogCapture();
        var hung = new TaskCompletionSource();
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            // The first call ignores its token, like a deadlock or an HTTP call without a timeout.
            Behaviour = (_, call, _) => call == 1 ? hung.Task : Task.CompletedTask
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe, logs], null,
            new NatsConsumerAttribute { Subject = ".created", AckWaitSeconds = 1, MaxConcurrency = 2, MaxHandlerSeconds = 2 });
        try
        {
            await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));

            var calls = await probe.WaitForCallsAsync(2, TimeSpan.FromSeconds(15));
            Assert.AreEqual(2, calls[1].Arg.DeliveryCount, "Redelivered while the first handler still runs");
            Assert.IsGreaterThanOrEqualTo(TimeSpan.FromSeconds(2), calls[1].At - calls[0].At,
                "Not before MaxHandlerSeconds: the heartbeat held it until then");
            await logs.WaitForAsync(e => e.Level == LogLevel.Error && e.Message.Contains("MaxHandlerTime"),
                TimeSpan.FromSeconds(5), "about the handler past its MaxHandlerTime");
            var status = scenario.Host.Status.Get(scenario.Component)!;
            StringAssert.Contains(status.Warning, "MaxHandlerTime");
            Assert.AreEqual(HealthStatus.Degraded, (await scenario.Host.CheckHealthAsync()).Status);
        }
        finally
        {
            hung.TrySetResult();
        }

        await Eventually.TrueAsync(() => Task.FromResult(scenario.Host.Status.Get(scenario.Component)!.Warning == null),
            TimeSpan.FromSeconds(5), "The Warning clears once the handler returns");
    }

    [TestMethod]
    public async Task HandlerThatStopsOnItsCancelledToken_AfterMaxHandlerSeconds_IsAFailedAttempt_AndRetried()
    {
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, call, ct) => call == 1 ? Task.Delay(Timeout.Infinite, ct) : Task.CompletedTask
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", MaxHandlerSeconds = 1, RetryDelaysSeconds = [0] });

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));

        var calls = await probe.WaitForCallsAsync(2, TimeSpan.FromSeconds(10));
        Assert.AreEqual(2, calls[1].Arg.DeliveryCount);
        Assert.IsLessThan(TimeSpan.FromSeconds(5), calls[1].At - calls[0].At,
            "NAKed as a failed attempt, not redelivered after AckWait (30 s)");
        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()).NumAckPending == 0, TimeSpan.FromSeconds(5),
            "The second delivery is acked");
    }

    [TestMethod]
    public async Task FetchHiccupAfterALongHandler_IsNotReportedAsAStall()
    {
        // The handler holds the only slot for longer than StallAfter, so no fetch runs meanwhile. The fetch after it
        // gets one trouble notification and lasts FetchExpires (5 s), which is shorter than StallAfter.
        var logs = new LogCapture();
        var handlerDone = 0;
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = async (_, _, ct) =>
            {
                await Task.Delay(TimeSpan.FromSeconds(8), ct);
                Volatile.Write(ref handlerDone, 1);
            }
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe, logs], o =>
            {
                o.ConsumerTestHooks.StallAfter = TimeSpan.FromSeconds(7);
                o.ConsumerTestHooks.FetchTrouble = () =>
                    Interlocked.CompareExchange(ref handlerDone, 2, 1) == 1 ? "injected missed heartbeats" : null;
            },
            new NatsConsumerAttribute { Subject = ".created" });

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));
        await probe.WaitForCallsAsync(1);
        await Eventually.TrueAsync(() => Task.FromResult(Volatile.Read(ref handlerDone) == 2), TimeSpan.FromSeconds(20),
            "A fetch after the handler got the injected trouble");
        await Task.Delay(500);

        Assert.IsFalse(logs.Entries.Any(e => e.Message.Contains("is stalled")),
            string.Join(Environment.NewLine, logs.Entries.Where(e => e.Message.Contains("stalled")).Select(e => e.Message)));
        Assert.AreEqual(ComponentState.Running, scenario.Host.Status.Get(scenario.Component)!.State);
    }

    [TestMethod]
    public async Task FetchTroubleForLongerThanStallAfter_WithFreeSlots_IsReportedAsAStall()
    {
        var logs = new LogCapture();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(
            [new Probe<MessageContext<OrderEvent>>(), logs], o =>
            {
                o.ConsumerTestHooks.StallAfter = TimeSpan.FromSeconds(7);
                o.ConsumerTestHooks.FetchTrouble = () => "injected missed heartbeats";
            },
            new NatsConsumerAttribute { Subject = ".created" });

        var stalled = await scenario.Host.WaitForStateAsync(scenario.Component, ComponentState.Retrying,
            TimeSpan.FromSeconds(20));
        StringAssert.Contains(stalled.Error, "injected missed heartbeats");
        Assert.IsTrue(logs.Entries.Any(e => e.Level == LogLevel.Warning && e.Message.Contains("is stalled")));
    }

    [TestMethod]
    public async Task Legacy_HandlerThatAcks_IsNotSettledAgain()
    {
        var probe = new Probe<INatsJSMsg<OrderEvent>>
        {
            Behaviour = async (msg, _, ct) =>
            {
                await msg.AckAsync(cancellationToken: ct);
                throw new InvalidOperationException("thrown after the ack");
            }
        };
        await using var scenario = await ConsumerScenario.StartAsync<LegacyOrderConsumer>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", AckWaitSeconds = 1, RetryDelaysSeconds = [0] });
        await using var deadLetters = await scenario.Host.CollectDeadLettersAsync();
        await using var settles = await SettleWire.StartAsync(scenario);

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));
        await probe.WaitForCallsAsync(1);
        await Task.Delay(2500);

        Assert.AreEqual(1, probe.Count, "Settled by the handler: no retry, although it threw");
        Assert.IsEmpty(deadLetters.Received, "Settled by the handler: no dead letter, although it threw");
        Assert.AreEqual(0, (await scenario.InfoAsync()).NumAckPending);
        CollectionAssert.AreEqual(new[] { "+ACK" }, settles.Settles.ToArray(),
            "Only the handler's ack is sent; the library sends no NAK after the throw");
    }

    [TestMethod]
    public async Task Legacy_HandlerThatNaks_IsNotAckedByTheLibrary()
    {
        var probe = new Probe<INatsJSMsg<OrderEvent>>
        {
            Behaviour = (msg, call, ct) => call == 1
                ? msg.NakAsync(new AckOpts { NakDelay = TimeSpan.FromSeconds(1) }, ct).AsTask()
                : msg.AckAsync(cancellationToken: ct).AsTask()
        };
        await using var scenario = await ConsumerScenario.StartAsync<LegacyOrderConsumer>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));

        var calls = await probe.WaitForCallsAsync(2, TimeSpan.FromSeconds(10));
        Assert.AreEqual(2UL, calls[1].Arg.Metadata!.Value.NumDelivered, "The handler's NAK stood: an auto-ack would have ended it");
        Assert.IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(900), calls[1].At - calls[0].At);
    }

    [TestMethod]
    public async Task Legacy_HandlerThatTerminates_IsNotAckedByTheLibrary()
    {
        var probe = new Probe<INatsJSMsg<OrderEvent>>
        {
            Behaviour = (msg, _, ct) => msg.AckTerminateAsync(cancellationToken: ct).AsTask()
        };
        await using var scenario = await ConsumerScenario.StartAsync<LegacyOrderConsumer>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", AckWaitSeconds = 1 });

        await using var connection = TestHost.CreateConnection();
        await using var terminated = await connection.SubscribeCoreAsync<byte[]>(
            $"$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.{scenario.Stream.Name}.{scenario.Durable}");
        await connection.PingAsync();
        await using var settles = await SettleWire.StartAsync(scenario);

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await terminated.Msgs.ReadAsync(timeout.Token);
        await Task.Delay(2000);
        Assert.AreEqual(1, probe.Count);
        Assert.AreEqual(0, (await scenario.InfoAsync()).NumAckPending);
        CollectionAssert.AreEqual(new[] { "+TERM" }, settles.Settles.ToArray(),
            "Only the handler's terminate is sent; the library sends no ack after it");
    }

    [TestMethod]
    public async Task Legacy_HandlerThatDoesNotSettle_IsAutoAcked()
    {
        var probe = new Probe<INatsJSMsg<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<LegacyOrderConsumer>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", AckWaitSeconds = 1 });

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));
        await probe.WaitForCallsAsync(1);

        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()).NumAckPending == 0,
            TimeSpan.FromSeconds(5), "The message is still pending");
        await Task.Delay(2500);
        Assert.AreEqual(1, probe.Count);
    }

    [TestMethod]
    public async Task Legacy_HandlerThatNaksEveryTime_IsDeadLetteredAtAttemptsPlusSlackPlusOne_WithoutTheHandler()
    {
        var probe = new Probe<INatsJSMsg<OrderEvent>>
        {
            Behaviour = (msg, _, ct) => msg.NakAsync(cancellationToken: ct).AsTask()
        };
        await using var scenario = await ConsumerScenario.StartAsync<LegacyOrderConsumer>([probe],
            o => o.MaxDeliverySlack = 2,
            new NatsConsumerAttribute { Subject = ".created", MaxDeliveryAttempts = 1 });
        await using var deadLetters = await scenario.Host.CollectDeadLettersAsync();

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));

        var dead = (await deadLetters.WaitForAsync(1, TimeSpan.FromSeconds(15))).Single();
        await Task.Delay(500);

        Assert.AreEqual(3, probe.Count);
        Assert.AreEqual("4", dead.Header("X-Delivery-Count"));
        Assert.AreEqual("MaxDeliveriesExceeded", dead.Header("X-Failure-Type"));
    }

    [TestMethod]
    public async Task Legacy_NullPayload_PassesThroughWithNullData()
    {
        var probe = new Probe<INatsJSMsg<OrderEvent>>
        {
            Behaviour = (msg, _, ct) => msg.AckTerminateAsync(cancellationToken: ct).AsTask()
        };
        await using var scenario = await ConsumerScenario.StartAsync<LegacyOrderConsumer>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });

        await scenario.PublishRawAsync($"{scenario.Root}.created", Encoding.UTF8.GetBytes("null"));

        var msg = (await probe.WaitForCallsAsync(1)).Single().Arg;
        Assert.IsNull(msg.Data);
        Assert.IsNull(msg.Error);
        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()).NumAckPending == 0,
            TimeSpan.FromSeconds(5), "AckTerminate settles the message");
        Assert.AreEqual(1, probe.Count);
    }

    [TestMethod]
    public async Task Legacy_UnreadablePayload_PassesThroughWithTheError()
    {
        var probe = new Probe<INatsJSMsg<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<LegacyOrderConsumer>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });

        await scenario.PublishRawAsync($"{scenario.Root}.created", Unreadable);

        var msg = (await probe.WaitForCallsAsync(1)).Single().Arg;
        Assert.IsNull(msg.Data);
        Assert.IsInstanceOfType<NatsDeserializeException>(msg.Error);
        Assert.ThrowsExactly<NatsDeserializeException>(msg.EnsureSuccess);
    }

    [TestMethod]
    public async Task Handler_UnreadablePayload_IsDeadLetteredWithoutTheHandler()
    {
        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        await using var deadLetters = await scenario.Host.CollectDeadLettersAsync();

        await scenario.PublishRawAsync($"{scenario.Root}.created", Unreadable);

        var dead = (await deadLetters.WaitForAsync(1, TimeSpan.FromSeconds(10))).Single();
        CollectionAssert.AreEqual(Unreadable, dead.Data, "The payload is forwarded byte for byte");
        Assert.AreEqual(typeof(NatsDeserializeException).FullName, dead.Header("X-Failure-Type"));
        Assert.AreEqual("1", dead.Header("X-Delivery-Count"));
        Assert.AreEqual(0, probe.Count);
    }

    [TestMethod]
    public async Task Handler_NullPayload_IsDeadLetteredWithoutTheHandler()
    {
        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        await using var deadLetters = await scenario.Host.CollectDeadLettersAsync();

        await scenario.PublishRawAsync($"{scenario.Root}.created", Encoding.UTF8.GetBytes("null"));

        var dead = (await deadLetters.WaitForAsync(1, TimeSpan.FromSeconds(10))).Single();
        Assert.AreEqual("NullPayload", dead.Header("X-Failure-Type"));
        Assert.AreEqual(0, probe.Count);
    }

    [TestMethod]
    public async Task Handler_PoisonMessageException_IsDeadLetteredAtOnce()
    {
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, _, _) => throw new PoisonMessageException("bad order")
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", MaxDeliveryAttempts = 5 });
        await using var deadLetters = await scenario.Host.CollectDeadLettersAsync();

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));

        var dead = (await deadLetters.WaitForAsync(1, TimeSpan.FromSeconds(10))).Single();
        await Task.Delay(1000);

        Assert.AreEqual(1, probe.Count);
        Assert.AreEqual("1", dead.Header("X-Delivery-Count"));
        Assert.AreEqual(typeof(PoisonMessageException).FullName, dead.Header("X-Failure-Type"));
        Assert.AreEqual("bad order", dead.Header("X-Failure-Reason"));
    }

    [TestMethod]
    public async Task MessageContext_IsPopulatedFromThePublish()
    {
        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".>" });

        var subject = $"{scenario.Root}.created.eu";
        var before = DateTimeOffset.UtcNow;
        var published = await scenario.Publisher.PublishAsync(subject, new OrderEvent("ORD-1", 3m), new PublishOptions
        {
            MessageId = "order-1",
            CorrelationId = "corr-1",
            Headers = new NatsHeaders { ["x-tenant"] = "acme" }
        });
        var after = DateTimeOffset.UtcNow;
        var unkeyed = await scenario.Publisher.PublishAsync(subject, new OrderEvent("ORD-2", 4m),
            new PublishOptions { RetryAttempts = 0 });

        var calls = await probe.WaitForCallsAsync(2);
        var context = calls[0].Arg;

        Assert.AreEqual("ORD-1", context.Message.OrderId);
        Assert.AreEqual(subject, context.Subject);
        Assert.AreEqual("acme", context.Headers["x-tenant"].ToString());
        Assert.AreEqual("order-1", context.MessageId);
        Assert.AreEqual("corr-1", context.CorrelationId);
        Assert.IsTrue(context.SentTime >= before.AddMilliseconds(-1) && context.SentTime <= after,
            $"SentTime {context.SentTime:O} is not between {before:O} and {after:O}");
        Assert.IsTrue(context.EnqueuedTime >= context.SentTime.AddSeconds(-1) && context.EnqueuedTime <= after.AddSeconds(1),
            $"EnqueuedTime {context.EnqueuedTime:O}");
        Assert.AreEqual(1, context.DeliveryCount);
        Assert.IsFalse(context.IsRedelivery);
        Assert.AreEqual(scenario.Stream.Name, context.Stream);
        Assert.AreEqual(scenario.Durable, context.Consumer);
        Assert.AreEqual(published.Sequence, context.StreamSequence);
        Assert.IsFalse(context.IsScheduled);
        Assert.IsNull(context.ScheduleId);
        Assert.IsNull(context.ScheduleKey);
        Assert.IsNull(context.ScheduledFor);

        Assert.AreEqual($"{scenario.Stream.Name}:{unkeyed.Sequence}", calls[1].Arg.MessageId,
            "Without a Nats-Msg-Id the id is stream:sequence");
    }

    [TestMethod]
    public async Task ASecondFireOfTheSameHolder_ReachesTheHandlerWithTheSameMessageIdAndScheduleId()
    {
        var root = TestNames.Unique("timers");
        var stream = StreamDefinition.Scheduled(TestNames.Unique("TIMERS"), $"{root}.>");
        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".due" });

        var subject = $"{root}.due";
        var schedule = await scenario.Publisher.ScheduleAsync(subject, new OrderEvent("ORD-1", 1m),
            DateTimeOffset.UtcNow.AddSeconds(1), new ScheduleOptions { Key = "order-1" });
        var first = (await probe.WaitForCallsAsync(1, TimeSpan.FromSeconds(10))).Single().Arg;

        // What the scheduler stores when an R3 leader change makes one holder fire twice: the same message with the
        // same copied headers, at a new sequence.
        var headers = new NatsHeaders();
        foreach (var (name, value) in first.Headers)
        {
            headers[name] = value;
        }

        await scenario.PublishRawAsync(subject,
            System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new OrderEvent("ORD-1", 1m)), headers);

        var calls = await probe.WaitForCallsAsync(2, TimeSpan.FromSeconds(10));
        Assert.AreNotEqual(calls[0].Arg.StreamSequence, calls[1].Arg.StreamSequence, "Two stored fires");
        Assert.IsTrue(calls.All(c => c.Arg.MessageId == schedule.ScheduleId), "Handlers can key idempotency on MessageId");
        Assert.IsTrue(calls.All(c => c.Arg.ScheduleId == schedule.ScheduleId));
    }

    [TestMethod]
    public async Task MessageContext_OfAFiredSchedule_CarriesTheScheduleFields()
    {
        var root = TestNames.Unique("timers");
        var stream = StreamDefinition.Scheduled(TestNames.Unique("TIMERS"), $"{root}.>");
        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".due" });

        var subject = $"{root}.due";
        var scheduledAt = DateTimeOffset.UtcNow;
        var deliverAt = scheduledAt.AddSeconds(1);
        var schedule = await scenario.Publisher.ScheduleAsync(subject, new OrderEvent("ORD-1", 1m), deliverAt,
            new ScheduleOptions { Key = "order-1", CorrelationId = "corr-1" });

        var context = (await probe.WaitForCallsAsync(1, TimeSpan.FromSeconds(10))).Single().Arg;

        Assert.IsTrue(context.IsScheduled);
        Assert.AreEqual(schedule.ScheduleId, context.ScheduleId);
        Assert.AreEqual(schedule.ScheduleId, context.MessageId, "A fired schedule is identified by its schedule id");
        Assert.AreEqual("order-1", context.ScheduleKey);
        Assert.AreEqual(deliverAt.UtcDateTime, context.ScheduledFor!.Value.UtcDateTime);
        Assert.AreEqual("corr-1", context.CorrelationId);
        Assert.AreEqual(subject, context.Subject);
        Assert.IsTrue(context.SentTime >= scheduledAt.AddMilliseconds(-1) && context.SentTime < deliverAt,
            $"SentTime {context.SentTime:O} is the ScheduleAsync call time");
        Assert.IsTrue(context.EnqueuedTime >= deliverAt.AddMilliseconds(-50),
            $"EnqueuedTime {context.EnqueuedTime:O} is the fire time ({deliverAt:O})");
    }

    [TestMethod]
    public void TestMessageContext_BuildsAContextWithTheGivenFields()
    {
        var sent = DateTimeOffset.UtcNow.AddMinutes(-1);
        var context = TestMessageContext.Create(new OrderEvent("ORD-1", 1m), "orders.created",
            new NatsHeaders { ["x-tenant"] = "acme" }, deliveryCount: 2, correlationId: "corr", sentTime: sent,
            scheduleId: "sched-1", scheduleKey: "key-1");

        Assert.AreEqual("ORD-1", context.Message.OrderId);
        Assert.AreEqual("orders.created", context.Subject);
        Assert.AreEqual("acme", context.Headers["x-tenant"].ToString());
        Assert.AreEqual(2, context.DeliveryCount);
        Assert.IsTrue(context.IsRedelivery);
        Assert.AreEqual("corr", context.CorrelationId);
        Assert.AreEqual(sent, context.SentTime);
        Assert.AreEqual("sched-1", context.ScheduleId);
        Assert.AreEqual("sched-1", context.MessageId);
        Assert.AreEqual("key-1", context.ScheduleKey);
        Assert.IsTrue(context.IsScheduled);

        Assert.AreEqual("id-1", TestMessageContext.Create("x", messageId: "id-1", scheduleId: "s").MessageId);
        Assert.AreEqual("orders:7", TestMessageContext.Create("x", stream: "orders", streamSequence: 7).MessageId);
    }

    [TestMethod]
    public void TestMessageContext_GivesEachContextItsOwnMessageId_AndTakesTheDeliveryFields()
    {
        var seen = new HashSet<string>();
        var handled = new List<string>();
        foreach (var order in new[] { "ORD-1", "ORD-2" })
        {
            var context = TestMessageContext.Create(new OrderEvent(order, 1m));
            if (seen.Add(context.MessageId))
            {
                handled.Add(context.Message.OrderId);
            }
        }

        CollectionAssert.AreEqual(new[] { "ORD-1", "ORD-2" }, handled, "An idempotent handler handles both");

        var enqueued = DateTimeOffset.UtcNow.AddMinutes(-5);
        var scheduledFor = DateTimeOffset.UtcNow.AddMinutes(-4);
        var full = TestMessageContext.Create("x", stream: "ORDERS", consumer: "billing_OrderHandler", streamSequence: 42,
            enqueuedTime: enqueued, scheduledFor: scheduledFor);
        Assert.AreEqual("ORDERS", full.Stream);
        Assert.AreEqual("billing_OrderHandler", full.Consumer);
        Assert.AreEqual(42UL, full.StreamSequence);
        Assert.AreEqual(enqueued, full.EnqueuedTime);
        Assert.AreEqual(scheduledFor, full.ScheduledFor);
        Assert.AreEqual("ORDERS:42", full.MessageId);
    }

    /// <summary>
    /// The settles sent for a durable's deliveries, read off the wire: every message on its ack subjects except the
    /// in-progress heartbeat (+WPI). The server ignores a second settle of an acked or terminated message, so only
    /// the wire shows one.
    /// </summary>
    private sealed class SettleWire : IAsyncDisposable
    {
        private readonly NatsConnection _connection;
        private readonly INatsSub<byte[]> _subscription;
        private readonly Task _reading;
        private readonly List<string> _settles = [];

        private SettleWire(NatsConnection connection, INatsSub<byte[]> subscription)
        {
            _connection = connection;
            _subscription = subscription;
            _reading = Task.Run(async () =>
            {
                await foreach (var msg in subscription.Msgs.ReadAllAsync())
                {
                    var kind = Encoding.UTF8.GetString(msg.Data ?? []).Split(' ')[0];
                    if (kind != "+WPI")
                    {
                        lock (_settles)
                        {
                            _settles.Add(kind);
                        }
                    }
                }
            });
        }

        public IReadOnlyList<string> Settles
        {
            get
            {
                lock (_settles)
                {
                    return _settles.ToList();
                }
            }
        }

        public static async Task<SettleWire> StartAsync(ConsumerScenario scenario)
        {
            var connection = TestHost.CreateConnection();
            var subscription = await connection.SubscribeCoreAsync<byte[]>(
                $"$JS.ACK.{scenario.Stream.Name}.{scenario.Durable}.>");
            await connection.PingAsync();
            return new SettleWire(connection, subscription);
        }

        public async ValueTask DisposeAsync()
        {
            await _subscription.DisposeAsync();
            await _reading;
            await _connection.DisposeAsync();
        }
    }
}
