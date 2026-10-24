using System.Text;
using Messaging.NATS.IntegrationTests.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class DeadLetterTests : NatsTestBase
{
    [TestMethod]
    public async Task RetryDelays_SpaceRedeliveriesAndEventuallySucceed()
    {
        var state = new FlakyHandlerState { Mode = FlakyHandler.Mode.SucceedAfterAttempt, SucceedOnAttempt = 3 };
        var (stream, root) = ConsumerBindings.UniqueStream("DLQ_BACKOFF");

        await using var host = await StartFlakyAsync(state, stream, new NatsConsumerAttribute
        {
            Subject = $"{root}.created", MaxDeliveryAttempts = 5, AckWaitSeconds = 10, RetryDelaysSeconds = [1, 1]
        });

        await host.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-BACKOFF", 1m));

        var attemptThatSucceeded = await state.Succeeded.Task.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.AreEqual(3, attemptThatSucceeded, "Handler should succeed on the 3rd attempt");
        Assert.HasCount(3, state.AttemptTimestamps);

        var gap1 = state.AttemptTimestamps[1] - state.AttemptTimestamps[0];
        var gap2 = state.AttemptTimestamps[2] - state.AttemptTimestamps[1];
        Assert.IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(800), gap1);
        Assert.IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(800), gap2);
    }

    [TestMethod]
    public async Task ExhaustedRetries_RoutesMessageToDeadLetterSubject()
    {
        var state = new FlakyHandlerState { Mode = FlakyHandler.Mode.AlwaysThrow };
        var (stream, root) = ConsumerBindings.UniqueStream("DLQ_EXHAUST");
        var subject = $"{root}.created";

        await using var host = await StartFlakyAsync(state, stream, new NatsConsumerAttribute
        {
            Subject = subject, MaxDeliveryAttempts = 3, AckWaitSeconds = 5, RetryDelaysSeconds = [0]
        });

        await using var connection = CreateConnection();
        await using var dlqSub = await connection.SubscribeCoreAsync<OrderEvent>(host.DeadLetterSubject(subject));
        await connection.PingAsync();

        await host.Publisher.PublishAsync(subject, new OrderEvent("ORD-DEAD", 42m));

        var dlqMsg = await NextAsync(dlqSub, TimeSpan.FromSeconds(20));

        Assert.AreEqual("ORD-DEAD", dlqMsg.Data!.OrderId);
        Assert.AreEqual(42m, dlqMsg.Data.Amount);
        Assert.IsNotNull(dlqMsg.Headers, "DLQ message should carry headers");
        Assert.AreEqual(subject, dlqMsg.Headers["X-Original-Subject"].ToString());
        Assert.AreEqual(stream.Name, dlqMsg.Headers["X-Stream"].ToString());
        Assert.AreEqual("3", dlqMsg.Headers["X-Delivery-Count"].ToString());
        StringAssert.Contains(dlqMsg.Headers["X-Failure-Type"].ToString(), "InvalidOperationException");
        Assert.AreEqual(3, Volatile.Read(ref state.Attempts), "Handler should be invoked exactly MaxDeliveryAttempts times");
    }

    [TestMethod]
    public async Task PoisonMessageException_ShortCircuitsToDeadLetterSubject()
    {
        var state = new FlakyHandlerState { Mode = FlakyHandler.Mode.AlwaysPoison };
        var (stream, root) = ConsumerBindings.UniqueStream("DLQ_POISON");
        var subject = $"{root}.created";

        await using var host = await StartFlakyAsync(state, stream, new NatsConsumerAttribute
        {
            Subject = subject, MaxDeliveryAttempts = 5, AckWaitSeconds = 10
        });

        await using var connection = CreateConnection();
        await using var dlqSub = await connection.SubscribeCoreAsync<OrderEvent>(host.DeadLetterSubject(subject));
        await connection.PingAsync();

        await host.Publisher.PublishAsync(subject, new OrderEvent("ORD-POISON", 7m));

        var dlqMsg = await NextAsync(dlqSub, TimeSpan.FromSeconds(10));

        Assert.AreEqual("ORD-POISON", dlqMsg.Data!.OrderId);
        StringAssert.Contains(dlqMsg.Headers!["X-Failure-Type"].ToString(), "PoisonMessageException");

        await Task.Delay(1000);
        Assert.AreEqual(1, Volatile.Read(ref state.Attempts),
            "PoisonMessageException should short-circuit to DLQ on the first attempt");
    }

    [TestMethod]
    public async Task DeadLetter_IsStoredInTheDlqStream_ByteIdentical_WithOriginalAndFailureHeaders_AndNoOriginalNatsHeaders()
    {
        var probe = new Probe<MessageContext<OrderEvent>> { Behaviour = (_, _, _) => throw new PoisonMessageException("bad") };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });

        // Odd spacing and key order: a re-serialised payload would not match byte for byte.
        var payload = Encoding.UTF8.GetBytes("{ \"Amount\" : 1.50,\n  \"OrderId\":\"ORD-RAW\" }");
        var subject = $"{scenario.Root}.created";
        var sequence = await scenario.PublishRawAsync(subject, payload, new NatsHeaders
        {
            ["Nats-Msg-Id"] = "business-id-1",
            ["Nats-Expected-Stream"] = scenario.Stream.Name,
            ["x-custom"] = "kept",
            ["X-Correlation-Id"] = "corr-1"
        });

        await Eventually.TrueAsync(async () => await MessagesAsync(scenario.Host) == 1, TimeSpan.FromSeconds(10),
            "The DLQ stream holds the dead letter");
        var stored = await LastAsync(scenario.Host, scenario.Host.DeadLetterSubject(subject));
        var headers = NatsDeadLetters.ParseHeaders(stored.Hdrs);

        CollectionAssert.AreEqual(payload, stored.Data.ToArray());
        Assert.AreEqual("kept", headers["x-custom"].ToString());
        Assert.AreEqual("corr-1", headers["X-Correlation-Id"].ToString());
        Assert.AreEqual(subject, headers["X-Original-Subject"].ToString());
        Assert.AreEqual("business-id-1", headers["X-Original-Msg-Id"].ToString());
        Assert.AreEqual(scenario.Stream.Name, headers["X-Stream"].ToString());
        Assert.AreEqual(scenario.Durable, headers["X-Consumer"].ToString());
        Assert.AreEqual(sequence.ToString(), headers["X-Stream-Sequence"].ToString());
        Assert.AreEqual("1", headers["X-Delivery-Count"].ToString());
        Assert.AreEqual(typeof(PoisonMessageException).FullName, headers["X-Failure-Type"].ToString());
        Assert.AreEqual("bad", headers["X-Failure-Reason"].ToString());
        Assert.IsTrue(headers.ContainsKey("X-Failed-At"));

        // The only Nats-* header is the dead-letter write's own message id; the original ones are dropped.
        var natsHeaders = headers.Keys.Where(k => k.StartsWith("Nats-", StringComparison.OrdinalIgnoreCase)).ToList();
        CollectionAssert.AreEqual(new[] { "Nats-Msg-Id" }, natsHeaders, string.Join(", ", headers.Keys));
        AssertDeadLetterId(scenario.Stream.Name, sequence, scenario.Durable, headers["Nats-Msg-Id"].ToString());

        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()).AckFloor.StreamSeq == sequence,
            TimeSpan.FromSeconds(5), "The source message is acked");
    }

    [TestMethod]
    public async Task SourceMessage_IsAckedOnlyAfterAPositivePubAck()
    {
        var probe = new Probe<MessageContext<OrderEvent>> { Behaviour = (_, _, _) => throw new PoisonMessageException("bad") };
        await using var host = await StartWithoutDeadLetterStreamAsync<OrderHandler>([probe],
            o => o.ConsumerTestHooks.DeadLetterRetryDelays = [TimeSpan.FromMilliseconds(300)]);

        // No stream captures the prefix, so this subscription stands in for the DLQ stream and controls the PubAcks.
        await using var fake = TestHost.CreateConnection();
        var requests = 0;
        var secondRequest = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        await using var sub = await fake.SubscribeCoreAsync<byte[]>($"{host.Options.DeadLetterSubjectPrefix}.>");
        var responder = Task.Run(async () =>
        {
            await foreach (var request in sub.Msgs.ReadAllAsync())
            {
                if (++requests == 1)
                {
                    await request.ReplyAsync(Encoding.UTF8.GetBytes(
                        "{\"error\":{\"code\":500,\"err_code\":10077,\"description\":\"fake store failure\"}}"));
                    continue;
                }

                secondRequest.TrySetResult();
                await release.Task;
                await request.ReplyAsync(Encoding.UTF8.GetBytes("{\"stream\":\"FAKE_DLQ\",\"seq\":1}"));
                return;
            }
        });
        await fake.PingAsync();

        var published = await host.Publisher.PublishAsync($"{host.Root}.created", new OrderEvent("ORD-1", 1m));
        await secondRequest.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Task.Delay(1000);

        var held = await host.ConsumerInfoAsync();
        Assert.AreEqual(0UL, held.AckFloor.StreamSeq, "Not acked after an error PubAck or while the PubAck is pending");
        Assert.AreEqual(1L, held.NumAckPending);

        release.SetResult();
        await responder.WaitAsync(TimeSpan.FromSeconds(5));
        await Eventually.TrueAsync(async () => (await host.ConsumerInfoAsync()).AckFloor.StreamSeq == published.Sequence,
            TimeSpan.FromSeconds(5), "Acked after the positive PubAck");
        Assert.AreEqual(1, probe.Count, "The inline retry wrote again without running the handler again");
    }

    [TestMethod]
    public async Task WithNoDeadLetterStream_TheMessageStaysAndIsRedelivered_UntilAStreamCapturesTheDlqSubjects()
    {
        using var metrics = new MetricCapture();
        var probe = new Probe<MessageContext<OrderEvent>> { Behaviour = (_, _, _) => throw new PoisonMessageException("bad") };
        await using var host = await StartWithoutDeadLetterStreamAsync<OrderHandler>([probe], o =>
        {
            o.PublishTimeout = TimeSpan.FromSeconds(1);
            o.ConsumerTestHooks.DeadLetterRetryDelays = [];
            o.ConsumerTestHooks.DeadLetterNakDelay = TimeSpan.FromSeconds(1);
        });

        var published = await host.Publisher.PublishAsync($"{host.Root}.created", new OrderEvent("ORD-1", 1m));
        var calls = await probe.WaitForCallsAsync(2, TimeSpan.FromSeconds(15));

        Assert.AreEqual(2, calls[1].Arg.DeliveryCount, "Redelivered after the failed write");
        Assert.AreEqual(ComponentState.Retrying, host.Status.Get("dlq")!.State);
        Assert.AreEqual(0UL, (await host.ConsumerInfoAsync()).AckFloor.StreamSeq, "Never acked while the write fails");
        Assert.AreEqual(1L, (await host.Host.JetStream.GetStreamAsync(host.Stream.Name)).Info.State.Messages);
        Assert.IsNotEmpty(metrics.Of("messaging.nats.dead_letter.failures", "messaging.consumer.group.name", host.Durable));

        var lateDlq = TestNames.Unique("DLQ_LATE");
        await using var setup = TestHost.CreateConnection();
        var js = new NatsJSContext(setup);
        await js.CreateStreamAsync(new StreamConfig(lateDlq, [$"{host.Options.DeadLetterSubjectPrefix}.>"]));
        try
        {
            await Eventually.TrueAsync(async () => (await js.GetStreamAsync(lateDlq)).Info.State.Messages == 1,
                TimeSpan.FromSeconds(15), "The message lands in the DLQ once a stream captures it");
            await Eventually.TrueAsync(
                async () => (await host.ConsumerInfoAsync()).AckFloor.StreamSeq == published.Sequence,
                TimeSpan.FromSeconds(5), "The source message is acked after the write");
        }
        finally
        {
            await js.DeleteStreamAsync(lateDlq);
        }
    }

    [TestMethod]
    public void DeclaredDeadLetterStreamWithInterestRetention_FailsRegistration()
    {
        var error = Assert.ThrowsExactly<ArgumentException>(() => TestHost.Create(o =>
            o.DeadLetterStream!.Retention = StreamConfigRetention.Interest));

        StringAssert.Contains(error.Message, "Interest retention");
    }

    [TestMethod]
    public async Task AnInterestStreamCapturingTheDlqSubjects_FaultsTheDlqCheck_AndTheMessageStaysInItsStream()
    {
        var prefix = TestNames.Unique("dlq");
        var interestDlq = TestNames.Unique("DLQI");
        await using var setup = TestHost.CreateConnection();
        var js = new NatsJSContext(setup);
        await js.CreateStreamAsync(new StreamConfig(interestDlq, [$"{prefix}.>"]) { Retention = StreamConfigRetention.Interest });
        try
        {
            var probe = new Probe<MessageContext<OrderEvent>> { Behaviour = (_, _, _) => throw new PoisonMessageException("bad") };
            await using var host = await StartWithoutDeadLetterStreamAsync<OrderHandler>([probe], o =>
            {
                o.DeadLetterSubjectPrefix = prefix;
                o.ConsumerTestHooks.DeadLetterRetryDelays = [];
                o.ConsumerTestHooks.DeadLetterNakDelay = TimeSpan.FromSeconds(1);
            });

            var faulted = await host.Host.WaitForStateAsync("dlq", ComponentState.Faulted);
            StringAssert.Contains(faulted.Error, "Interest retention");

            await host.Publisher.PublishAsync($"{host.Root}.created", new OrderEvent("ORD-1", 1m));
            await probe.WaitForCallsAsync(2, TimeSpan.FromSeconds(15));

            Assert.AreEqual(0UL, (await host.ConsumerInfoAsync()).AckFloor.StreamSeq,
                "The source is never acked on a PubAck from a stream that keeps nothing");
            Assert.AreEqual(1L, (await js.GetStreamAsync(host.Stream.Name)).Info.State.Messages);
        }
        finally
        {
            await js.DeleteStreamAsync(interestDlq);
        }
    }

    [TestMethod]
    public async Task DeadLetterWrite_IsIdempotentPerMessageAndDurable()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("DLQ_IDEMPOTENT");
        var prefix = TestNames.Unique("dlq");
        var dlq = StreamDefinition.DeadLetter(TestNames.Unique("DLQ"), $"{prefix}.>");
        await using var connection = TestHost.CreateConnection();
        var js = new NatsJSContext(connection);
        await js.CreateStreamAsync(stream.ToStreamConfig());
        await js.CreateStreamAsync(dlq.ToStreamConfig());
        (await js.PublishAsync($"{root}.created", Encoding.UTF8.GetBytes("{}"))).EnsureSuccess();
        var consumer = await js.CreateOrUpdateConsumerAsync(stream.Name, new ConsumerConfig(TestNames.Unique("raw")));
        INatsJSMsg<byte[]>? delivered = null;
        await foreach (var msg in consumer.FetchAsync(new NatsJSFetchOpts { MaxMsgs = 1 }, NatsRawSerializer<byte[]>.Default))
        {
            delivered = msg;
        }

        var writer = new DeadLetterWriter(connection, new NatsMessagingOptions(), NullLogger.Instance);
        var subject = $"{prefix}.{root}.created";
        var first = await writer.WriteAsync(subject, delivered!, "durable_a", "Failure", "first", CancellationToken.None);
        var again = await writer.WriteAsync(subject, delivered!, "durable_a", "Failure", "again", CancellationToken.None);
        var other = await writer.WriteAsync(subject, delivered!, "durable_b", "Failure", "other", CancellationToken.None);

        Assert.IsFalse(first.Duplicate);
        Assert.IsTrue(again.Duplicate, "The same message from the same durable is stored once");
        Assert.IsFalse(other.Duplicate, "Another durable gets its own entry");
        Assert.AreEqual(2L, (await js.GetStreamAsync(dlq.Name)).Info.State.Messages);
    }

    [TestMethod]
    public async Task TwoFanOutDurables_FailingTheSameMessage_GiveTwoEntries()
    {
        var probes = new FanOutProbes();
        probes.First.Behaviour = (_, _, _) => throw new PoisonMessageException("first");
        probes.Second.Behaviour = (_, _, _) => throw new PoisonMessageException("second");
        await using var fanOut = await FanOut.StartAsync(probes);
        await using var deadLetters = await fanOut.Host.CollectDeadLettersAsync();

        var published = await fanOut.Host.Publisher.PublishAsync(fanOut.Subject, new OrderEvent("ORD-1", 1m));
        var entries = await deadLetters.WaitForAsync(2, TimeSpan.FromSeconds(10));
        await Task.Delay(500);

        Assert.HasCount(2, deadLetters.Received);
        CollectionAssert.AreEquivalent(new[] { fanOut.FirstDurable, fanOut.SecondDurable },
            entries.Select(e => e.Header("X-Consumer")).ToArray());
        Assert.IsTrue(entries.All(e => e.Header("X-Original-Msg-Id") == published.MessageId));
    }

    [TestMethod]
    public async Task Replay_RepublishesTheEntry_DeletesIt_AndKeepsTheOriginalMessageId()
    {
        var failing = 1;
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, _, _) => Volatile.Read(ref failing) == 1 ? throw new PoisonMessageException("bad") : Task.CompletedTask
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        var subject = $"{scenario.Root}.created";

        var published = await scenario.Publisher.PublishAsync(subject, new OrderEvent("ORD-1", 1m));
        await Eventually.TrueAsync(async () => await MessagesAsync(scenario.Host) == 1, TimeSpan.FromSeconds(10),
            "The message is dead-lettered");
        Volatile.Write(ref failing, 0);

        var result = await scenario.Host.Services.GetRequiredService<NatsDeadLetters>()
            .ReplayAsync(new ReplayOptions { SubjectFilter = subject });

        Assert.AreEqual(1, result.Replayed);
        Assert.IsEmpty(result.Skipped);
        Assert.AreEqual(0L, await MessagesAsync(scenario.Host), "The entry is deleted after the replay is acked");

        var replayed = (await probe.WaitForCallsAsync(2))[1].Arg;
        Assert.AreEqual(published.MessageId, replayed.MessageId);
        Assert.AreEqual("ORD-1", replayed.Message.OrderId);
        Assert.AreEqual(1, replayed.DeliveryCount);
        Assert.AreEqual($"{scenario.Host.DeadLetterStream}:1", replayed.Headers["X-Replayed-From"].ToString());
        Assert.AreEqual(scenario.Durable, replayed.Headers["X-Replay-Consumer"].ToString());
        Assert.IsFalse(replayed.Headers.ContainsKey("X-Failure-Type"));
        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()).NumAckPending == 0,
            TimeSpan.FromSeconds(5), "The replayed message is acked");
    }

    [TestMethod]
    public async Task TargetedReplay_OnAFanOutSubject_RunsOnlyTheDurableThatFailed()
    {
        var probes = new FanOutProbes();
        probes.First.Behaviour = (_, call, _) => call == 1 ? throw new PoisonMessageException("first") : Task.CompletedTask;
        await using var fanOut = await FanOut.StartAsync(probes);

        await fanOut.Host.Publisher.PublishAsync(fanOut.Subject, new OrderEvent("ORD-1", 1m));
        await probes.Second.WaitForCallsAsync(1);
        await Eventually.TrueAsync(async () => await MessagesAsync(fanOut.Host) == 1, TimeSpan.FromSeconds(10),
            "The first durable's failure is dead-lettered");

        var result = await fanOut.Host.Services.GetRequiredService<NatsDeadLetters>().ReplayAsync(new ReplayOptions());

        Assert.AreEqual(1, result.Replayed);
        await probes.First.WaitForCallsAsync(2);
        await Eventually.TrueAsync(
            async () => (await fanOut.Host.ConsumerInfoAsync(fanOut.Stream.Name, fanOut.SecondDurable)).AckFloor.StreamSeq == 2,
            TimeSpan.FromSeconds(5), "The other durable acks the replay");
        await Task.Delay(500);
        Assert.AreEqual(1, probes.Second.Count, "The other durable skips the replay without running its handler");
    }

    [TestMethod]
    public async Task TargetedReplay_ForADurableThatNoLongerExists_IsSkippedAndKept()
    {
        var probe = new Probe<MessageContext<OrderEvent>> { Behaviour = (_, _, _) => throw new PoisonMessageException("bad") };
        var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        var prefix = scenario.Host.Options.DeadLetterSubjectPrefix;
        var dlq = scenario.Host.DeadLetterStream;
        await using (scenario)
        {
            await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));
            await Eventually.TrueAsync(async () => await MessagesAsync(scenario.Host) == 1, TimeSpan.FromSeconds(10),
                "The message is dead-lettered");
        }

        await using var setup = TestHost.CreateConnection();
        var js = new NatsJSContext(setup);
        await js.DeleteConsumerAsync(scenario.Stream.Name, scenario.Durable);

        // A publish-only host on the same DLQ, as an operator's replay tool would be.
        await using var tool = await TestHost.StartAsync(o =>
        {
            o.DeadLetterSubjectPrefix = prefix;
            o.DeadLetterStream = StreamDefinition.DeadLetter(dlq, $"{prefix}.>");
        });
        var result = await tool.Services.GetRequiredService<NatsDeadLetters>().ReplayAsync(new ReplayOptions());

        Assert.AreEqual(0, result.Replayed);
        var skip = result.Skipped.Single();
        Assert.AreEqual(1UL, skip.DeadLetterSequence);
        StringAssert.Contains(skip.Reason, scenario.Durable);
        Assert.AreEqual(1L, (await js.GetStreamAsync(dlq)).Info.State.Messages, "The entry is kept");
        Assert.AreEqual(1L, (await js.GetStreamAsync(scenario.Stream.Name)).Info.State.Messages,
            "Nothing was republished to the source stream");
    }

    [TestMethod]
    public async Task TargetedReplay_ToTheOrphanOfARenamedHandler_IsSkippedAndKept()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("ORPHANREPLAY");
        var queueGroup = TestNames.Unique("qg");
        var probe = new Probe<MessageContext<OrderEvent>> { Behaviour = (_, _, _) => throw new PoisonMessageException("bad") };
        var before = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup });
        var orphan = before.Durable;
        var dlq = before.Host.DeadLetterStream;
        var prefix = before.Host.Options.DeadLetterSubjectPrefix;
        await using (before)
        {
            await before.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m));
            await Eventually.TrueAsync(async () => await MessagesAsync(before.Host) == 1, TimeSpan.FromSeconds(10),
                "The message is dead-lettered");
        }

        // The handler class is renamed: the new durable runs, the old one stays on the server with nothing pulling.
        var probes = new FanOutProbes();
        await using var after = await ConsumerScenario.StartAsync<FirstFanOutHandler>(stream, root, [probes], o =>
            {
                o.DeadLetterSubjectPrefix = prefix;
                o.DeadLetterStream = StreamDefinition.DeadLetter(dlq, $"{prefix}.>");
            },
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup });

        var result = await after.Host.Services.GetRequiredService<NatsDeadLetters>().ReplayAsync(new ReplayOptions());

        Assert.AreEqual(0, result.Replayed);
        var skip = result.Skipped.Single();
        StringAssert.Contains(skip.Reason, orphan);
        StringAssert.Contains(skip.Reason, "no running handler");
        Assert.AreEqual(1L, await MessagesAsync(after.Host), "The entry is kept");
        Assert.AreEqual(1L, (await after.Host.JetStream.GetStreamAsync(stream.Name)).Info.State.Messages,
            "Nothing was republished, so the orphan durable holds no copy");
        Assert.AreEqual(0, probes.First.Count);
    }

    [TestMethod]
    public async Task TargetedReplay_WhenAnotherStreamNowCapturesTheSubject_IsSkippedAndKept()
    {
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([new Probe<MessageContext<OrderEvent>>()],
            null, new NatsConsumerAttribute { Subject = ".created" });
        var (moved, movedRoot) = ConsumerBindings.UniqueStream("MOVED");
        await scenario.Host.JetStream.CreateStreamAsync(moved.ToStreamConfig());
        try
        {
            // An entry written by the scenario's durable, for a subject another stream captures now.
            var target = $"{movedRoot}.created";
            await scenario.PublishRawAsync(scenario.Host.DeadLetterSubject(target), Encoding.UTF8.GetBytes("{}"),
                new NatsHeaders
                {
                    ["X-Original-Subject"] = target,
                    ["X-Stream"] = scenario.Stream.Name,
                    ["X-Consumer"] = scenario.Durable
                });

            var result = await scenario.Host.Services.GetRequiredService<NatsDeadLetters>().ReplayAsync(new ReplayOptions());

            Assert.AreEqual(0, result.Replayed);
            StringAssert.Contains(result.Skipped.Single().Reason, $"now captured by {moved.Name}");
            Assert.AreEqual(1L, await MessagesAsync(scenario.Host), "The entry is kept");
            Assert.AreEqual(0L, (await scenario.Host.JetStream.GetStreamAsync(moved.Name)).Info.State.Messages);
        }
        finally
        {
            await scenario.Host.JetStream.DeleteStreamAsync(moved.Name);
        }
    }

    [TestMethod]
    public async Task UntargetedReplay_ToAnInterestStreamThatNoConsumerFiltersTheSubjectOn_IsSkippedAndKept()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("INTREPLAY", StreamConfigRetention.Interest);
        var probe = new Probe<MessageContext<OrderEvent>> { Behaviour = (_, _, _) => throw new PoisonMessageException("bad") };
        var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        var prefix = scenario.Host.Options.DeadLetterSubjectPrefix;
        var dlq = scenario.Host.DeadLetterStream;
        await using (scenario)
        {
            await scenario.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m));
            await Eventually.TrueAsync(async () => await MessagesAsync(scenario.Host) == 1, TimeSpan.FromSeconds(10),
                "The message is dead-lettered");
        }

        // The failing handler was removed: no consumer is interested in the subject any more.
        await using var setup = TestHost.CreateConnection();
        var js = new NatsJSContext(setup);
        await js.DeleteConsumerAsync(stream.Name, scenario.Durable);
        var lastSequence = (await js.GetStreamAsync(stream.Name)).Info.State.LastSeq;

        await using var tool = await TestHost.StartAsync(o =>
        {
            o.DeadLetterSubjectPrefix = prefix;
            o.DeadLetterStream = StreamDefinition.DeadLetter(dlq, $"{prefix}.>");
        });
        var deadLetters = tool.Services.GetRequiredService<NatsDeadLetters>();
        var result = await deadLetters.ReplayAsync(new ReplayOptions { Targeted = false });

        Assert.AreEqual(0, result.Replayed);
        StringAssert.Contains(result.Skipped.Single().Reason, "Interest retention");
        Assert.AreEqual(1L, (await js.GetStreamAsync(dlq)).Info.State.Messages, "The entry, the only copy, is kept");
        Assert.AreEqual(lastSequence, (await js.GetStreamAsync(stream.Name)).Info.State.LastSeq, "Nothing was published");

        // Once a consumer filters the subject again, the replay goes through and is kept by the stream.
        await js.CreateOrUpdateConsumerAsync(stream.Name, new ConsumerConfig(TestNames.Unique("restored"))
        {
            FilterSubject = $"{root}.>",
            AckPolicy = ConsumerConfigAckPolicy.Explicit
        });
        result = await deadLetters.ReplayAsync(new ReplayOptions { Targeted = false });

        Assert.AreEqual(1, result.Replayed);
        Assert.AreEqual(0L, (await js.GetStreamAsync(dlq)).Info.State.Messages);
        Assert.AreEqual(1L, (await js.GetStreamAsync(stream.Name)).Info.State.Messages, "The stream kept the replay");
    }

    [TestMethod]
    public async Task TargetedReplay_ToADurableThatNoLongerFiltersTheSubject_IsSkippedAndKept()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("REFILTERED");
        var queueGroup = TestNames.Unique("qg");
        var probe = new Probe<MessageContext<OrderEvent>> { Behaviour = (_, _, _) => throw new PoisonMessageException("bad") };
        var before = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup });
        var dlq = before.Host.DeadLetterStream;
        var prefix = before.Host.Options.DeadLetterSubjectPrefix;
        await using (before)
        {
            await before.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m));
            await Eventually.TrueAsync(async () => await MessagesAsync(before.Host) == 1, TimeSpan.FromSeconds(10),
                "The message is dead-lettered");
        }

        // The same durable now consumes another subject, so a replay to it would reach no handler.
        await using var after = await ConsumerScenario.StartAsync<OrderHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>()], o =>
            {
                o.DeadLetterSubjectPrefix = prefix;
                o.DeadLetterStream = StreamDefinition.DeadLetter(dlq, $"{prefix}.>");
            },
            new NatsConsumerAttribute { Subject = ".updated", QueueGroup = queueGroup });
        Assert.AreEqual(before.Durable, after.Durable);

        var result = await after.Host.Services.GetRequiredService<NatsDeadLetters>().ReplayAsync(new ReplayOptions());

        Assert.AreEqual(0, result.Replayed);
        StringAssert.Contains(result.Skipped.Single().Reason, "no longer filters");
        Assert.AreEqual(1L, await MessagesAsync(after.Host), "The entry is kept");
    }

    [TestMethod]
    public async Task UntargetedReplay_RunsEveryDurableOnTheSubject()
    {
        var probes = new FanOutProbes();
        probes.First.Behaviour = (_, call, _) => call == 1 ? throw new PoisonMessageException("first") : Task.CompletedTask;
        await using var fanOut = await FanOut.StartAsync(probes);

        var published = await fanOut.Host.Publisher.PublishAsync(fanOut.Subject, new OrderEvent("ORD-1", 1m));
        await probes.Second.WaitForCallsAsync(1);
        await Eventually.TrueAsync(async () => await MessagesAsync(fanOut.Host) == 1, TimeSpan.FromSeconds(10),
            "The first durable's failure is dead-lettered");

        var result = await fanOut.Host.Services.GetRequiredService<NatsDeadLetters>()
            .ReplayAsync(new ReplayOptions { Targeted = false });

        Assert.AreEqual(1, result.Replayed);
        var first = (await probes.First.WaitForCallsAsync(2))[1].Arg;
        var second = (await probes.Second.WaitForCallsAsync(2))[1].Arg;
        Assert.IsFalse(first.Headers.ContainsKey("X-Replay-Consumer"));
        Assert.AreEqual(published.MessageId, first.MessageId);
        Assert.AreEqual(published.MessageId, second.MessageId);
    }

    [TestMethod]
    public async Task FiniteServerMaxDeliver_AfterARejectedUpdate_IsAComponentWarningNamingTheFix()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("STRAND", StreamConfigRetention.Workqueue);
        var queueGroup = TestNames.Unique("worker");
        var durable = $"{queueGroup}_{nameof(OrderHandler)}";
        await using (var setup = TestHost.CreateConnection())
        {
            var js = new NatsJSContext(setup);
            await js.CreateStreamAsync(stream.ToStreamConfig());
            await js.CreateOrUpdateConsumerAsync(stream.Name, new ConsumerConfig(TestNames.Unique("other"))
            {
                FilterSubject = $"{root}.other",
                AckPolicy = ConsumerConfigAckPolicy.Explicit
            });
            await js.CreateOrUpdateConsumerAsync(stream.Name, new ConsumerConfig(durable)
            {
                FilterSubject = $"{root}.created",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                MaxDeliver = 10
            });
        }

        // The wider filter overlaps the other consumer on a WorkQueue stream (10100), so MaxDeliver stays 10.
        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".>", QueueGroup = queueGroup });

        var status = scenario.Host.Status.Get(scenario.Component)!;
        Assert.AreEqual(ComponentState.Running, status.State);
        StringAssert.Contains(status.Warning, "MaxDeliver");
        StringAssert.Contains(status.Warning, "is 10");
        StringAssert.Contains(status.Warning, "delete the durable");
        Assert.AreEqual(10L, (await scenario.InfoAsync()).Config.MaxDeliver);
        Assert.AreEqual(HealthStatus.Degraded, (await scenario.Host.CheckHealthAsync()).Status);
    }

    [TestMethod]
    public async Task AckWaitLongerThanTheDlqDuplicateWindow_LogsAWarning()
    {
        var logs = new LogCapture();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(
            [new Probe<MessageContext<OrderEvent>>(), logs],
            o => o.DeadLetterStream!.DuplicateWindow = TimeSpan.FromSeconds(2),
            new NatsConsumerAttribute { Subject = ".created", AckWaitSeconds = 5 });

        var warning = await logs.WaitForAsync(e => e.Level == LogLevel.Warning && e.Message.Contains("DuplicateWindow"),
            TimeSpan.FromSeconds(5), "about the DLQ DuplicateWindow");
        StringAssert.Contains(warning.Message, scenario.Durable);
    }

    private static async Task<long> MessagesAsync(TestHost host) =>
        (await host.JetStream.GetStreamAsync(host.DeadLetterStream)).Info.State.Messages;

    private static async Task<StoredMessage> LastAsync(TestHost host, string subject)
    {
        var stream = await host.JetStream.GetStreamAsync(host.DeadLetterStream);
        return (await stream.GetAsync(new StreamMsgGetRequest { LastBySubj = subject })).Message;
    }

    private static async Task<TestHost> StartFlakyAsync(FlakyHandlerState state, StreamDefinition stream,
        NatsConsumerAttribute attribute)
    {
        attribute.Stream = stream.Name;
        attribute.QueueGroup = TestNames.Unique("flaky");
        var host = await TestHost.StartAsync(o =>
            {
                o.StreamDefinitions = [stream];
                o.Bind<FlakyHandler>(attribute);
            },
            services => services.AddSingleton(state));
        await host.WaitUntilStartedAsync();
        return host;
    }

    /// <summary>
    /// A host with <typeparamref name="THandler"/> on "{root}.created" and DeadLetterStream = null, so the
    /// dead-letter subjects have no stream until the test creates one. Waits for the consumer only: "dlq" retries.
    /// </summary>
    private static async Task<NoDlqHost> StartWithoutDeadLetterStreamAsync<THandler>(IEnumerable<object> singletons,
        Action<NatsMessagingOptions> configure)
    {
        var (stream, root) = ConsumerBindings.UniqueStream("NODLQ");
        var queueGroup = TestNames.Unique("qg");
        var host = await TestHost.StartAsync(o =>
            {
                o.DeadLetterStream = null;
                o.StreamDefinitions = [stream];
                o.Bind<THandler>(new NatsConsumerAttribute
                {
                    Stream = stream.Name, Subject = $"{root}.created", QueueGroup = queueGroup
                });
                configure(o);
            },
            services =>
            {
                foreach (var singleton in singletons)
                {
                    services.AddSingleton(singleton.GetType(), singleton);
                }
            });

        var durable = $"{queueGroup}_{typeof(THandler).Name}";
        var noDlq = new NoDlqHost(host, stream, root, durable);
        await host.WaitForStateAsync($"consumer:{stream.Name}/{durable}", ComponentState.Running);
        return noDlq;
    }

    private sealed record NoDlqHost(TestHost Host, StreamDefinition Stream, string Root, string Durable) : IAsyncDisposable
    {
        public IPublisher Publisher => Host.Publisher;

        public NatsMessagingOptions Options => Host.Options;

        public NatsMessagingStatus Status => Host.Status;

        public Task<ConsumerInfo> ConsumerInfoAsync() => Host.ConsumerInfoAsync(Stream.Name, Durable);

        public ValueTask DisposeAsync() => Host.DisposeAsync();
    }

    [TestMethod]
    public async Task LegacyDeferredMessage_GetsAllItsAttempts_TheDeferralDeliveryDoesNotCount()
    {
        var state = new FlakyHandlerState { Mode = FlakyHandler.Mode.AlwaysThrow };
        var (stream, root) = ConsumerBindings.UniqueStream("DLQ_LEGACYDEFER");
        var subject = $"{root}.created";

        await using var host = await StartFlakyAsync(state, stream, new NatsConsumerAttribute
        {
            Subject = subject, MaxDeliveryAttempts = 3, AckWaitSeconds = 5, RetryDelaysSeconds = [0]
        });

        await using var connection = CreateConnection();
        await using var dlqSub = await connection.SubscribeCoreAsync<OrderEvent>(host.DeadLetterSubject(subject));
        await connection.PingAsync();

        // As the 4.x publisher wrote it: whole seconds, invariant culture.
        var until = DateTime.UtcNow.AddSeconds(3).ToString(System.Globalization.CultureInfo.InvariantCulture);
        var ack = await new NatsJSContext(connection).PublishAsync(subject,
            System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new OrderEvent("ORD-DEFERRED", 1m)),
            NatsRawSerializer<byte[]>.Default, headers: new NatsHeaders { ["Msg-DeferredUntil"] = until });
        ack.EnsureSuccess();

        var dlqMsg = await NextAsync(dlqSub, TimeSpan.FromSeconds(30));

        Assert.AreEqual("ORD-DEFERRED", dlqMsg.Data!.OrderId);
        Assert.AreEqual(3, Volatile.Read(ref state.Attempts), "The handler runs MaxDeliveryAttempts times");
        Assert.AreEqual("4", dlqMsg.Headers!["X-Delivery-Count"].ToString(), "One deferral delivery plus three attempts");
    }

    [TestMethod]
    public async Task StreamDeletedAndCreatedAgain_ADeadLetterWithAReusedSequence_IsStoredAgain()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("DLQ_REUSED");
        var prefix = TestNames.Unique("dlq");
        var deadLetterStream = TestNames.Unique("DLQ");
        var queueGroup = TestNames.Unique("qg");
        var probe = new Probe<MessageContext<OrderEvent>> { Behaviour = (_, _, _) => throw new PoisonMessageException("bad") };

        void Configure(NatsMessagingOptions o)
        {
            o.DeadLetterSubjectPrefix = prefix;
            o.DeadLetterStream = StreamDefinition.DeadLetter(deadLetterStream, $"{prefix}.>");
        }

        ulong firstSequence;
        await using (var first = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], Configure,
                         new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup }))
        {
            firstSequence = (await first.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-FIRST", 1m))).Sequence;
            await Eventually.TrueAsync(async () => (await first.InfoAsync()).AckFloor.StreamSeq == firstSequence,
                TimeSpan.FromSeconds(10), "The first message is dead-lettered and acked");
            await first.Host.JetStream.DeleteStreamAsync(stream.Name);
        }

        await using var second = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], Configure,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup });
        var secondSequence = (await second.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-SECOND", 2m))).Sequence;
        Assert.AreEqual(firstSequence, secondSequence, "The new stream restarts at sequence 1");

        await Eventually.TrueAsync(async () => (await second.Host.JetStream.GetStreamAsync(deadLetterStream)).Info.State.Messages == 2,
            TimeSpan.FromSeconds(10), "Both messages are in the dead-letter stream");
        await using var dead = await StreamCollector.StartAsync(second.Host.JetStream, deadLetterStream, $"{prefix}.>");
        var stored = await dead.WaitForAsync(2, TimeSpan.FromSeconds(10));
        CollectionAssert.AreEquivalent(new[] { "ORD-FIRST", "ORD-SECOND" },
            stored.Select(s => s.Json<OrderEvent>().OrderId).ToArray());
    }

    /// <summary>
    /// "{stream}:{sequence}:{stored UTC ticks}:{durable}", with a stored time from the last minute.
    /// </summary>
    internal static void AssertDeadLetterId(string stream, ulong sequence, string durable, string? id)
    {
        Assert.IsNotNull(id);
        StringAssert.StartsWith(id, $"{stream}:{sequence}:");
        StringAssert.EndsWith(id, $":{durable}");
        var ticks = long.Parse(id[$"{stream}:{sequence}:".Length..^$":{durable}".Length]);
        var stored = new DateTimeOffset(ticks, TimeSpan.Zero);
        Assert.IsLessThan(TimeSpan.FromMinutes(1), (DateTimeOffset.UtcNow - stored).Duration(), id);
    }

    private static async Task<NatsMsg<T>> NextAsync<T>(INatsSub<T> sub, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        return await sub.Msgs.ReadAsync(cts.Token);
    }
}

/// <summary>
/// One host with <see cref="FirstFanOutHandler"/> and <see cref="SecondFanOutHandler"/> on the same subject of a
/// unique stream: two durables, each receiving every message.
/// </summary>
public sealed class FanOut : IAsyncDisposable
{
    private FanOut(TestHost host, StreamDefinition stream, string subject, string queueGroup)
    {
        Host = host;
        Stream = stream;
        Subject = subject;
        FirstDurable = $"{queueGroup}_{nameof(FirstFanOutHandler)}";
        SecondDurable = $"{queueGroup}_{nameof(SecondFanOutHandler)}";
    }

    public TestHost Host { get; }

    public StreamDefinition Stream { get; }

    public string Subject { get; }

    public string FirstDurable { get; }

    public string SecondDurable { get; }

    public static async Task<FanOut> StartAsync(FanOutProbes probes, Action<NatsMessagingOptions>? configure = null)
    {
        var (stream, root) = ConsumerBindings.UniqueStream("FANOUT");
        var subject = $"{root}.created";
        var queueGroup = TestNames.Unique("qg");
        var host = await TestHost.StartAsync(o =>
            {
                o.StreamDefinitions = [stream];
                o.Bind<FirstFanOutHandler>(new NatsConsumerAttribute
                {
                    Stream = stream.Name, Subject = subject, QueueGroup = queueGroup, RetryDelaysSeconds = [0]
                });
                o.Bind<SecondFanOutHandler>(new NatsConsumerAttribute
                {
                    Stream = stream.Name, Subject = subject, QueueGroup = queueGroup, RetryDelaysSeconds = [0]
                });
                configure?.Invoke(o);
            },
            services => services.AddSingleton(probes));
        await host.WaitUntilStartedAsync();
        return new FanOut(host, stream, subject, queueGroup);
    }

    public ValueTask DisposeAsync() => Host.DisposeAsync();
}
