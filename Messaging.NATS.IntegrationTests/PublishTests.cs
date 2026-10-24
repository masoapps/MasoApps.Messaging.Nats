using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.Serializers.Json;

namespace Messaging.NATS.IntegrationTests;

[JsonSerializable(typeof(OrderEvent))]
internal partial class PublishTestJsonContext : JsonSerializerContext;

[TestClass]
public class PublishTests : NatsTestBase
{
    [TestMethod]
    public async Task Publish_ReturnsStreamSequenceAndMessageId()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var (stream, root) = await CreateStreamAsync(js);
        await using var collector = await StreamCollector.StartAsync(js, stream, $"{root}.>");

        var result = await CreatePublisher(connection, js).PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m));

        Assert.AreEqual(stream, result.Stream);
        Assert.AreEqual(1UL, result.Sequence);
        Assert.IsFalse(result.Duplicate);
        Assert.IsNotNull(result.MessageId);
        Assert.AreEqual(32, result.MessageId.Length);

        var received = (await collector.WaitForAsync(1, TimeSpan.FromSeconds(5)))[0];
        Assert.AreEqual(result.MessageId, received.Header("Nats-Msg-Id"));
        Assert.AreEqual("ORD-1", received.Json<OrderEvent>().OrderId);
    }

    [TestMethod]
    public async Task Publish_NoStream_ThrowsNoStreamWithinTheBudget()
    {
        await using var connection = CreateConnection();
        var publisher = CreatePublisher(connection, CreateJetStreamContext(connection));
        var subject = $"{TestNames.Unique("nostream")}.created";

        var stopwatch = Stopwatch.StartNew();
        var error = await Assert.ThrowsExactlyAsync<NatsPublishException>(() =>
            publisher.PublishAsync(subject, new OrderEvent("ORD", 1m), new PublishOptions { Timeout = TimeSpan.FromSeconds(3) }));
        stopwatch.Stop();

        Assert.AreEqual(PublishFailure.NoStream, error.Failure);
        Assert.IsFalse(error.OutcomeUnknown);
        Assert.AreEqual(subject, error.Subject);
        Assert.IsTrue(stopwatch.Elapsed < TimeSpan.FromSeconds(3), stopwatch.Elapsed.ToString());
    }

    [TestMethod]
    public async Task Publish_SameMessageId_IsADuplicate()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var (stream, root) = await CreateStreamAsync(js);
        var publisher = CreatePublisher(connection, js);
        var options = new PublishOptions { MessageId = TestNames.Unique("upload") };

        var first = await publisher.PublishAsync($"{root}.created", new OrderEvent("ORD", 1m), options);
        var second = await publisher.PublishAsync($"{root}.created", new OrderEvent("ORD", 1m), options);

        Assert.IsFalse(first.Duplicate);
        Assert.IsTrue(second.Duplicate);
        Assert.AreEqual(first.Sequence, second.Sequence);
        Assert.AreEqual(options.MessageId, second.MessageId);
        Assert.AreEqual(1L, (await js.GetStreamAsync(stream)).Info.State.Messages);
    }

    [TestMethod]
    public async Task Publish_HeadersCorrelationIdAndSentTime_Arrive()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var (stream, root) = await CreateStreamAsync(js);
        await using var collector = await StreamCollector.StartAsync(js, stream, $"{root}.>");
        var publisher = CreatePublisher(connection, js);
        var headers = new NatsHeaders
        {
            ["X-Tenant"] = "acme",
            ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            ["X-Correlation-Id"] = "forwarded"
        };

        var before = DateTimeOffset.UtcNow;
        await publisher.PublishAsync($"{root}.a", new OrderEvent("A", 1m),
            new PublishOptions { Headers = headers, CorrelationId = "corr-1" });
        await publisher.PublishAsync($"{root}.b", new OrderEvent("B", 1m), new PublishOptions { Headers = headers });
        var after = DateTimeOffset.UtcNow;

        var received = await collector.WaitForAsync(2, TimeSpan.FromSeconds(5));
        var a = received.Single(r => r.Subject == $"{root}.a");
        var b = received.Single(r => r.Subject == $"{root}.b");

        Assert.AreEqual("acme", a.Header("X-Tenant"));
        Assert.AreNotEqual(headers["traceparent"].ToString(), a.Header("traceparent"),
            "A forwarded traceparent is dropped; only the publish span's own is sent");
        Assert.AreEqual("corr-1", a.Header("X-Correlation-Id"));
        Assert.AreEqual("forwarded", b.Header("X-Correlation-Id"), "A forwarded correlation id is kept");

        var sentTime = DateTimeOffset.Parse(a.Header("X-Sent-Time")!, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
        Assert.IsTrue(sentTime >= before && sentTime <= after, $"{sentTime:O} not in {before:O}..{after:O}");
        StringAssert.Matches(a.Header("X-Sent-Time"), new System.Text.RegularExpressions.Regex(@"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{7}Z$"));
        Assert.AreEqual(3, headers.Count, "The caller's headers are not changed");
    }

    [TestMethod]
    public async Task Publish_ForwardedNatsAndLibraryHeaders_AreDropped()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var (stream, root) = await CreateStreamAsync(js);
        await using var collector = await StreamCollector.StartAsync(js, stream, $"{root}.>");
        var publisher = CreatePublisher(connection, js);

        // Headers a handler would forward from a fired schedule or a dead letter. Each of the Nats-* ones would
        // change what the server does: a duplicate drop, a rollup, a rejected expectation, a holder purge.
        var forwarded = new NatsHeaders
        {
            ["Nats-Msg-Id"] = "forwarded-id",
            ["nats-rollup"] = "sub",
            ["Nats-Expected-Last-Sequence"] = "999",
            ["Nats-Scheduler"] = $"_sched.{stream}.{root}.x.k",
            ["Nats-Schedule-Next"] = "purge",
            ["X-Schedule-Id"] = "old-schedule",
            ["X-Scheduled-For"] = "2020-01-01T00:00:00Z",
            ["X-Sent-Time"] = "2020-01-01T00:00:00Z",
            ["X-Original-Subject"] = "somewhere",
            ["X-Failure-Reason"] = "boom",
            ["X-Delivery-Count"] = "3",
            ["X-Consumer"] = "durable",
            ["X-Stream"] = "SOURCE",
            ["X-Stream-Sequence"] = "1",
            ["X-Replay-Consumer"] = "durable",
            ["X-Kept"] = "yes",
            ["X-Streaming-Mode"] = "live"
        };

        var first = await publisher.PublishAsync($"{root}.a", new OrderEvent("A", 1m), new PublishOptions { Headers = forwarded });
        var second = await publisher.PublishAsync($"{root}.a", new OrderEvent("A", 1m), new PublishOptions { Headers = forwarded });

        Assert.IsFalse(second.Duplicate, "The forwarded Nats-Msg-Id must not cause a duplicate drop");
        Assert.AreNotEqual(first.Sequence, second.Sequence);

        var received = await collector.WaitForAsync(2, TimeSpan.FromSeconds(5));
        foreach (var message in received)
        {
            Assert.AreEqual("yes", message.Header("X-Kept"));
            Assert.AreEqual("live", message.Header("X-Streaming-Mode"), "Only the library's exact X-Stream names are dropped");
            Assert.AreNotEqual("forwarded-id", message.Header("Nats-Msg-Id"));
            Assert.AreNotEqual("2020-01-01T00:00:00Z", message.Header("X-Sent-Time"));
            foreach (var name in new[] { "Nats-Rollup", "Nats-Expected-Last-Sequence", "Nats-Scheduler", "Nats-Schedule-Next",
                         "X-Schedule-Id", "X-Scheduled-For", "X-Original-Subject", "X-Failure-Reason", "X-Delivery-Count",
                         "X-Consumer", "X-Stream", "X-Stream-Sequence", "X-Replay-Consumer" })
            {
                Assert.IsNull(message.Header(name), $"{name} was forwarded");
            }
        }
    }

    [TestMethod]
    public async Task Publish_RetryAttemptsZeroWithoutMessageId_SendsNoMessageId()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var (stream, root) = await CreateStreamAsync(js);
        await using var collector = await StreamCollector.StartAsync(js, stream, $"{root}.>");

        var result = await CreatePublisher(connection, js)
            .PublishAsync($"{root}.a", new OrderEvent("A", 1m), new PublishOptions { RetryAttempts = 0 });

        Assert.IsNull(result.MessageId);
        var received = (await collector.WaitForAsync(1, TimeSpan.FromSeconds(5)))[0];
        Assert.IsNull(received.Header("Nats-Msg-Id"));
        Assert.IsNotNull(received.Header("X-Sent-Time"));
    }

    [TestMethod]
    public async Task Publish_FireAndForget_ReachesACoreSubscriber()
    {
        await using var connection = CreateConnection();
        var publisher = CreatePublisher(connection, CreateJetStreamContext(connection));
        var subject = $"{TestNames.Unique("core")}.event";

        await using var subscription = await connection.SubscribeCoreAsync(subject, serializer: NatsRawSerializer<byte[]>.Default);
        var next = subscription.Msgs.ReadAsync().AsTask();

        var result = await publisher.PublishAsync(subject, new OrderEvent("FF", 2m),
            new PublishOptions { FireAndForget = true, CorrelationId = "ff" });

        Assert.AreEqual(new PublishResult(string.Empty, 0, false, null), result);
        var message = await next.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreEqual("FF", JsonSerializer.Deserialize<OrderEvent>(message.Data!)!.OrderId);
        Assert.AreEqual("ff", message.Headers!["X-Correlation-Id"].ToString());
        Assert.IsTrue(message.Headers.ContainsKey("X-Sent-Time"));
        Assert.IsFalse(message.Headers.ContainsKey("Nats-Msg-Id"));
    }

    [TestMethod]
    [DataRow(null)]
    [DataRow(0)]
    [DataRow(-5)]
    public async Task LegacyOverload_NullZeroOrNegativeDelay_IsAckedAndArrivesAtOnce(int? delaySeconds)
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var (stream, root) = await CreateStreamAsync(js, schedules: true);
        await using var collector = await StreamCollector.StartAsync(js, stream, $"{root}.>");
        IPublisher publisher = CreatePublisher(connection, js);
        TimeSpan? deferredBy = delaySeconds is { } s ? TimeSpan.FromSeconds(s) : null;

        var stopwatch = Stopwatch.StartNew();
        await publisher.PublishAsync($"{root}.legacy", new TestMessage("now"), deferredBy);
        var received = (await collector.WaitForAsync(1, TimeSpan.FromSeconds(5)))[0];
        stopwatch.Stop();

        Assert.IsTrue(stopwatch.Elapsed < TimeSpan.FromSeconds(1), stopwatch.Elapsed.ToString());
        Assert.AreEqual("now", received.Json<TestMessage>().Content);
        Assert.IsNull(received.Header("X-Schedule-Id"), "Not a schedule");
        Assert.IsNotNull(received.Header("Nats-Msg-Id"), "Acked publish with the default retries");

        // Acked: a subject without a stream throws instead of being dropped.
        var error = await Assert.ThrowsExactlyAsync<NatsPublishException>(() =>
            publisher.PublishAsync($"{TestNames.Unique("nostream")}.legacy", new TestMessage("lost"), deferredBy));
        Assert.AreEqual(PublishFailure.NoStream, error.Failure);
    }

    [TestMethod]
    public async Task PublishWithNullThirdArgument_BindsTheGenericOverload_AndPublishesAtOnce()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var (_, root) = await CreateStreamAsync(js);
        IPublisher publisher = CreatePublisher(connection, js);

        // An explicit null delay binds the generic overload.
        var result = await publisher.PublishAsync($"{root}.a", new TestMessage("x"), null);

        Assert.AreEqual(1UL, result.Sequence);
    }

    [TestMethod]
    public async Task LegacyOverload_WithSourceGeneratedContext_SerialisesTheRuntimeType()
    {
        await using var connection = new NatsConnection(new NatsOpts
        {
            Url = NatsContainerFixture.NatsUrl,
            SerializerRegistry = new NatsJsonContextSerializerRegistry(PublishTestJsonContext.Default)
        });
        var js = CreateJetStreamContext(connection);
        var (stream, root) = await CreateStreamAsync(js);
        await using var collector = await StreamCollector.StartAsync(js, stream, $"{root}.>");
        var publisher = CreatePublisher(connection, js);
        object message = new OrderEvent("ORD-SG", 3.5m);

        await publisher.PublishAsync($"{root}.legacy", message, (TimeSpan?)null);
        await publisher.PublishAsync($"{root}.typed", new OrderEvent("ORD-SG", 3.5m));

        var received = await collector.WaitForAsync(2, TimeSpan.FromSeconds(5));
        var legacy = received.Single(r => r.Subject == $"{root}.legacy");
        var typed = received.Single(r => r.Subject == $"{root}.typed");
        Assert.AreEqual(new OrderEvent("ORD-SG", 3.5m), legacy.Json<OrderEvent>());
        CollectionAssert.AreEqual(typed.Data, legacy.Data, "Same bytes as the typed path");
    }

    [TestMethod]
    public async Task Publish_HonoursCancellation()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var publisher = CreatePublisher(connection, js);
        var subject = $"{TestNames.Unique("silent")}.event";

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            publisher.PublishAsync(subject, new OrderEvent("C", 1m), cancelled.Token));

        // A responder that never answers keeps the attempt waiting until the caller cancels.
        await using var silent = await connection.SubscribeCoreAsync<byte[]>(subject);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var stopwatch = Stopwatch.StartNew();
        await Assert.ThrowsAsync<OperationCanceledException>(() => publisher.PublishAsync(subject, new OrderEvent("C", 1m),
            new PublishOptions { Timeout = TimeSpan.FromSeconds(20) }, cts.Token));
        Assert.IsTrue(stopwatch.Elapsed < TimeSpan.FromSeconds(3), stopwatch.Elapsed.ToString());
    }

    [TestMethod]
    public async Task Publish_ServerDown_IsBoundedByTheTimeout()
    {
        // An OS-picked port instead of NATS_TEST_SPARE_PORT, so this can run in parallel with the startup test
        // that uses the spare port.
        await using var server = await NatsServerProcess.StartAsync(TestEnvironment.UnusedPort());
        await using var connection = new NatsConnection(new NatsOpts
        {
            Url = server.Url,
            SerializerRegistry = NatsJsonSerializerRegistry.Default
        });
        var js = CreateJetStreamContext(connection);
        var (_, root) = await CreateStreamAsync(js);
        var publisher = CreatePublisher(connection, js);
        await publisher.PublishAsync($"{root}.a", new OrderEvent("UP", 1m));

        await server.DisposeAsync();

        var timeout = TimeSpan.FromSeconds(2);
        var stopwatch = Stopwatch.StartNew();
        var error = await Assert.ThrowsExactlyAsync<NatsPublishException>(() =>
            publisher.PublishAsync($"{root}.a", new OrderEvent("DOWN", 1m), new PublishOptions { Timeout = timeout }));
        stopwatch.Stop();

        Assert.AreEqual(PublishFailure.Timeout, error.Failure);
        Assert.IsTrue(error.OutcomeUnknown);
        Assert.IsTrue(stopwatch.Elapsed < timeout + TimeSpan.FromSeconds(1), stopwatch.Elapsed.ToString());
    }

    [TestMethod]
    public async Task Publish_ReplyNeverComes_IsTimeoutWithUnknownOutcome_AndRetriesReuseTheId()
    {
        await using var connection = new NatsConnection(new NatsOpts
        {
            Url = NatsContainerFixture.NatsUrl,
            SerializerRegistry = NatsJsonSerializerRegistry.Default,
            RequestTimeout = TimeSpan.FromSeconds(1)
        });
        var publisher = CreatePublisher(connection, CreateJetStreamContext(connection));
        var subject = $"{TestNames.Unique("silent")}.event";

        var seenIds = new List<string>();
        await using var silent = await connection.SubscribeCoreAsync<byte[]>(subject);
        var listening = Task.Run(async () =>
        {
            await foreach (var msg in silent.Msgs.ReadAllAsync())
            {
                lock (seenIds)
                {
                    seenIds.Add(msg.Headers!["Nats-Msg-Id"].ToString());
                }
            }
        });

        var stopwatch = Stopwatch.StartNew();
        var error = await Assert.ThrowsExactlyAsync<NatsPublishException>(() =>
            publisher.PublishAsync(subject, new OrderEvent("T", 1m),
                new PublishOptions { Timeout = TimeSpan.FromSeconds(3.5), RetryAttempts = 2 }));
        stopwatch.Stop();

        Assert.AreEqual(PublishFailure.Timeout, error.Failure);
        Assert.IsTrue(error.OutcomeUnknown);
        Assert.IsNotNull(error.MessageId);
        Assert.IsTrue(stopwatch.Elapsed >= TimeSpan.FromSeconds(2.9) && stopwatch.Elapsed < TimeSpan.FromSeconds(4.5),
            stopwatch.Elapsed.ToString());

        lock (seenIds)
        {
            Assert.HasCount(3, seenIds, "1 attempt + 2 retries, one per RequestTimeout");
            Assert.IsTrue(seenIds.All(id => id == error.MessageId), string.Join(", ", seenIds));
        }
    }

    private static async Task<(string Stream, string Root)> CreateStreamAsync(NatsJSContext js, bool schedules = false)
    {
        var stream = TestNames.Unique("PUB");
        var root = TestNames.Unique("pub");
        var definition = schedules
            ? StreamDefinition.Scheduled(stream, $"{root}.>")
            : new StreamDefinition { Name = stream, Subjects = [$"{root}.>"] };
        await js.CreateStreamAsync(definition.ToStreamConfig());
        return (stream, root);
    }
}
