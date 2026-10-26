using System.Collections.Concurrent;
using System.Diagnostics;
using Messaging.NATS.IntegrationTests.Handlers;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// R3 behaviour on a 3-node cluster (NATS_CLUSTER_BASE_PORT). The tests share one cluster and run one at a time,
/// because some of them stop nodes. Duplicate fires are found by X-Schedule-Id: two fires of one holder get two
/// stream sequences, while a redelivery to the collector keeps its sequence.
/// </summary>
[TestClass]
[TestCategory("Cluster")]
[DoNotParallelize]
public class ClusterTests : NatsTestBase
{
    private static NatsClusterFixture? _cluster;

    public TestContext TestContext { get; set; } = null!;

    private static NatsClusterFixture Cluster => _cluster!;

    [TestInitialize]
    public async Task StartCluster()
    {
        _cluster ??= await NatsClusterFixture.StartFromEnvironmentAsync();
        foreach (var node in _cluster.Nodes.Where(n => !n.IsRunning))
        {
            await _cluster.RestartServerAsync(node.Name);
        }
    }

    [ClassCleanup]
    public static async Task StopCluster()
    {
        if (_cluster != null)
        {
            await _cluster.DisposeAsync();
            _cluster = null;
        }
    }

    [TestMethod]
    public async Task StreamLeaderStepdown_BeforeTheFireTime_EachScheduleFiresOnce()
    {
        await using var s = await ScheduleStream.CreateAsync(Cluster);
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(3);

        var schedules = await s.ScheduleManyAsync(10, i => deliverAt.AddMilliseconds(i * 50),
            i => i % 2 == 0 ? null : $"k{i}");
        await Task.Delay(TimeSpan.FromSeconds(1));
        var leader = await Cluster.StepDownStreamLeaderAsync(s.Stream);
        TestContext.WriteLine($"new stream leader {leader}");

        await s.AssertFiresAsync(schedules, exactlyOnce: true, TestContext);
    }

    [TestMethod]
    public async Task StreamLeaderKilled_BeforeTheFireTime_AndBackAfterIt_EachScheduleFiresOnce()
    {
        await using var s = await ScheduleStream.CreateAsync(Cluster);
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(3);

        var schedules = await s.ScheduleManyAsync(10, i => deliverAt.AddMilliseconds(i * 50),
            i => i % 2 == 0 ? null : $"k{i}");
        await Task.Delay(TimeSpan.FromSeconds(1));
        var killed = (await Cluster.LeaderOfAsync(s.Stream))!;
        await Cluster.KillServerAsync(killed);
        var leader = await WaitForLeaderOtherThanAsync(s.Stream, killed);
        TestContext.WriteLine($"killed {killed}, new stream leader {leader}");

        await ScheduleRaces.DelayUntil(schedules.Max(x => x.Result.DeliverAt).AddSeconds(2));
        await Cluster.RestartServerAsync(killed);
        await Cluster.WaitForStreamAsync(s.Stream);
        await Task.Delay(TimeSpan.FromSeconds(2));

        await s.AssertFiresAsync(schedules, exactlyOnce: true, TestContext);
    }

    [TestMethod]
    public async Task StreamLeaderStepdown_AtTheFireTime_20Runs_EachFiresAtLeastOnce_AndAnyDuplicateKeepsItsScheduleId()
    {
        await using var s = await ScheduleStream.CreateAsync(Cluster);
        var schedules = new List<Scheduled>();
        var log = new List<string>();

        for (var run = 0; run < 20; run++)
        {
            var offset = TimeSpan.FromMilliseconds(-200 + run * 400.0 / 19);
            var deliverAt = DateTimeOffset.UtcNow.AddSeconds(1.5);
            var scheduled = await s.ScheduleAsync($"run{run}", deliverAt, run % 2 == 0 ? null : "k");
            schedules.Add(scheduled);

            await ScheduleRaces.DelayUntil(deliverAt + offset);
            var stepdownAt = DateTimeOffset.UtcNow;
            var leader = await Cluster.StepDownStreamLeaderAsync(s.Stream);
            log.Add($"run {run,2}: stepdown at {(stepdownAt - deliverAt).TotalMilliseconds,6:F0} ms, new leader {leader}");
        }

        var fires = await s.AssertFiresAsync(schedules, exactlyOnce: false, TestContext);
        foreach (var (line, scheduled) in log.Zip(schedules))
        {
            TestContext.WriteLine($"{line}, fires {fires[scheduled.Result.ScheduleId].Count}");
        }
    }

    [TestMethod]
    public async Task BurstOfSchedules_WithTheStreamLeaderKilledWhileTheyFire_EachFiresAtLeastOnce_NeverEarly()
    {
        await using var s = await ScheduleStream.CreateAsync(Cluster);
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(8);

        // The leader proposes the fires one by one, so a kill at the fire time lands while some are in flight and
        // the new leader fires the rest (its in-flight guard starts empty).
        var schedules = await s.ScheduleManyAsync(2000, _ => deliverAt, i => i % 2 == 0 ? null : $"k{i}");
        var killed = (await Cluster.LeaderOfAsync(s.Stream))!;
        await ScheduleRaces.DelayUntil(deliverAt);
        var firstFire = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(5);
        while (s.Collector.Received.Count == 0 && DateTimeOffset.UtcNow < firstFire)
        {
            await Task.Delay(1);
        }

        var killedAt = DateTimeOffset.UtcNow;
        await Cluster.KillServerAsync(killed);
        var leader = await WaitForLeaderOtherThanAsync(s.Stream, killed);
        await Cluster.RestartServerAsync(killed);

        var fires = await s.AssertFiresAsync(schedules, exactlyOnce: false, TestContext);
        var beforeKill = fires.Values.Count(f => f[0].Stored < killedAt);
        TestContext.WriteLine($"killed {killed} {(killedAt - deliverAt).TotalMilliseconds:F0} ms after the fire time, " +
                              $"new leader {leader}; {beforeKill} fired before the kill, {2000 - beforeKill} after; " +
                              $"{fires.Count(f => f.Value.Count > 1)} of 2000 schedule ids fired more than once");
        if (beforeKill == 0 || beforeKill == 2000)
        {
            Assert.Inconclusive($"The kill did not land inside the burst ({beforeKill} of 2000 fired before it)");
        }
    }

    [TestMethod]
    public async Task HundredSchedules_WithAStreamLeaderStepdownEveryTwoSeconds_EachFiresAtLeastOnce_NeverEarly()
    {
        await using var s = await ScheduleStream.CreateAsync(Cluster);
        var start = DateTimeOffset.UtcNow.AddSeconds(2);

        var schedules = await s.ScheduleManyAsync(100, i => start.AddMilliseconds(i * 100), _ => null);
        var stepdowns = new List<string>();
        var stopwatch = Stopwatch.StartNew();
        for (var i = 1; i <= 6; i++)
        {
            await ScheduleRaces.DelayUntil(start.AddSeconds(2 * i - 1));
            stepdowns.Add($"{stopwatch.Elapsed.TotalSeconds:F1} s: new leader {await Cluster.StepDownStreamLeaderAsync(s.Stream)}");
        }

        var fires = await s.AssertFiresAsync(schedules, exactlyOnce: false, TestContext);
        TestContext.WriteLine(string.Join(Environment.NewLine, stepdowns));
        TestContext.WriteLine($"{fires.Count(f => f.Value.Count > 1)} of 100 schedule ids fired more than once");
    }

    [TestMethod]
    public async Task ReplaceAndCreateIfAbsent_HoldAcrossAStreamLeaderStepdown()
    {
        await using var s = await ScheduleStream.CreateAsync(Cluster);
        var replaceSubject = $"{s.Root}.reschedule";
        var createSubject = $"{s.Root}.arriving";
        var replace = new ScheduleOptions { Key = "journey-1" };
        var create = new ScheduleOptions { Key = "session-1", Mode = ScheduleMode.CreateIfAbsent };
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(4);

        var v1 = await s.Publisher.ScheduleAsync(replaceSubject, new TestMessage("v1"), deliverAt, replace);
        var c1 = await s.Publisher.ScheduleAsync(createSubject, new TestMessage("c1"), deliverAt, create);
        await Cluster.StepDownStreamLeaderAsync(s.Stream);
        var v2 = await s.Publisher.ScheduleAsync(replaceSubject, new TestMessage("v2"), deliverAt.AddMilliseconds(500), replace);
        var c2 = await s.Publisher.ScheduleAsync(createSubject, new TestMessage("c2"), deliverAt.AddMilliseconds(500), create);

        Assert.IsTrue(v1.Created && v2.Created && c1.Created);
        Assert.IsFalse(c2.Created);
        Assert.AreEqual(c1.ScheduleId, c2.ScheduleId);
        Assert.AreEqual(c1.Sequence, c2.Sequence);
        Assert.AreEqual(2L, await s.HolderCountAsync(), "One pending holder per key");

        await ScheduleRaces.DelayUntil(deliverAt.AddSeconds(2.5));
        var received = s.Collector.Received.DistinctBy(r => r.Sequence).ToList();

        var rescheduled = received.Where(r => r.Subject == replaceSubject).ToList();
        Assert.HasCount(1, rescheduled);
        Assert.AreEqual("v2", rescheduled[0].Json<TestMessage>().Content);
        Assert.AreEqual(v2.ScheduleId, rescheduled[0].Header("X-Schedule-Id"));

        var arriving = received.Where(r => r.Subject == createSubject).ToList();
        Assert.HasCount(1, arriving);
        Assert.AreEqual("c1", arriving[0].Json<TestMessage>().Content);
        Assert.AreEqual(c1.ScheduleId, arriving[0].Header("X-Schedule-Id"));
    }

    [TestMethod]
    public async Task CancelAfterAStreamLeaderStepdown_NeverFires()
    {
        await using var s = await ScheduleStream.CreateAsync(Cluster);
        var subject = $"{s.Root}.push";
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(3);

        await s.Publisher.ScheduleAsync(subject, new TestMessage("cancelled"), deliverAt,
            new ScheduleOptions { Key = "notification-1" });
        await Cluster.StepDownStreamLeaderAsync(s.Stream);

        Assert.IsTrue(await s.Publisher.CancelScheduleAsync(subject, "notification-1"));
        Assert.AreEqual(0L, await s.HolderCountAsync());

        await ScheduleRaces.DelayUntil(deliverAt.AddSeconds(2));
        Assert.HasCount(0, s.Collector.Received);
    }

    [TestMethod]
    public async Task Replace_RacingTheFire_OnR3_EndsInADocumentedOutcome()
    {
        await using var s = await ScheduleStream.CreateAsync(Cluster);

        var outcomes = await ScheduleRaces.ReplaceAsync(s.Publisher, s.Collector, s.Root);

        TestContext.WriteLine(string.Join(Environment.NewLine, outcomes));
    }

    [TestMethod]
    public async Task Cancel_RacingTheFire_OnR3_EndsInADocumentedOutcome()
    {
        await using var s = await ScheduleStream.CreateAsync(Cluster);

        var outcomes = await ScheduleRaces.CancelAsync(s.Publisher, s.Collector, s.Root);

        TestContext.WriteLine(string.Join(Environment.NewLine, outcomes));
    }

    [TestMethod]
    public async Task FullClusterRestart_WhileSchedulesArePending_AllFireAfterTheRestart()
    {
        await using var s = await ScheduleStream.CreateAsync(Cluster);
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(3);

        var schedules = await s.ScheduleManyAsync(10, i => deliverAt.AddMilliseconds(i * 100),
            i => i % 2 == 0 ? null : $"k{i}");
        foreach (var node in Cluster.Nodes)
        {
            await Cluster.StopServerAsync(node.Name);
        }

        await ScheduleRaces.DelayUntil(schedules.Max(x => x.Result.DeliverAt).AddSeconds(1));
        Assert.HasCount(0, s.Collector.Received, "Nothing can fire while every node is down");

        var restartedAt = DateTimeOffset.UtcNow;
        foreach (var node in Cluster.Nodes)
        {
            await Cluster.RestartServerAsync(node.Name);
        }

        await Cluster.WaitForStreamAsync(s.Stream);

        var fires = await s.AssertFiresAsync(schedules, exactlyOnce: true, TestContext);
        Assert.IsTrue(fires.Values.All(f => f[0].Stored >= restartedAt), "Every schedule fired after the restart");
    }

    [TestMethod]
    public async Task Consumer_KeepsProcessing_AcrossConsumerLeaderStepdowns()
    {
        var probe = new Probe<MessageContext<OrderEvent>>();
        var (stream, root) = ConsumerBindings.UniqueStream("R3_CONSUMER");
        stream.Replicas = 3;

        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe],
            o => o.NatsUrl = Cluster.Url,
            new NatsConsumerAttribute { Subject = ".orders", MaxConcurrency = 4, AckWaitSeconds = 5 });
        await Cluster.WaitForStreamAsync(stream.Name);

        const int count = 300;
        var publishing = Task.Run(async () =>
        {
            for (var i = 0; i < count; i++)
            {
                await scenario.Publisher.PublishAsync($"{root}.orders", new OrderEvent($"ORD-{i}", i));
                await Task.Delay(20);
            }
        });

        var leaders = new List<string>();
        for (var i = 0; i < 3; i++)
        {
            await Task.Delay(TimeSpan.FromSeconds(1.5));
            leaders.Add(await Cluster.StepDownConsumerLeaderAsync(stream.Name, scenario.Durable));
        }

        await publishing;
        await Eventually.TrueAsync(
            () => Task.FromResult(probe.Calls.Select(c => c.Arg.Message.OrderId).Distinct().Count() == count),
            TimeSpan.FromSeconds(30), "Every message is handled");
        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()) is { NumAckPending: 0, NumPending: 0 },
            TimeSpan.FromSeconds(15), "Every message is acked");

        Assert.AreEqual(ComponentState.Running, scenario.Host.Status.Get(scenario.Component)!.State);
        TestContext.WriteLine($"consumer leaders {string.Join(", ", leaders)}; {probe.Count} handler calls for {count} messages");
    }

    [TestMethod]
    public async Task Consumer_WithoutQuorum_IsReportedStalled_AndRecoversWhenQuorumReturns()
    {
        var logs = new LogCapture();
        var probe = new Probe<MessageContext<OrderEvent>>();
        var (stream, root) = ConsumerBindings.UniqueStream("R3_STALL");
        stream.Replicas = 3;

        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe, logs],
            o => o.NatsUrl = Cluster.Url,
            new NatsConsumerAttribute { Subject = ".orders", AckWaitSeconds = 5 });
        await Cluster.WaitForStreamAsync(stream.Name);
        await scenario.Publisher.PublishAsync($"{root}.orders", new OrderEvent("ORD-1", 1m));
        await probe.WaitForCallsAsync(1);

        // Two of three nodes gone: the stream and its durable have no quorum, and the client stays connected to n2.
        await Cluster.KillServerAsync("n0");
        await Cluster.KillServerAsync("n1");

        var stalled = await scenario.Host.WaitForStateAsync(scenario.Component, ComponentState.Retrying,
            TimeSpan.FromSeconds(45));
        StringAssert.Contains(stalled.Error, "Stalled");
        Assert.AreEqual(HealthStatus.Degraded,
            (await scenario.Host.CheckHealthAsync()).Status);
        var warning = await logs.WaitForAsync(e => e.Level == LogLevel.Warning && e.Message.Contains("is stalled"),
            TimeSpan.FromSeconds(5), "about the stalled consumer");
        StringAssert.Contains(warning.Message, scenario.Durable);
        TestContext.WriteLine(warning.Message);

        await Cluster.RestartServerAsync("n0");
        await Cluster.RestartServerAsync("n1");
        await Cluster.WaitForStreamAsync(stream.Name);

        await scenario.Host.WaitForStateAsync(scenario.Component, ComponentState.Running, TimeSpan.FromSeconds(45));
        await scenario.Publisher.PublishAsync($"{root}.orders", new OrderEvent("ORD-2", 2m));
        await probe.WaitForCallsAsync(2, TimeSpan.FromSeconds(20));
    }

    [TestMethod]
    public async Task PublishWithAFixedMessageId_DuringAStreamLeaderStepdown_CompletesOrThrowsWithinItsTimeout_AndARetryStoresOneMessage()
    {
        var stream = TestNames.Unique("R3_PUBLISH");
        var root = TestNames.Unique("r3publish");
        await Cluster.CreateStreamAsync(new StreamConfig(stream, [$"{root}.>"]) { NumReplicas = 3 });
        await using var connection = Cluster.CreateConnection();
        var publisher = CreatePublisher(connection, new NatsJSContext(connection));
        var timeout = TimeSpan.FromSeconds(1);
        var tolerance = TimeSpan.FromMilliseconds(250);

        var attempts = new List<(string Id, string Subject, TimeSpan Elapsed, NatsPublishException? Error)>();
        for (var run = 0; run < 5; run++)
        {
            var publishes = Enumerable.Range(0, 20).Select(async i =>
            {
                await Task.Delay(i * 15);
                var id = $"run{run}-msg{i}";
                var subject = $"{root}.r{run}.m{i}";
                var stopwatch = Stopwatch.StartNew();
                try
                {
                    await publisher.PublishAsync(subject, new TestMessage(id),
                        new PublishOptions { MessageId = id, Timeout = timeout });
                    return (id, subject, stopwatch.Elapsed, (NatsPublishException?)null);
                }
                catch (NatsPublishException e)
                {
                    return (id, subject, stopwatch.Elapsed, e);
                }
            }).ToList();

            await Task.Delay(50);
            await Cluster.StepDownStreamLeaderAsync(stream);
            attempts.AddRange(await Task.WhenAll(publishes));
        }

        foreach (var attempt in attempts)
        {
            Assert.IsLessThanOrEqualTo(timeout + tolerance, attempt.Elapsed,
                $"{attempt.Id} took {attempt.Elapsed.TotalMilliseconds:F0} ms ({attempt.Error?.Failure})");
        }

        // The caller's retry: same id, a fresh budget. A message the first call stored is a duplicate.
        foreach (var attempt in attempts)
        {
            var retry = await publisher.PublishAsync(attempt.Subject, new TestMessage(attempt.Id),
                new PublishOptions { MessageId = attempt.Id, Timeout = TimeSpan.FromSeconds(10) });
            if (attempt.Error == null)
            {
                Assert.IsTrue(retry.Duplicate, $"{attempt.Id} was acked, so the retry is a duplicate");
            }
        }

        var info = await Cluster.JetStream.GetStreamAsync(stream, new StreamInfoRequest { SubjectsFilter = $"{root}.>" });
        var stored = info.Info.State.Subjects ?? new Dictionary<string, long>();
        foreach (var attempt in attempts)
        {
            Assert.AreEqual(1L, stored.TryGetValue(attempt.Subject, out var n) ? n : 0, $"{attempt.Id} is stored once");
        }

        Assert.AreEqual((ulong)attempts.Count, (ulong)info.Info.State.Messages);
        var failures = attempts.Where(a => a.Error != null)
            .GroupBy(a => a.Error!.Failure)
            .Select(g => $"{g.Key} x{g.Count()} (outcome unknown: {g.First().Error!.OutcomeUnknown})")
            .DefaultIfEmpty("none");
        TestContext.WriteLine($"{attempts.Count} publishes, max {attempts.Max(a => a.Elapsed).TotalMilliseconds:F0} ms; " +
                              $"failures: {string.Join(", ", failures)}");
    }

    [TestMethod]
    public async Task DeadLetterStreamAtR3_WorksAcrossStreamLeaderStepdowns()
    {
        var probe = new Probe<MessageContext<OrderEvent>> { Behaviour = (_, _, _) => throw new PoisonMessageException("bad") };
        var (stream, root) = ConsumerBindings.UniqueStream("R3_DLQ");
        stream.Replicas = 3;

        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe],
            o =>
            {
                o.NatsUrl = Cluster.Url;
                o.DeadLetterStream!.Replicas = 3;
            },
            new NatsConsumerAttribute { Subject = ".orders", MaxConcurrency = 4, AckWaitSeconds = 5 });
        var deadLetters = scenario.Host.DeadLetterStream;
        await Cluster.WaitForStreamAsync(deadLetters);

        const int count = 40;
        var publishing = Task.Run(async () =>
        {
            for (var i = 0; i < count; i++)
            {
                await scenario.Publisher.PublishAsync($"{root}.orders", new OrderEvent($"ORD-{i}", i));
                await Task.Delay(50);
            }
        });

        await Task.Delay(400);
        await Cluster.StepDownStreamLeaderAsync(deadLetters);
        await Task.Delay(400);
        await Cluster.StepDownStreamLeaderAsync(stream.Name);
        await Task.Delay(400);
        await Cluster.StepDownStreamLeaderAsync(deadLetters);
        await publishing;

        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()) is { NumAckPending: 0, NumPending: 0 },
            TimeSpan.FromSeconds(30), "Every message is dead-lettered and acked");
        await using var collector = await scenario.Host.CollectDeadLettersAsync();
        var entries = await collector.WaitForAsync(count, TimeSpan.FromSeconds(10));
        await Task.Delay(1000);

        var info = await Cluster.JetStream.GetStreamAsync(deadLetters);
        Assert.AreEqual((ulong)count, (ulong)info.Info.State.Messages, "One entry per message");
        Assert.AreEqual(count, entries.Select(e => e.Header("X-Stream-Sequence")).Distinct().Count());
        Assert.IsTrue(entries.All(e => e.Header("X-Consumer") == scenario.Durable));
        TestContext.WriteLine($"{probe.Count} handler calls for {count} messages");
    }

    private static async Task<string> WaitForLeaderOtherThanAsync(string stream, string previous)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                if (await Cluster.LeaderOfAsync(stream) is { } leader && leader != previous)
                {
                    return leader;
                }
            }
            catch (NatsException)
            {
            }

            await Task.Delay(100);
        }

        throw new TimeoutException($"No leader for {stream} after {previous} was killed");
    }

    private sealed record Scheduled(string Payload, ScheduleResult Result);

    /// <summary>
    /// An R3 stream from <see cref="StreamDefinition.Scheduled"/> on the cluster, with a publisher and a collector on
    /// "{root}.>".
    /// </summary>
    private sealed class ScheduleStream : IAsyncDisposable
    {
        // NATS.Net 2.7.3 truncates the ack timestamp to whole milliseconds, so a fire can look up to 1 ms early.
        // The server itself only fires once now >= @at (thw.go: expires <= now).
        private static readonly TimeSpan EarlyTolerance = TimeSpan.FromMilliseconds(1);

        private ScheduleStream(NatsConnection connection, string stream, string root, NatsPublisher publisher,
            StreamCollector collector)
        {
            Connection = connection;
            Stream = stream;
            Root = root;
            Publisher = publisher;
            Collector = collector;
        }

        public NatsConnection Connection { get; }

        public string Stream { get; }

        public string Root { get; }

        public NatsPublisher Publisher { get; }

        public StreamCollector Collector { get; }

        public static async Task<ScheduleStream> CreateAsync(NatsClusterFixture cluster)
        {
            var stream = TestNames.Unique("TIMERS");
            var root = TestNames.Unique("timers");
            var definition = StreamDefinition.Scheduled(stream, $"{root}.>");
            definition.Replicas = 3;
            await cluster.CreateStreamAsync(definition.ToStreamConfig());

            var connection = cluster.CreateConnection();
            var js = new NatsJSContext(connection);
            var publisher = CreatePublisher(connection, js, o =>
            {
                o.StreamDefinitions = [definition];
                o.PublishTimeout = TimeSpan.FromSeconds(10);
            });
            var collector = await StreamCollector.StartAsync(js, stream, $"{root}.>");
            return new ScheduleStream(connection, stream, root, publisher, collector);
        }

        public async Task<Scheduled> ScheduleAsync(string name, DateTimeOffset deliverAt, string? key)
        {
            var payload = $"{name}-{Guid.NewGuid():N}";
            var result = await Publisher.ScheduleAsync($"{Root}.{name}", new TestMessage(payload), deliverAt,
                new ScheduleOptions { Key = key });
            return new Scheduled(payload, result);
        }

        public async Task<IReadOnlyList<Scheduled>> ScheduleManyAsync(int count, Func<int, DateTimeOffset> deliverAt,
            Func<int, string?> key) =>
            await Task.WhenAll(Enumerable.Range(0, count).Select(i => ScheduleAsync($"t{i}", deliverAt(i), key(i))));

        public async Task<long> HolderCountAsync()
        {
            var info = await new NatsJSContext(Connection).GetStreamAsync(Stream,
                new StreamInfoRequest { SubjectsFilter = "_sched.>" });
            return info.Info.State.Subjects?.Values.Sum(v => (long)v) ?? 0;
        }

        /// <summary>
        /// Waits until every schedule has fired, then checks: each fired at least once (exactly once when
        /// <paramref name="exactlyOnce"/>), never before its time, and every fire carries the ScheduleId of the
        /// schedule its payload came from. Returns the fires (distinct stream sequences) per schedule id.
        /// </summary>
        public async Task<IReadOnlyDictionary<string, IReadOnlyList<ReceivedMessage>>> AssertFiresAsync(
            IReadOnlyCollection<Scheduled> schedules, bool exactlyOnce, TestContext context)
        {
            var latest = schedules.Max(s => s.Result.DeliverAt);
            var deadline = (DateTimeOffset.UtcNow > latest ? DateTimeOffset.UtcNow : latest) + TimeSpan.FromSeconds(30);
            while (DateTimeOffset.UtcNow < deadline && Fires().Count < schedules.Count)
            {
                await Task.Delay(100);
            }

            // Late duplicates (a second fire after a leader change) need a moment to show up.
            await Task.Delay(TimeSpan.FromSeconds(2));
            var fires = Fires();

            var byPayload = schedules.ToDictionary(s => s.Payload);
            foreach (var received in Collector.Received)
            {
                var payload = received.Json<TestMessage>().Content;
                Assert.IsTrue(byPayload.TryGetValue(payload, out var scheduled), $"Unexpected payload {payload}");
                Assert.AreEqual(scheduled.Result.ScheduleId, received.Header("X-Schedule-Id"),
                    $"{payload} carries another schedule's id");
            }

            var missing = schedules.Where(s => !fires.ContainsKey(s.Result.ScheduleId)).ToList();
            if (missing.Count > 0)
            {
                var described = await Task.WhenAll(missing.Take(5).Select(DescribeMissingAsync));
                Assert.Fail($"{missing.Count} of {schedules.Count} never reached the collector: " +
                            string.Join("; ", described));
            }

            var duplicates = new List<string>();
            foreach (var scheduled in schedules)
            {
                var stored = fires[scheduled.Result.ScheduleId];
                foreach (var fire in stored)
                {
                    Assert.IsTrue(fire.Stored >= scheduled.Result.DeliverAt - EarlyTolerance,
                        $"{scheduled.Payload} fired {(scheduled.Result.DeliverAt - fire.Stored).TotalMilliseconds:F1} ms early");
                }

                if (stored.Count > 1)
                {
                    duplicates.Add($"{scheduled.Payload}: {stored.Count} fires, sequences " +
                                   string.Join("/", stored.Select(f => f.Sequence)) + ", late by " +
                                   string.Join("/", stored.Select(f => $"{(f.Stored - scheduled.Result.DeliverAt).TotalMilliseconds:F0} ms")));
                }
            }

            var lateness = fires.Values.Select(f => (f[0].Stored - schedules.Single(s => s.Result.ScheduleId ==
                f[0].Header("X-Schedule-Id")).Result.DeliverAt).TotalMilliseconds).ToList();
            context.WriteLine($"{schedules.Count} schedules, {fires.Values.Sum(f => f.Count)} fires, " +
                              $"first fire late by {lateness.Min():F0}..{lateness.Max():F0} ms");
            context.WriteLine(duplicates.Count == 0 ? "no duplicate fires" : string.Join(Environment.NewLine, duplicates));

            if (exactlyOnce)
            {
                Assert.HasCount(0, duplicates, string.Join("; ", duplicates));
            }

            return fires;
        }

        public async ValueTask DisposeAsync()
        {
            await Collector.DisposeAsync();
            await Connection.DisposeAsync();
        }

        // Tells a lost fire from a late delivery: a fire the collector missed is still in the stream (not acked), and
        // a schedule that has not fired still has its holder
        private async Task<string> DescribeMissingAsync(Scheduled scheduled)
        {
            var subject = $"{Root}.{scheduled.Payload[..scheduled.Payload.IndexOf('-')]}";
            var info = await new NatsJSContext(Connection).GetStreamAsync(Stream,
                new StreamInfoRequest { SubjectsFilter = subject });
            var onSubject = info.Info.State.Subjects?.Values.Sum(v => (long)v) ?? 0;
            return $"{scheduled.Payload} ({scheduled.Result.ScheduleId}): {onSubject} message(s) still on {subject}, " +
                   $"{await HolderCountAsync()} schedule holder(s) left in the stream";
        }

        private Dictionary<string, IReadOnlyList<ReceivedMessage>> Fires() =>
            Collector.Received
                .Where(r => r.Header("X-Schedule-Id") != null)
                .DistinctBy(r => r.Sequence)
                .GroupBy(r => r.Header("X-Schedule-Id")!)
                .ToDictionary(g => g.Key, g => (IReadOnlyList<ReceivedMessage>)g.OrderBy(r => r.Sequence).ToList());
    }
}
