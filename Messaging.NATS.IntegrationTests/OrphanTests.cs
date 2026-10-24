using Messaging.NATS.IntegrationTests.Handlers;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class OrphanTests
{
    private const string Gauge = "messaging.nats.consumer.orphans";

    [TestMethod]
    public async Task OwnedOrphans_AreReportedInTheLogAndGauge_FourXOnesAsPossiblyUnused_AndBothAreKept()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("ORPHAN", StreamConfigRetention.Interest);
        var service = TestNames.Unique("svc");
        var queueGroup = TestNames.Unique("qg");
        var owned = $"{service}_retired";
        var fourX = $"{queueGroup}_RemovedConsumer";
        await using (var setup = TestHost.CreateConnection())
        {
            var js = new NatsJSContext(setup);
            await js.CreateStreamAsync(stream.ToStreamConfig());
            await CreateConsumerAsync(js, stream.Name, owned, $"{root}.old",
                new Dictionary<string, string> { ["messaging-nats.owner"] = service });
            await CreateConsumerAsync(js, stream.Name, fourX, $"{root}.created", null);
            (await js.PublishAsync($"{root}.created", "{}"u8.ToArray())).EnsureSuccess();
        }

        using var metrics = new MetricCapture();
        var logs = new LogCapture();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>(), logs], o => o.ServiceName = service,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup });

        var ownedLog = await logs.WaitForAsync(e => e.Level == LogLevel.Warning && e.Message.Contains(owned),
            TimeSpan.FromSeconds(10), $"about {owned}");
        var fourXLog = await logs.WaitForAsync(e => e.Message.Contains(fourX), TimeSpan.FromSeconds(10), $"about {fourX}");
        StringAssert.Contains(ownedLog.Message, "Interest stream");
        StringAssert.Contains(ownedLog.Message, $"nats consumer rm {stream.Name} {owned}");

        // Another service with the same queue group may own a 4.x durable, so it gets no delete command.
        Assert.AreEqual(LogLevel.Information, fourXLog.Level);
        StringAssert.Contains(fourXLog.Message, "may be unused");
        StringAssert.Contains(fourXLog.Message, "1 pending");
        Assert.DoesNotContain("nats consumer rm", fourXLog.Message);
        Assert.IsFalse(logs.Entries.Any(e => e.Message.Contains(scenario.Durable) &&
                                             (e.Message.Contains("no handler uses") || e.Message.Contains("may be unused"))),
            "The current durable is not an orphan");

        Assert.AreEqual(1d, await GaugeAsync(metrics, stream.Name));

        await using var check = TestHost.CreateConnection();
        var verify = new NatsJSContext(check);
        Assert.AreEqual(owned, (await verify.GetConsumerAsync(stream.Name, owned)).Info.Name, "Never deleted");
        Assert.AreEqual(fourX, (await verify.GetConsumerAsync(stream.Name, fourX)).Info.Name, "Never deleted");
    }

    [TestMethod]
    public async Task ConsumersOfOtherOwners_AreIgnored()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("ORPHAN");
        var service = TestNames.Unique("svc");
        var queueGroup = TestNames.Unique("qg");
        var otherOwner = $"{queueGroup}_OtherServicesConsumer";
        var foreign = TestNames.Unique("foreign");
        await using (var setup = TestHost.CreateConnection())
        {
            var js = new NatsJSContext(setup);
            await js.CreateStreamAsync(stream.ToStreamConfig());
            await CreateConsumerAsync(js, stream.Name, otherOwner, $"{root}.old",
                new Dictionary<string, string> { ["messaging-nats.owner"] = "another-service" });
            await CreateConsumerAsync(js, stream.Name, foreign, $"{root}.old", null);
        }

        using var metrics = new MetricCapture();
        var logs = new LogCapture();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>(), logs], o => o.ServiceName = service,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup });

        Assert.AreEqual(0d, await GaugeAsync(metrics, stream.Name), "The report ran and found nothing");
        Assert.IsFalse(logs.Entries.Any(e => e.Message.Contains(otherOwner) || e.Message.Contains(foreign)),
            string.Join(Environment.NewLine, logs.Entries.Select(e => e.Message)));
    }

    [TestMethod]
    public async Task ServicesSharingAQueueGroup_WithoutServiceName_DoNotReportEachOthersLiveDurablesAsOrphans()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("ORPHAN", StreamConfigRetention.Interest);
        var queueGroup = TestNames.Unique("data-transformation");

        await using var first = await ConsumerScenario.StartAsync<OrderHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>()], null,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup });

        using var metrics = new MetricCapture();
        var logs = new LogCapture();
        await using var second = await ConsumerScenario.StartAsync<LegacyOrderConsumer>(stream, root,
            [new Probe<INatsJSMsg<OrderEvent>>(), logs], null,
            new NatsConsumerAttribute { Subject = ".paid", QueueGroup = queueGroup });

        var report = await logs.WaitForAsync(e => e.Message.Contains(first.Durable), TimeSpan.FromSeconds(10),
            $"about {first.Durable}");
        Assert.AreEqual(LogLevel.Information, report.Level);
        StringAssert.Contains(report.Message, "Another service with the same queue group may own it");
        Assert.DoesNotContain("nats consumer rm", report.Message);
        Assert.IsFalse(logs.Entries.Any(e => e.Level >= LogLevel.Warning && e.Message.Contains(first.Durable)),
            string.Join(Environment.NewLine, logs.Entries.Select(e => e.Message)));

        var gauges = await GaugesAsync(metrics, stream.Name, 2);
        CollectionAssert.AreEqual(new[] { 0d, 0d }, gauges.ToArray(),
            "Both reports ran, and a durable that may belong to another service is not counted");
    }

    private static async Task CreateConsumerAsync(NatsJSContext js, string stream, string name, string filter,
        IDictionary<string, string>? metadata) =>
        await js.CreateOrUpdateConsumerAsync(stream, new ConsumerConfig(name)
        {
            FilterSubject = filter,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            Metadata = metadata
        });

    /// <summary>
    /// The orphans gauge for <paramref name="stream"/>, once the report has set it.
    /// </summary>
    private static async Task<double> GaugeAsync(MetricCapture metrics, string stream)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            metrics.Observe();
            var measured = metrics.Of(Gauge, "messaging.nats.stream", stream);
            if (measured.Count > 0)
            {
                return measured[^1].Value;
            }

            await Task.Delay(100);
        }

        Assert.Fail($"The orphan report for {stream} did not run");
        return -1;
    }

    /// <summary>
    /// The orphans gauges for <paramref name="stream"/> from one observation, once <paramref name="hosts"/> reports
    /// have set them (each host reports its own value).
    /// </summary>
    private static async Task<IReadOnlyList<double>> GaugesAsync(MetricCapture metrics, string stream, int hosts)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            var before = metrics.Of(Gauge, "messaging.nats.stream", stream).Count;
            metrics.Observe();
            var observed = metrics.Of(Gauge, "messaging.nats.stream", stream).Skip(before).ToList();
            if (observed.Count >= hosts)
            {
                return observed.Select(m => m.Value).ToList();
            }

            await Task.Delay(100);
        }

        Assert.Fail($"{hosts} orphan reports for {stream} did not run");
        return [];
    }
}
