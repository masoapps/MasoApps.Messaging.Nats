using System.Globalization;
using System.Text.Json;
using Messaging.NATS.IntegrationTests.Handlers;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class LegacyDeferralTests
{
    [TestMethod]
    [DataRow(StreamConfigRetention.Workqueue)]
    [DataRow(StreamConfigRetention.Limits)]
    public async Task FutureMsgDeferredUntil_IsHandledOnlyAfterThatTime(StreamConfigRetention retention)
    {
        var (stream, root) = ConsumerBindings.UniqueStream("DEFERRED", retention);
        var probe = new Probe<INatsJSMsg<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<LegacyOrderConsumer>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".created" });

        // 4.x wrote DateTime.UtcNow + deferredBy with ToString(CultureInfo.InvariantCulture): no zone, whole seconds.
        // 5 s ahead, so the "not before" window survives a slow publish on a loaded agent.
        var header = DateTime.UtcNow.AddSeconds(5).ToString(CultureInfo.InvariantCulture);
        var until = DateTime.SpecifyKind(DateTime.Parse(header, CultureInfo.InvariantCulture), DateTimeKind.Utc);
        await scenario.PublishRawAsync($"{scenario.Root}.created",
            JsonSerializer.SerializeToUtf8Bytes(new OrderEvent("ORD-LATER", 1m)),
            new NatsHeaders { ["Msg-DeferredUntil"] = header });

        await ScheduleRaces.DelayUntil(new DateTimeOffset(until).AddMilliseconds(-300));
        if (DateTime.UtcNow < until)
        {
            Assert.AreEqual(0, probe.Count, "Not handled before Msg-DeferredUntil");
        }

        var call = (await probe.WaitForCallsAsync(1, TimeSpan.FromSeconds(10))).Single();
        Assert.IsGreaterThanOrEqualTo(until.AddMilliseconds(-50), call.At);
        Assert.AreEqual("ORD-LATER", call.Arg.Data!.OrderId);
        Assert.AreEqual(2UL, call.Arg.Metadata!.Value.NumDelivered, "The first delivery was NAKed with the delay");
    }

    [TestMethod]
    public async Task PastOrUnreadableMsgDeferredUntil_IsHandledAtOnce()
    {
        var probe = new Probe<INatsJSMsg<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<LegacyOrderConsumer>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", MaxConcurrency = 2 });

        var past = DateTime.UtcNow.AddMinutes(-1).ToString(CultureInfo.InvariantCulture);
        await scenario.PublishRawAsync($"{scenario.Root}.created",
            JsonSerializer.SerializeToUtf8Bytes(new OrderEvent("PAST", 1m)), new NatsHeaders { ["Msg-DeferredUntil"] = past });
        await scenario.PublishRawAsync($"{scenario.Root}.created",
            JsonSerializer.SerializeToUtf8Bytes(new OrderEvent("GARBAGE", 1m)), new NatsHeaders { ["Msg-DeferredUntil"] = "soon" });

        var calls = await probe.WaitForCallsAsync(2, TimeSpan.FromSeconds(3));
        Assert.IsTrue(calls.All(c => c.Arg.Metadata!.Value.NumDelivered == 1));
    }

    [TestMethod]
    public void LegacyDeferral_ParsesTheFourXFormatAsUtc_InAnyCulture()
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("ar-SA");
            var header = DateTime.UtcNow.AddMinutes(10).ToString(CultureInfo.InvariantCulture);

            var delay = ConsumerWorker.LegacyDeferral(new NatsHeaders { ["Msg-DeferredUntil"] = header });

            Assert.IsNotNull(delay);
            Assert.IsTrue(delay > TimeSpan.FromMinutes(9) && delay <= TimeSpan.FromMinutes(10), delay.ToString());
            Assert.IsNull(ConsumerWorker.LegacyDeferral(null));
            Assert.IsNull(ConsumerWorker.LegacyDeferral(new NatsHeaders()));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }
}
