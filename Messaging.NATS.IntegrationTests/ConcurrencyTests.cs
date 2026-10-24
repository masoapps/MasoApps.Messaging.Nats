using Messaging.NATS.IntegrationTests.Handlers;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class ConcurrencyTests
{
    [TestMethod]
    public async Task MaxConcurrency_IsThePeakNumberOfHandlersRunningAtOnce()
    {
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, _, ct) => Task.Delay(400, ct)
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", MaxConcurrency = 4 });

        for (var i = 0; i < 16; i++)
        {
            await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent($"ORD-{i}", i));
        }

        await probe.WaitForCallsAsync(16);
        Assert.AreEqual(4, probe.Peak);
    }

    [TestMethod]
    public async Task FiftyMessages_WithConcurrencyOne_AreEachHandledExactlyOnce()
    {
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, _, ct) => Task.Delay(200, ct)
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", MaxConcurrency = 1, AckWaitSeconds = 2 });

        for (var i = 0; i < 50; i++)
        {
            await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent($"ORD-{i}", i));
        }

        var calls = await probe.WaitForCallsAsync(50, TimeSpan.FromSeconds(30));
        await Task.Delay(2500);

        Assert.AreEqual(50, probe.Count);
        Assert.AreEqual(50, calls.Select(c => c.Arg.Message.OrderId).Distinct().Count());
        Assert.IsTrue(calls.All(c => c.Arg.DeliveryCount == 1));
        Assert.AreEqual(1, probe.Peak);
        CollectionAssert.AreEqual(Enumerable.Range(0, 50).Select(i => $"ORD-{i}").ToArray(),
            calls.Select(c => c.Arg.Message.OrderId).ToArray(), "Concurrency 1 keeps the order");
    }

    [TestMethod]
    public async Task Heartbeat_KeepsAHandlerLongerThanAckWait_FromBeingRedelivered()
    {
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, _, ct) => Task.Delay(TimeSpan.FromSeconds(6), ct)
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", AckWaitSeconds = 2, MaxConcurrency = 2 });

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-SLOW", 1m));
        await probe.WaitForCallsAsync(1);

        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()).NumAckPending == 0,
            TimeSpan.FromSeconds(10), "The slow handler's message is acked");
        Assert.AreEqual(1, probe.Count, "A free slot was available, so a redelivery would have run");
        Assert.AreEqual(0, (await scenario.InfoAsync()).NumRedelivered);
    }

    [TestMethod]
    public async Task HandlerLongerThanAckWait_DoesNotAgeTheNextMessage_WithConcurrencyOne()
    {
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, _, ct) => Task.Delay(TimeSpan.FromSeconds(3), ct)
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", AckWaitSeconds = 1, MaxConcurrency = 1 });

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));
        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-2", 2m));

        var calls = await probe.WaitForCallsAsync(2, TimeSpan.FromSeconds(15));

        Assert.AreEqual("ORD-2", calls[1].Arg.Message.OrderId);
        Assert.AreEqual(1, calls[1].Arg.DeliveryCount, "The second message was not pulled while the first ran");
        Assert.IsGreaterThanOrEqualTo(TimeSpan.FromSeconds(2.9), calls[1].At - calls[0].At);
        await Task.Delay(1500);
        Assert.AreEqual(2, probe.Count);
    }
}
