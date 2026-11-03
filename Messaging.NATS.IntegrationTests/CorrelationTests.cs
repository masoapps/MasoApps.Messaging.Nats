using Messaging.NATS.IntegrationTests.Handlers;
using Messaging.NATS.Testing;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class CorrelationTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(15);

    [TestMethod]
    public async Task PublishFromHandler_CarriesTheHandledMessagesCorrelationId_AndItsIdAsCausationId()
    {
        IPublisher? publisher = null;
        string? root = null;
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (context, _, token) => publisher!.PublishAsync($"{root}.followup",
                new OrderEvent(context.Message.OrderId, 0m), token)
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        (publisher, root) = (scenario.Publisher, scenario.Root);
        await using var followUps = await StreamCollector.StartAsync(scenario.Host.JetStream, scenario.Stream.Name,
            $"{root}.followup");

        await publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m),
            new PublishOptions { MessageId = "in-1", CorrelationId = "corr-1" });
        await publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-2", 1m),
            new PublishOptions { MessageId = "in-2", CorrelationId = "corr-2" });

        var received = await followUps.WaitForAsync(2, Wait);
        var first = received.Single(m => m.Json<OrderEvent>().OrderId == "ORD-1");
        var second = received.Single(m => m.Json<OrderEvent>().OrderId == "ORD-2");
        Assert.AreEqual("corr-1", first.Header("X-Correlation-Id"));
        Assert.AreEqual("in-1", first.Header("X-Causation-Id"));
        Assert.AreEqual("corr-2", second.Header("X-Correlation-Id"));
        Assert.AreEqual("in-2", second.Header("X-Causation-Id"));
    }

    [TestMethod]
    public async Task PublishFromHandler_OfAMessageWithoutCorrelationId_UsesItsMessageIdAsCorrelationId()
    {
        IPublisher? publisher = null;
        string? root = null;
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (context, _, token) => publisher!.PublishAsync($"{root}.followup",
                new OrderEvent(context.Message.OrderId, 0m), token)
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        (publisher, root) = (scenario.Publisher, scenario.Root);
        await using var followUps = await StreamCollector.StartAsync(scenario.Host.JetStream, scenario.Stream.Name,
            $"{root}.followup");

        await publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m),
            new PublishOptions { MessageId = "root-1" });

        var followUp = (await followUps.WaitForAsync(1, Wait)).Single();
        Assert.AreEqual("root-1", followUp.Header("X-Correlation-Id"));
        Assert.AreEqual("root-1", followUp.Header("X-Causation-Id"));

        var handled = (await probe.WaitForCallsAsync(1)).Single().Arg;
        Assert.IsNull(handled.CorrelationId, "A publish outside a handler gets no correlation id");
        Assert.IsNull(handled.CausationId, "A publish outside a handler gets no causation id");
    }

    [TestMethod]
    public async Task PublishFromHandler_OptionsWin_AndAForwardedCausationIdIsDropped()
    {
        IPublisher? publisher = null;
        string? root = null;
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = async (context, _, token) =>
            {
                await publisher!.PublishAsync($"{root}.followup.explicit", context.Message,
                    new PublishOptions { CorrelationId = "mine", CausationId = "my-cause" }, token);
                await publisher.PublishAsync($"{root}.followup.forwarded", context.Message,
                    new PublishOptions
                    {
                        Headers = new NatsHeaders { ["X-Correlation-Id"] = "forwarded", ["X-Causation-Id"] = "stale" }
                    }, token);
            }
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        (publisher, root) = (scenario.Publisher, scenario.Root);
        await using var followUps = await StreamCollector.StartAsync(scenario.Host.JetStream, scenario.Stream.Name,
            $"{root}.followup.>");

        await publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m),
            new PublishOptions { MessageId = "in-1", CorrelationId = "corr-1" });

        var received = await followUps.WaitForAsync(2, Wait);
        var explicitIds = received.Single(m => m.Subject.EndsWith(".explicit"));
        var forwarded = received.Single(m => m.Subject.EndsWith(".forwarded"));
        Assert.AreEqual("mine", explicitIds.Header("X-Correlation-Id"));
        Assert.AreEqual("my-cause", explicitIds.Header("X-Causation-Id"));
        Assert.AreEqual("forwarded", forwarded.Header("X-Correlation-Id"));
        Assert.AreEqual("in-1", forwarded.Header("X-Causation-Id"));
    }

    [TestMethod]
    public async Task ScheduleFromHandler_FiresWithTheHandledMessagesCorrelationAndCausation()
    {
        var (stream, root) = (TestNames.Unique("CORRSCHED"), TestNames.Unique("corrsched"));
        IPublisher? publisher = null;
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (context, _, token) => publisher!.ScheduleAsync($"{root}.followup", context.Message,
                DateTimeOffset.UtcNow, null, token)
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(
            StreamDefinition.Scheduled(stream, $"{root}.>"), root, [probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        publisher = scenario.Publisher;
        await using var fired = await StreamCollector.StartAsync(scenario.Host.JetStream, stream, $"{root}.followup");

        await publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m),
            new PublishOptions { MessageId = "in-1", CorrelationId = "corr-1" });

        var followUp = (await fired.WaitForAsync(1, Wait)).Single();
        Assert.AreEqual("corr-1", followUp.Header("X-Correlation-Id"));
        Assert.AreEqual("in-1", followUp.Header("X-Causation-Id"));
    }

    [TestMethod]
    public async Task PublishFromLegacyConsumer_CarriesCorrelationAndCausation()
    {
        IPublisher? publisher = null;
        string? root = null;
        var probe = new Probe<INatsJSMsg<OrderEvent>>
        {
            Behaviour = (msg, _, token) => publisher!.PublishAsync($"{root}.followup", msg.Data!, token)
        };
        await using var scenario = await ConsumerScenario.StartAsync<LegacyOrderConsumer>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        (publisher, root) = (scenario.Publisher, scenario.Root);
        await using var followUps = await StreamCollector.StartAsync(scenario.Host.JetStream, scenario.Stream.Name,
            $"{root}.followup");

        await publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m),
            new PublishOptions { MessageId = "in-1", CorrelationId = "corr-1" });

        var followUp = (await followUps.WaitForAsync(1, Wait)).Single();
        Assert.AreEqual("corr-1", followUp.Header("X-Correlation-Id"));
        Assert.AreEqual("in-1", followUp.Header("X-Causation-Id"));
    }

    [TestMethod]
    public void MessageContext_ReadsTheCausationIdHeader()
    {
        var context = TestMessageContext.Create(new OrderEvent("ORD-1", 1m),
            headers: new NatsHeaders { ["X-Causation-Id"] = "parent-1" });

        Assert.AreEqual("parent-1", context.CausationId);
    }
}
