using Messaging.NATS.IntegrationTests.Handlers;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.JetStream;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class PubSubTests
{
    [TestMethod]
    public async Task Consumer_ReceivesPublishedMessage()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("PUBSUB");
        var probe = new Probe<INatsJSMsg<OrderEvent>>
        {
            Behaviour = (msg, _, ct) => msg.AckAsync(cancellationToken: ct).AsTask()
        };

        await using var host = await TestHost.StartAsync(o =>
            {
                o.StreamDefinitions = [stream];
                o.Bind<LegacyOrderConsumer>(new NatsConsumerAttribute
                {
                    Stream = stream.Name, Subject = $"{root}.created", QueueGroup = TestNames.Unique("group")
                });
            },
            services => services.AddSingleton(probe));
        await host.WaitUntilStartedAsync();

        await host.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 99.99m));

        var result = (await probe.WaitForCallsAsync(1)).Single().Arg.Data!;
        Assert.AreEqual("ORD-1", result.OrderId);
        Assert.AreEqual(99.99m, result.Amount);
    }
}
