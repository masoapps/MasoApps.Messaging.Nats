using Messaging.NATS.IntegrationTests.Handlers;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class HostedServiceTests
{
    [TestMethod]
    public async Task DeclaredStreamConsumer_StartsThroughTheHostedService_AndReceivesMessages()
    {
        var signals = new HostedSignals();
        await using var host = await TestHost.StartAsync(
            o =>
            {
                o.HandlerTypes = [typeof(HostedOrderConsumer)];
                o.StreamDefinitions = [new StreamDefinition { Name = HostedOrderConsumer.Stream, Subjects = ["hostedorders.>"] }];
            },
            services => services.AddSingleton(signals));

        await host.WaitUntilStartedAsync();
        Assert.AreEqual(ComponentState.Running,
            host.Status.Get($"consumer:{HostedOrderConsumer.Stream}/{HostedOrderConsumer.Durable}")!.State);
        Assert.AreEqual(ComponentState.Running, host.Status.Get("dlq")!.State);

        var orderId = TestNames.Unique("ORD");
        await host.Services.GetRequiredService<IPublisher>().PublishAsync("hostedorders.created", new OrderEvent(orderId, 5m));

        var received = await signals.WaitForOrder(orderId).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.AreEqual(5m, received.Amount);
    }

    [TestMethod]
    public async Task UndeclaredStreamConsumer_RetriesUntilTheStreamExists()
    {
        await using (var connection = TestHost.CreateConnection())
        {
            try
            {
                await new NatsJSContext(connection).DeleteStreamAsync(UndeclaredStreamConsumer.Stream);
            }
            catch (NatsJSApiException e) when (e.Error.ErrCode == 10059)
            {
            }
        }

        var signals = new HostedSignals();
        await using var host = await TestHost.StartAsync(
            o => o.HandlerTypes = [typeof(UndeclaredStreamConsumer)],
            services => services.AddSingleton(signals));

        var component = $"consumer:{UndeclaredStreamConsumer.Stream}/{UndeclaredStreamConsumer.Durable}";
        var retrying = await host.WaitForStateAsync(component, ComponentState.Retrying);
        StringAssert.Contains(retrying.Error, "10059");

        await using (var connection = TestHost.CreateConnection())
        {
            await new NatsJSContext(connection).CreateStreamAsync(
                new StreamConfig(UndeclaredStreamConsumer.Stream, ["hostedwaiting.>"]));
        }

        await host.WaitForStateAsync(component, ComponentState.Running, TimeSpan.FromSeconds(30));

        var orderId = TestNames.Unique("ORD");
        await host.Services.GetRequiredService<IPublisher>().PublishAsync("hostedwaiting.created", new OrderEvent(orderId, 1m));
        await signals.WaitForOrder(orderId).WaitAsync(TimeSpan.FromSeconds(10));
    }

    [TestMethod]
    public async Task ReplyHandler_StartsThroughTheHostedService()
    {
        await using var host = await TestHost.StartAsync(o => o.HandlerTypes = [typeof(HostedPingHandler)]);
        await host.WaitUntilStartedAsync();

        Assert.IsNull(host.Status.Get("dlq"), "No consumers, so no dead-letter component");

        var response = await host.Services.GetRequiredService<IPublisher>()
            .RequestAsync<PingRequest, PingResponse>(HostedPingHandler.Subject, new PingRequest("hi"));

        Assert.AreEqual("hi", response!.Payload);
        Assert.AreEqual(nameof(HostedPingHandler), response.HandledBy);
    }

    [TestMethod]
    public async Task DeadLetterStreamNull_DlqComponentWaitsForAStreamThatCapturesThePrefix()
    {
        var prefix = TestNames.Unique("owned");
        await using var host = await TestHost.StartAsync(o =>
        {
            o.HandlerTypes = [typeof(OwnedDeadLetterConsumer)];
            o.StreamDefinitions = [new StreamDefinition { Name = OwnedDeadLetterConsumer.Stream, Subjects = ["hostedowned.>"] }];
            o.DeadLetterStream = null;
            o.DeadLetterSubjectPrefix = prefix;
        });

        await host.WaitForStateAsync("dlq", ComponentState.Retrying);

        var dlqStream = TestNames.Unique("OWNED_DLQ");
        await using (var connection = TestHost.CreateConnection())
        {
            await new NatsJSContext(connection).CreateStreamAsync(new StreamConfig(dlqStream, [$"{prefix}.>"]));
        }

        await host.WaitForStateAsync("dlq", ComponentState.Running, TimeSpan.FromSeconds(30));
    }
    [TestMethod]
    [DataRow(StreamProvisioning.CreateIfMissing)]
    [DataRow(StreamProvisioning.CreateOrUpdate)]
    public async Task DeclaredStreamLostWhileRunning_IsProvisionedAgain_AndConsumingResumes(StreamProvisioning mode)
    {
        var probe = new Probe<MessageContext<OrderEvent>>();
        var logs = new LogCapture();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe, logs], o =>
            {
                o.StreamProvisioning = mode;
                o.ConsumerTestHooks.InfoRefreshInterval = TimeSpan.FromMilliseconds(300);
            },
            new NatsConsumerAttribute { Subject = ".created" });
        var streamComponent = $"stream:{scenario.Stream.Name}";

        // A Memory stream after a server restart, or a delete by hand.
        await scenario.Host.JetStream.DeleteStreamAsync(scenario.Stream.Name);

        await logs.WaitForAsync(e => e.Message.Contains($"Stream {scenario.Stream.Name} is back"), TimeSpan.FromSeconds(20),
            "the stream is provisioned again");
        Assert.AreEqual(scenario.Stream.Name, (await scenario.Host.JetStream.GetStreamAsync(scenario.Stream.Name)).Info.Config.Name);
        await scenario.Host.WaitForStateAsync(streamComponent, ComponentState.Running);
        await scenario.Host.WaitForStateAsync(scenario.Component, ComponentState.Running);

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-AFTER", 1m));
        Assert.AreEqual("ORD-AFTER", (await probe.WaitForCallsAsync(1, TimeSpan.FromSeconds(10))).Single().Arg.Message.OrderId);
    }

    [TestMethod]
    public async Task ConsumerThatFailsRightAfterItStarts_BacksOffInsteadOfRetryingEverySecond()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("BACKOFF");
        var queueGroup = TestNames.Unique("worker");
        await using (var setup = TestHost.CreateConnection())
        {
            var js = new NatsJSContext(setup);
            await js.CreateStreamAsync(stream.ToStreamConfig());

            // Every fetch from a push consumer fails at once with 409, after the durable was ensured.
            await js.CreateOrUpdateConsumerAsync(stream.Name, new ConsumerConfig($"{queueGroup}_{nameof(OrderHandler)}")
            {
                FilterSubject = $"{root}.created",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverSubject = TestNames.Unique("push")
            });
        }

        var logs = new LogCapture();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>(), logs], null,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup });

        await Task.Delay(TimeSpan.FromSeconds(12));

        // 1, 2, 4, 8 s: at most five ensures in 12 s. Without the growing backoff it was one every second.
        var ensures = logs.Entries.Count(e => e.Message.Contains("is consuming"));
        Assert.IsGreaterThanOrEqualTo(2, ensures, "The consumer was retried");
        Assert.IsLessThanOrEqualTo(6, ensures, $"{ensures} ensures in 12 s");
        var retrying = await scenario.Host.WaitForStateAsync(scenario.Component, ComponentState.Retrying);
        StringAssert.Contains(retrying.Error, "push based");
    }
}
