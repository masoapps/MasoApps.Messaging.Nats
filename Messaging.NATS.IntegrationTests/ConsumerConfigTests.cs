using System.Text.Json;
using Messaging.NATS.IntegrationTests.Handlers;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class ConsumerConfigTests
{
    [TestMethod]
    public async Task NewDurable_HasNoBackoff_UnlimitedMaxDeliver_AndTheConfiguredSettings()
    {
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(
            [new Probe<MessageContext<OrderEvent>>()], o => o.ServiceName = "billing",
            new NatsConsumerAttribute { Subject = ".created", AckWaitSeconds = 45, MaxAckPending = 200, MaxConcurrency = 2 });

        var config = (await scenario.InfoAsync()).Config;

        Assert.IsTrue(config.Backoff == null || config.Backoff.Count == 0);
        Assert.AreEqual(-1L, config.MaxDeliver);
        Assert.AreEqual(TimeSpan.FromSeconds(45), config.AckWait);
        Assert.AreEqual(200L, config.MaxAckPending);
        Assert.AreEqual(ConsumerConfigAckPolicy.Explicit, config.AckPolicy);
        Assert.AreEqual($"{scenario.Root}.created", config.FilterSubject);
        Assert.IsNull(config.DeliverGroup);
        Assert.AreEqual(TimeSpan.Zero, config.InactiveThreshold);
        Assert.AreEqual("billing", config.Metadata!["messaging-nats.owner"]);
        Assert.AreEqual(typeof(OrderHandler).FullName, config.Metadata["messaging-nats.handler"]);
    }

    [TestMethod]
    [DataRow(StreamConfigRetention.Limits, DeliverFrom.Default, ConsumerConfigDeliverPolicy.New)]
    [DataRow(StreamConfigRetention.Interest, DeliverFrom.Default, ConsumerConfigDeliverPolicy.New)]
    [DataRow(StreamConfigRetention.Workqueue, DeliverFrom.Default, ConsumerConfigDeliverPolicy.All)]
    [DataRow(StreamConfigRetention.Limits, DeliverFrom.All, ConsumerConfigDeliverPolicy.All)]
    [DataRow(StreamConfigRetention.Interest, DeliverFrom.New, ConsumerConfigDeliverPolicy.New)]
    public async Task NewDurable_DeliverPolicy_FollowsDeliverFromAndRetention(StreamConfigRetention retention,
        DeliverFrom deliverFrom, ConsumerConfigDeliverPolicy expected)
    {
        var (stream, root) = ConsumerBindings.UniqueStream("POLICY", retention);
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>()], null,
            new NatsConsumerAttribute { Subject = ".created", DeliverFrom = deliverFrom });

        Assert.AreEqual(expected, (await scenario.InfoAsync()).Config.DeliverPolicy);
    }

    [TestMethod]
    public async Task NewDurable_OnALimitsStream_StartsAtNew()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("NEW");
        await using (var setup = TestHost.CreateConnection())
        {
            await new NatsJSContext(setup).CreateStreamAsync(stream.ToStreamConfig());
        }

        await PublishJsonAsync($"{root}.created", new OrderEvent("BEFORE", 1m));

        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        await scenario.Publisher.PublishAsync($"{root}.created", new OrderEvent("AFTER", 1m));

        await probe.WaitForCallsAsync(1);
        await Task.Delay(500);
        CollectionAssert.AreEqual(new[] { "AFTER" }, probe.Calls.Select(c => c.Arg.Message.OrderId).ToArray());
    }

    [TestMethod]
    [DataRow(StreamConfigRetention.Limits)]
    [DataRow(StreamConfigRetention.Interest)]
    [DataRow(StreamConfigRetention.Workqueue)]
    public async Task ExistingFourXDurable_KeepsDeliverPolicyAndDeliverGroup_AndGetsUnlimitedMaxDeliver(
        StreamConfigRetention retention)
    {
        var (stream, root) = ConsumerBindings.UniqueStream("LEGACY4X", retention);
        var queueGroup = TestNames.Unique("worker");
        var durable = $"{queueGroup}_{nameof(LegacyOrderConsumer)}";
        await using (var setup = TestHost.CreateConnection())
        {
            var js = new NatsJSContext(setup);
            await js.CreateStreamAsync(stream.ToStreamConfig());
            await js.CreateOrUpdateConsumerAsync(stream.Name, new ConsumerConfig(durable)
            {
                DeliverPolicy = ConsumerConfigDeliverPolicy.All,
                FilterSubject = $"{root}.created",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                AckWait = TimeSpan.FromMinutes(5),
                MaxDeliver = 10,
                MaxAckPending = 1000,
                DeliverGroup = queueGroup,
                Backoff = [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5)]
            });
        }

        await PublishJsonAsync($"{root}.created", new OrderEvent("PENDING", 1m));

        var probe = new Probe<INatsJSMsg<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<LegacyOrderConsumer>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup });

        Assert.AreEqual(durable, scenario.Durable);
        var status = scenario.Host.Status.Get(scenario.Component)!;
        Assert.AreEqual(ComponentState.Running, status.State);
        Assert.IsNull(status.Warning, status.Warning);

        var config = (await scenario.InfoAsync()).Config;
        Assert.AreEqual(ConsumerConfigDeliverPolicy.All, config.DeliverPolicy);
        Assert.AreEqual(queueGroup, config.DeliverGroup);
        Assert.AreEqual(-1L, config.MaxDeliver);
        Assert.AreEqual(TimeSpan.FromSeconds(30), config.AckWait);
        Assert.IsTrue(config.Backoff == null || config.Backoff.Count == 0);

        Assert.AreEqual("PENDING", (await probe.WaitForCallsAsync(1)).Single().Arg.Data!.OrderId,
            "A message pending before the upgrade is delivered");
    }

    [TestMethod]
    public async Task InactiveThreshold_IsApplied()
    {
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(
            [new Probe<MessageContext<OrderEvent>>()], null,
            new NatsConsumerAttribute { Subject = ".created", InactiveThresholdSeconds = 30 });

        Assert.AreEqual(TimeSpan.FromSeconds(30), (await scenario.InfoAsync()).Config.InactiveThreshold);
    }

    [TestMethod]
    public async Task DurableDeletedWhileRunning_IsRecreatedFromTheAckFloor_AndNoMessageIsSkipped()
    {
        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe],
            o => o.ConsumerTestHooks.InfoRefreshInterval = TimeSpan.FromMilliseconds(300),
            new NatsConsumerAttribute { Subject = ".created" });
        var subject = $"{scenario.Root}.created";

        for (var i = 1; i <= 3; i++)
        {
            await scenario.Publisher.PublishAsync(subject, new OrderEvent($"ORD-{i}", i));
        }

        await probe.WaitForCallsAsync(3);
        await Eventually.TrueAsync(async () => (await scenario.InfoAsync()).AckFloor.StreamSeq == 3,
            TimeSpan.FromSeconds(5), "The first three are acked");
        await Task.Delay(1000);

        await scenario.Host.JetStream.DeleteConsumerAsync(scenario.Stream.Name, scenario.Durable);
        for (var i = 4; i <= 5; i++)
        {
            await scenario.Publisher.PublishAsync(subject, new OrderEvent($"ORD-{i}", i));
        }

        ConsumerInfo? recreated = null;
        await Eventually.TrueAsync(async () =>
        {
            try
            {
                recreated = await scenario.InfoAsync();
                return true;
            }
            catch (NatsJSApiException e) when (e.Error.ErrCode == 10014)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(20), "The durable is recreated");

        await scenario.Publisher.PublishAsync(subject, new OrderEvent("ORD-6", 6));
        await probe.WaitForCallsAsync(6);
        await Task.Delay(500);

        Assert.AreEqual(ConsumerConfigDeliverPolicy.ByStartSequence, recreated!.Config.DeliverPolicy);
        Assert.AreEqual(4UL, recreated.Config.OptStartSeq);
        var handled = probe.Calls.Select(c => c.Arg.Message.OrderId).ToList();
        CollectionAssert.AreEquivalent(Enumerable.Range(1, 6).Select(i => $"ORD-{i}").ToArray(), handled.ToArray(),
            string.Join(", ", handled));
        Assert.AreEqual(ComponentState.Running, scenario.Host.Status.Get(scenario.Component)!.State);
    }

    [TestMethod]
    public async Task RejectedUpdateOfAnExistingDurable_IsAWarning_AndConsumesWithTheServerConfig()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("REJECT", StreamConfigRetention.Workqueue);
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
                AckPolicy = ConsumerConfigAckPolicy.Explicit
            });
        }

        // The wider filter overlaps the other consumer, which a WorkQueue stream rejects (10100).
        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".>", QueueGroup = queueGroup });

        var status = scenario.Host.Status.Get(scenario.Component)!;
        Assert.AreEqual(ComponentState.Running, status.State);
        StringAssert.Contains(status.Warning, "10100");
        Assert.AreEqual($"{root}.created", (await scenario.InfoAsync()).Config.FilterSubject);

        await scenario.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m));
        Assert.AreEqual("ORD-1", (await probe.WaitForCallsAsync(1)).Single().Arg.Message.OrderId);
    }

    [TestMethod]
    public async Task DurableDeletedBeforeItsFirstAck_IsRecreatedAfterTheMessagesItStartedPast()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("NOACK");
        await using (var setup = TestHost.CreateConnection())
        {
            await new NatsJSContext(setup).CreateStreamAsync(stream.ToStreamConfig());
        }

        for (var i = 1; i <= 3; i++)
        {
            await PublishJsonAsync($"{root}.created", new OrderEvent($"BEFORE-{i}", i));
        }

        // Default on a Limits stream is New: the durable starts past the three stored messages, with ack floor 0.
        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe],
            o => o.ConsumerTestHooks.InfoRefreshInterval = TimeSpan.FromMilliseconds(300),
            new NatsConsumerAttribute { Subject = ".created" });
        Assert.AreEqual(0UL, (await scenario.InfoAsync()).AckFloor.StreamSeq);
        await Task.Delay(1000);

        await scenario.Host.JetStream.DeleteConsumerAsync(scenario.Stream.Name, scenario.Durable);
        var recreated = await WaitForDurableAsync(scenario);

        await scenario.Publisher.PublishAsync($"{root}.created", new OrderEvent("AFTER", 9));
        await probe.WaitForCallsAsync(1);
        await Task.Delay(1000);

        Assert.AreEqual(ConsumerConfigDeliverPolicy.ByStartSequence, recreated.Config.DeliverPolicy);
        Assert.AreEqual(4UL, recreated.Config.OptStartSeq);
        var handled = probe.Calls.Select(c => c.Arg.Message.OrderId).ToArray();
        CollectionAssert.AreEqual(new[] { "AFTER" }, handled, string.Join(", ", handled));
    }

    [TestMethod]
    public async Task StreamDeletedAndCreatedAgain_TheDurableResumesFromTheStartOfTheNewStream()
    {
        // An undeclared stream, so only the owner (this test) creates it again.
        var (stream, root) = ConsumerBindings.UniqueStream("RECREATED");
        var queueGroup = TestNames.Unique("qg");
        await using (var setup = TestHost.CreateConnection())
        {
            await new NatsJSContext(setup).CreateStreamAsync(stream.ToStreamConfig());
        }

        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var host = await TestHost.StartAsync(o =>
        {
            o.Bind<OrderHandler>(new NatsConsumerAttribute
            {
                Stream = stream.Name, Subject = $"{root}.created", QueueGroup = queueGroup
            });
            o.ConsumerTestHooks.InfoRefreshInterval = TimeSpan.FromMilliseconds(300);
        }, services => services.AddSingleton(probe));
        await host.WaitUntilStartedAsync();
        var durable = $"{queueGroup}_{nameof(OrderHandler)}";

        for (var i = 1; i <= 3; i++)
        {
            await host.Publisher.PublishAsync($"{root}.created", new OrderEvent($"ORD-{i}", i));
        }

        await probe.WaitForCallsAsync(3);
        await Eventually.TrueAsync(async () => (await host.ConsumerInfoAsync(stream.Name, durable)).AckFloor.StreamSeq == 3,
            TimeSpan.FromSeconds(5), "The first three are acked");
        await Task.Delay(1000);

        await host.JetStream.DeleteStreamAsync(stream.Name);
        await host.JetStream.CreateStreamAsync(stream.ToStreamConfig());
        ConsumerInfo? recreated = null;
        await Eventually.TrueAsync(async () =>
        {
            try
            {
                recreated = await host.ConsumerInfoAsync(stream.Name, durable);
                return true;
            }
            catch (NatsJSApiException e) when (e.Error.ErrCode == 10014)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(20), "The durable is recreated on the new stream");

        for (var i = 4; i <= 8; i++)
        {
            await host.Publisher.PublishAsync($"{root}.created", new OrderEvent($"ORD-{i}", i));
        }

        await probe.WaitForCallsAsync(8, TimeSpan.FromSeconds(10));
        Assert.AreEqual(ConsumerConfigDeliverPolicy.All, recreated!.Config.DeliverPolicy, "Not from the old stream's ack floor");
        var handled = probe.Calls.Select(c => c.Arg.Message.OrderId).ToArray();
        CollectionAssert.AreEquivalent(Enumerable.Range(1, 8).Select(i => $"ORD-{i}").ToArray(), handled,
            string.Join(", ", handled));
    }

    [TestMethod]
    public async Task AckWaitOfOneSecond_WithFreeSlots_ARunningHandlerIsNotRedelivered()
    {
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, _, ct) => Task.Delay(TimeSpan.FromSeconds(4), ct)
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", AckWaitSeconds = 1, MaxConcurrency = 4 });

        for (var i = 1; i <= 3; i++)
        {
            await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent($"ORD-{i}", i));
        }

        await probe.WaitForCallsAsync(3);
        await Task.Delay(TimeSpan.FromSeconds(6));

        var deliveries = probe.Calls.Select(c => $"{c.Arg.Message.OrderId}/{c.Arg.DeliveryCount}").ToArray();
        Assert.AreEqual(3, probe.Count, string.Join(", ", deliveries));
        Assert.IsTrue(probe.Calls.All(c => c.Arg.DeliveryCount == 1), string.Join(", ", deliveries));
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task RejectedUpdate_WithAShorterServerAckWait_TheHeartbeatFollowsTheServer(bool backoff)
    {
        var (stream, root) = ConsumerBindings.UniqueStream("HEARTBEAT", StreamConfigRetention.Workqueue);
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

            // A 5.x durable: Backoff [2 s, 5 s] makes the server wait only 2 s for the first delivery.
            var config = new ConsumerConfig(durable)
            {
                FilterSubject = $"{root}.created",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                AckWait = backoff ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(2),
                MaxDeliver = backoff ? 5 : -1
            };
            if (backoff)
            {
                config.Backoff = [TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5)];
            }

            await js.CreateOrUpdateConsumerAsync(stream.Name, config);
        }

        // The wider filter overlaps the other consumer, which a WorkQueue stream rejects (10100), so the server's
        // shorter wait stays in force.
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, _, ct) => Task.Delay(TimeSpan.FromSeconds(6), ct)
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".>", QueueGroup = queueGroup, MaxConcurrency = 4, AckWaitSeconds = 30 });
        var status = scenario.Host.Status.Get(scenario.Component)!;
        StringAssert.Contains(status.Warning, "10100");
        StringAssert.Contains(status.Warning, "shorter than the 00:00:30 it is declared with");

        await scenario.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m));
        await probe.WaitForCallsAsync(1);
        await Task.Delay(TimeSpan.FromSeconds(8));

        Assert.AreEqual(1, probe.Count, string.Join(", ", probe.Calls.Select(c => $"{c.At:HH:mm:ss.fff}/{c.Arg.DeliveryCount}")));
        Assert.AreEqual(0L, (await scenario.InfoAsync()).NumRedelivered);
    }

    [TestMethod]
    public async Task ExistingDurableOfAnotherOwner_IsNotTakenOver_AndTheConsumerIsFaulted()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("OWNER");
        var queueGroup = TestNames.Unique("qg");
        var durable = $"{queueGroup}_{nameof(OrderHandler)}";
        await using (var setup = TestHost.CreateConnection())
        {
            var js = new NatsJSContext(setup);
            await js.CreateStreamAsync(stream.ToStreamConfig());
            await js.CreateOrUpdateConsumerAsync(stream.Name, new ConsumerConfig(durable)
            {
                FilterSubject = $"{root}.other",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                Metadata = new Dictionary<string, string> { ["messaging-nats.owner"] = "service-a" }
            });
        }

        await using var host = await TestHost.StartAsync(o =>
        {
            o.ServiceName = "service-b";
            o.StreamDefinitions = [stream];
            o.Bind<OrderHandler>(new NatsConsumerAttribute
            {
                Stream = stream.Name, Subject = $"{root}.created", QueueGroup = queueGroup
            });
        }, services => services.AddSingleton(new Probe<MessageContext<OrderEvent>>()));

        var faulted = await host.WaitForStateAsync($"consumer:{stream.Name}/{durable}", ComponentState.Faulted);
        StringAssert.Contains(faulted.Error, "belongs to 'service-a'");
        var config = (await host.ConsumerInfoAsync(stream.Name, durable)).Config;
        Assert.AreEqual($"{root}.other", config.FilterSubject, "The other service's filter is kept");
        Assert.AreEqual("service-a", config.Metadata!["messaging-nats.owner"]);
    }

    [TestMethod]
    public async Task DurableOwnedByTheQueueGroup_IsKept_WhenTheServiceSetsAServiceNameLater()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("OWNER");
        var queueGroup = TestNames.Unique("qg");
        await using (await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [new Probe<MessageContext<OrderEvent>>()],
                         null, new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup }))
        {
        }

        await using var renamed = await ConsumerScenario.StartAsync<OrderHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>()], o => o.ServiceName = "billing",
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup });

        Assert.AreEqual(ComponentState.Running, renamed.Host.Status.Get(renamed.Component)!.State);
        Assert.AreEqual("billing", (await renamed.InfoAsync()).Config.Metadata!["messaging-nats.owner"]);
    }

    private static async Task<ConsumerInfo> WaitForDurableAsync(ConsumerScenario scenario)
    {
        ConsumerInfo? info = null;
        await Eventually.TrueAsync(async () =>
        {
            try
            {
                info = await scenario.InfoAsync();
                return true;
            }
            catch (NatsJSApiException e) when (e.Error.ErrCode == 10014)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(20), "The durable is recreated");
        return info!;
    }

    private static async Task PublishJsonAsync<T>(string subject, T message)
    {
        await using var connection = TestHost.CreateConnection();
        var ack = await new NatsJSContext(connection).PublishAsync(subject, JsonSerializer.SerializeToUtf8Bytes(message));
        ack.EnsureSuccess();
    }
}
