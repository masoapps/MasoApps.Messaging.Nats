using System.Text.Json;
using Messaging.NATS.IntegrationTests.Handlers;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class BindingTests
{
    [TestMethod]
    public void FourXAttributeShape_BindsWithTheSameDurableName()
    {
        var durable = Register(o => o.HandlerTypes = [typeof(FourXShapedConsumer)]).Consumers.Single();

        Assert.AreEqual("order-worker_FourXShapedConsumer", durable.Durable);
        Assert.AreEqual("orders", durable.Stream);
        Assert.AreEqual("orders.created", durable.Bindings.Single().Filter);
        Assert.AreEqual(10, durable.Bindings.Single().MaxDeliveryAttempts);
        Assert.IsTrue(durable.Bindings.Single().IsLegacy);
    }

    [TestMethod]
    public void AbstractBaseClassInterface_IsDiscovered()
    {
        var durable = Register(o => o.HandlerTypes = [typeof(DailyReportConsumer)]).Consumers.Single();

        Assert.AreEqual("reports_DailyReportConsumer", durable.Durable);
        Assert.AreEqual("REPORTS", durable.Stream);
        var binding = durable.Bindings.Single();
        Assert.AreEqual(typeof(OrderEvent), binding.MessageType);
        Assert.IsTrue(binding.IsLegacy);
        Assert.AreEqual("reports.daily", binding.Filter);
    }

    [TestMethod]
    public async Task AbstractBaseClassInterface_ReceivesMessages()
    {
        var probe = new Probe<INatsJSMsg<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<DailyReportConsumer>([probe], null,
            new NatsConsumerAttribute { Subject = ".daily" });

        await scenario.Publisher.PublishAsync($"{scenario.Root}.daily", new OrderEvent("REPORT-1", 1m));

        Assert.AreEqual("REPORT-1", (await probe.WaitForCallsAsync(1)).Single().Arg.Data!.OrderId);
    }

    [TestMethod]
    public void MultiTypeClass_WithRealAttributes_GetsOneDurableWithABindingPerType()
    {
        var durable = Register(o => o.HandlerTypes = [typeof(AttributedMultiTypeHandler)]).Consumers.Single();

        Assert.AreEqual("activity_AttributedMultiTypeHandler", durable.Durable);
        Assert.HasCount(2, durable.Bindings);
        var payments = durable.Bindings.Single(b => b.MessageType == typeof(PaymentEvent));
        Assert.AreEqual("activity.payments", payments.Filter);
        Assert.AreEqual(7, payments.MaxDeliveryAttempts);
        Assert.AreEqual(3, durable.Bindings.Single(b => b.MessageType == typeof(OrderEvent)).MaxDeliveryAttempts);
    }

    [TestMethod]
    public async Task MultiTypeClass_GetsOneDurableWithFilterSubjects_AndEachTypeGetsItsOwnMessages()
    {
        var orders = new Probe<MessageContext<OrderEvent>>();
        var payments = new Probe<MessageContext<PaymentEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<MultiTypeHandler>([orders, payments], null,
            new NatsConsumerAttribute { Subject = ".orders", MessageType = typeof(OrderEvent) },
            new NatsConsumerAttribute { Subject = ".payments.*", MessageType = typeof(PaymentEvent) });

        var consumers = await ConsumerNamesAsync(scenario.Host, scenario.Stream.Name);
        CollectionAssert.AreEqual(new[] { scenario.Durable }, consumers);
        var config = (await scenario.InfoAsync()).Config;
        Assert.IsNull(config.FilterSubject);
        CollectionAssert.AreEqual(new[] { $"{scenario.Root}.orders", $"{scenario.Root}.payments.*" },
            config.FilterSubjects!.ToArray());

        await scenario.Publisher.PublishAsync($"{scenario.Root}.orders", new OrderEvent("ORD-1", 1m));
        await scenario.Publisher.PublishAsync($"{scenario.Root}.payments.card", new PaymentEvent("PAY-1", 2m));
        await scenario.Publisher.PublishAsync($"{scenario.Root}.orders", new OrderEvent("ORD-2", 3m));

        var orderCalls = await orders.WaitForCallsAsync(2);
        var paymentCalls = await payments.WaitForCallsAsync(1);
        await Task.Delay(300);

        CollectionAssert.AreEquivalent(new[] { "ORD-1", "ORD-2" }, orderCalls.Select(c => c.Arg.Message.OrderId).ToArray());
        Assert.AreEqual("PAY-1", paymentCalls.Single().Arg.Message.PaymentId);
        Assert.AreEqual($"{scenario.Root}.payments.card", paymentCalls.Single().Arg.Subject);
        Assert.AreEqual(2, orders.Count);
        Assert.AreEqual(1, payments.Count);
    }

    [TestMethod]
    public async Task AddingASecondType_KeepsTheDurable_AndUpdatesItsFilterInPlace()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("ACTIVITY");
        var queueGroup = TestNames.Unique("activity");
        var durable = $"{queueGroup}_ActivityHandler";

        var ordersV1 = new Probe<MessageContext<OrderEvent>>();
        ConsumerInfo before;
        await using (var v1 = await ConsumerScenario.StartAsync<Handlers.V1.ActivityHandler>(stream, root, [ordersV1], null,
                         new NatsConsumerAttribute { Subject = ".orders", QueueGroup = queueGroup }))
        {
            Assert.AreEqual(durable, v1.Durable);
            before = await v1.InfoAsync();
            Assert.AreEqual($"{root}.orders", before.Config.FilterSubject, "One binding uses FilterSubject");
            Assert.IsTrue(before.Config.FilterSubjects == null || before.Config.FilterSubjects.Count == 0);

            await v1.Publisher.PublishAsync($"{root}.orders", new OrderEvent("ORD-1", 1m));
            await ordersV1.WaitForCallsAsync(1);
            await Eventually.TrueAsync(async () => (await v1.InfoAsync()).NumAckPending == 0, TimeSpan.FromSeconds(5),
                "ORD-1 is acked");
        }

        await PublishJsonAsync($"{root}.orders", new OrderEvent("ORD-2", 2m));
        await PublishJsonAsync($"{root}.payments", new PaymentEvent("PAY-1", 3m));

        var ordersV2 = new Probe<MessageContext<OrderEvent>>();
        var paymentsV2 = new Probe<MessageContext<PaymentEvent>>();
        await using var v2 = await ConsumerScenario.StartAsync<Handlers.V2.ActivityHandler>(stream, root,
            [ordersV2, paymentsV2], null,
            new NatsConsumerAttribute { Subject = ".orders", MessageType = typeof(OrderEvent), QueueGroup = queueGroup },
            new NatsConsumerAttribute { Subject = ".payments", MessageType = typeof(PaymentEvent) });

        Assert.AreEqual(durable, v2.Durable);
        Assert.AreEqual("ORD-2", (await ordersV2.WaitForCallsAsync(1)).Single().Arg.Message.OrderId);
        Assert.AreEqual("PAY-1", (await paymentsV2.WaitForCallsAsync(1)).Single().Arg.Message.PaymentId);

        CollectionAssert.AreEqual(new[] { durable }, await ConsumerNamesAsync(v2.Host, stream.Name));
        var after = await v2.InfoAsync();
        Assert.AreEqual(before.Created, after.Created, "The same durable, updated in place");
        Assert.AreEqual(before.Config.DeliverPolicy, after.Config.DeliverPolicy);
        Assert.IsNull(after.Config.FilterSubject);
        CollectionAssert.AreEqual(new[] { $"{root}.orders", $"{root}.payments" }, after.Config.FilterSubjects!.ToArray());
    }

    [TestMethod]
    public async Task OldAndNewVersionsRunningTogether_TheOldOneHandsTheNewSubjectBack_InsteadOfDeadLetteringIt()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("ROLLING");
        var queueGroup = TestNames.Unique("activity");

        var ordersV1 = new Probe<MessageContext<OrderEvent>>();
        await using var v1 = await ConsumerScenario.StartAsync<Handlers.V1.ActivityHandler>(stream, root, [ordersV1], null,
            new NatsConsumerAttribute { Subject = ".orders", QueueGroup = queueGroup });
        await using var v1DeadLetters = await v1.Host.CollectDeadLettersAsync();

        // A rolling deploy: v2 adds a binding and widens the shared durable's filters while v1 still runs.
        var ordersV2 = new Probe<MessageContext<OrderEvent>>();
        var paymentsV2 = new Probe<MessageContext<PaymentEvent>>();
        await using var v2 = await ConsumerScenario.StartAsync<Handlers.V2.ActivityHandler>(stream, root,
            [ordersV2, paymentsV2], null,
            new NatsConsumerAttribute { Subject = ".orders", MessageType = typeof(OrderEvent), QueueGroup = queueGroup },
            new NatsConsumerAttribute { Subject = ".payments", MessageType = typeof(PaymentEvent) });
        await using var v2DeadLetters = await v2.Host.CollectDeadLettersAsync();

        for (var i = 0; i < 20; i++)
        {
            await PublishJsonAsync($"{root}.payments", new PaymentEvent($"PAY-{i}", i));
        }

        await Eventually.TrueAsync(() => Task.FromResult(paymentsV2.Calls.Select(c => c.Arg.Message.PaymentId).Distinct().Count() == 20),
            TimeSpan.FromSeconds(40), $"v2 handled {paymentsV2.Count} payments");
        Assert.HasCount(0, v1DeadLetters.Received, "The old pod dead-lettered none of the new subject");
        Assert.HasCount(0, v2DeadLetters.Received);
    }

    [TestMethod]
    public async Task AMessageTheOldVersionHandedBack_IsHandledByTheNewVersion_NotDeadLetteredByTheCrashLoopGuard()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("HANDBACK");
        var queueGroup = TestNames.Unique("activity");
        NatsConsumerAttribute[] V2() =>
        [
            new() { Subject = ".orders", MessageType = typeof(OrderEvent), QueueGroup = queueGroup },
            new() { Subject = ".payments", MessageType = typeof(PaymentEvent) }
        ];

        // v2 widened the durable's filters and went away (its pod restarts); only v1 runs.
        var paymentsV2 = new Probe<MessageContext<PaymentEvent>>();
        await (await ConsumerScenario.StartAsync<Handlers.V2.ActivityHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>(), paymentsV2], null, V2())).DisposeAsync();
        await using var v1 = await ConsumerScenario.StartAsync<Handlers.V1.ActivityHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>()], null,
            new NatsConsumerAttribute { Subject = ".orders", QueueGroup = queueGroup });
        await using var deadLetters = await v1.Host.CollectDeadLettersAsync();

        // v1 keeps the widened filters only until it writes its own back, so restore them for the test.
        await using (var setup = TestHost.CreateConnection())
        {
            var js = new NatsJSContext(setup);
            var config = (await js.GetConsumerAsync(stream.Name, v1.Durable)).Info.Config;
            await js.CreateOrUpdateConsumerAsync(stream.Name, config with
            {
                FilterSubject = null,
                FilterSubjects = [$"{root}.orders", $"{root}.payments"]
            });
        }

        await PublishJsonAsync($"{root}.payments", new PaymentEvent("PAY-1", 1m));
        await Eventually.TrueAsync(async () => (await v1.InfoAsync()).NumRedelivered >= 1, TimeSpan.FromSeconds(20),
            "v1 handed the payment back");

        await using var v2 = await ConsumerScenario.StartAsync<Handlers.V2.ActivityHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>(), paymentsV2], null, V2());
        var handled = (await paymentsV2.WaitForCallsAsync(1, TimeSpan.FromSeconds(30))).Single().Arg;

        Assert.AreEqual("PAY-1", handled.Message.PaymentId);
        Assert.IsGreaterThan(1, handled.DeliveryCount, "It was handed back first");
        Assert.IsLessThanOrEqualTo(v2.Host.Options.MaxDeliverySlack + 1, handled.DeliveryCount,
            "Hand-backs stop at MaxDeliverySlack, so the crash-loop guard (A + S) never trips for them");
        Assert.IsEmpty(deadLetters.Received);
    }

    [TestMethod]
    public async Task AMessageNoProcessCanHandle_IsDeadLetteredAsUnroutable_AfterMaxDeliverySlackHandBacks()
    {
        var (stream, root) = ConsumerBindings.UniqueStream("UNROUTABLE");
        var queueGroup = TestNames.Unique("activity");
        await (await ConsumerScenario.StartAsync<Handlers.V2.ActivityHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>(), new Probe<MessageContext<PaymentEvent>>()], null,
            new NatsConsumerAttribute { Subject = ".orders", MessageType = typeof(OrderEvent), QueueGroup = queueGroup },
            new NatsConsumerAttribute { Subject = ".payments", MessageType = typeof(PaymentEvent) })).DisposeAsync();
        await using var v1 = await ConsumerScenario.StartAsync<Handlers.V1.ActivityHandler>(stream, root,
            [new Probe<MessageContext<OrderEvent>>()], o => o.MaxDeliverySlack = 2,
            new NatsConsumerAttribute { Subject = ".orders", QueueGroup = queueGroup });
        await using var deadLetters = await v1.Host.CollectDeadLettersAsync();
        await using (var setup = TestHost.CreateConnection())
        {
            var js = new NatsJSContext(setup);
            var config = (await js.GetConsumerAsync(stream.Name, v1.Durable)).Info.Config;
            await js.CreateOrUpdateConsumerAsync(stream.Name, config with
            {
                FilterSubject = null,
                FilterSubjects = [$"{root}.orders", $"{root}.payments"]
            });
        }

        await PublishJsonAsync($"{root}.payments", new PaymentEvent("PAY-1", 1m));

        var dead = (await deadLetters.WaitForAsync(1, TimeSpan.FromSeconds(30))).Single();
        Assert.AreEqual("UnroutableSubject", dead.Header("X-Failure-Type"));
        Assert.AreEqual("3", dead.Header("X-Delivery-Count"), "Handed back MaxDeliverySlack (2) times");
    }

    [TestMethod]
    public void ConsumerLevelSettings_SetOnOneAttribute_ApplyToTheDurable()
    {
        var durable = Register(o => o.Bind<MultiTypeHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", MessageType = typeof(OrderEvent), QueueGroup = "g" },
            new NatsConsumerAttribute { Stream = "S", Subject = "s.payments", MessageType = typeof(PaymentEvent), MaxConcurrency = 4, AckWaitSeconds = 90 })).Consumers.Single();

        Assert.AreEqual("g_MultiTypeHandler", durable.Durable);
        Assert.AreEqual(4, durable.MaxConcurrency);
        Assert.AreEqual(TimeSpan.FromSeconds(90), durable.AckWait);
        Assert.AreEqual(1000, durable.MaxAckPending);
    }

    [TestMethod]
    public void ConflictingConsumerLevelSettings_Throw()
    {
        var error = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<MultiTypeHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", MessageType = typeof(OrderEvent), QueueGroup = "g", AckWaitSeconds = 10 },
            new NatsConsumerAttribute { Stream = "S", Subject = "s.payments", MessageType = typeof(PaymentEvent), AckWaitSeconds = 20 })));
        StringAssert.Contains(error.Message, "AckWaitSeconds");

        error = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<MultiTypeHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", MessageType = typeof(OrderEvent), QueueGroup = "a" },
            new NatsConsumerAttribute { Stream = "S", Subject = "s.payments", MessageType = typeof(PaymentEvent), QueueGroup = "b" })));
        StringAssert.Contains(error.Message, "QueueGroup");
    }

    [TestMethod]
    public void OverlappingFiltersWithinAClass_Throw()
    {
        var error = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<MultiTypeHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.>", MessageType = typeof(OrderEvent), QueueGroup = "g" },
            new NatsConsumerAttribute { Stream = "S", Subject = "s.payments", MessageType = typeof(PaymentEvent) })));
        StringAssert.Contains(error.Message, "overlap");
    }

    [TestMethod]
    public void EmptySubject_WithSeveralBindings_Throws()
    {
        var error = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<MultiTypeHandler>(
            new NatsConsumerAttribute { Stream = "S", MessageType = typeof(OrderEvent), QueueGroup = "g" },
            new NatsConsumerAttribute { Stream = "S", Subject = "s.payments", MessageType = typeof(PaymentEvent) })));
        StringAssert.Contains(error.Message, "empty Subject");
    }

    [TestMethod]
    public void MultiTypeClass_AttributeErrors_Throw()
    {
        var noType = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<MultiTypeHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", QueueGroup = "g" })));
        StringAssert.Contains(noType.Message, "MessageType");

        var unbound = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<MultiTypeHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", MessageType = typeof(OrderEvent), QueueGroup = "g" })));
        StringAssert.Contains(unbound.Message, nameof(PaymentEvent));

        var notHandled = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<OrderHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", MessageType = typeof(PaymentEvent), QueueGroup = "g" })));
        StringAssert.Contains(notHandled.Message, "does not implement");

        var both = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<BothInterfacesHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", QueueGroup = "g" })));
        StringAssert.Contains(both.Message, "both");

        var none = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<NotAHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", QueueGroup = "g" })));
        StringAssert.Contains(none.Message, "implements neither");
    }

    [TestMethod]
    public void InvalidValues_Throw()
    {
        Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<OrderHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", QueueGroup = "g", MaxDeliveryAttempts = 0 })));
        Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<OrderHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", QueueGroup = "g", RetryDelaysSeconds = [1, -1] })));
        Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<OrderHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", QueueGroup = "g", MaxConcurrency = 8, MaxAckPending = 4 })));
        Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<OrderHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s..orders", QueueGroup = "g" })));
        Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<OrderHandler>(
            new NatsConsumerAttribute { Subject = "s.orders", QueueGroup = "g" })));
    }

    [TestMethod]
    public void QueueGroup_FallsBackToDefaultQueueGroup_ThenServiceName()
    {
        NatsConsumerAttribute Attribute(string queueGroup = "") =>
            new() { Stream = "S", Subject = "s.orders", QueueGroup = queueGroup };

        Assert.AreEqual("attr_OrderHandler", Register(o =>
        {
            o.DefaultQueueGroup = "default";
            o.ServiceName = "service";
            o.Bind<OrderHandler>(Attribute("attr"));
        }).Consumers.Single().Durable);

        Assert.AreEqual("default_OrderHandler", Register(o =>
        {
            o.DefaultQueueGroup = "default";
            o.ServiceName = "service";
            o.Bind<OrderHandler>(Attribute());
        }).Consumers.Single().Durable);

        var serviceOnly = Register(o =>
        {
            o.ServiceName = "service";
            o.Bind<OrderHandler>(Attribute());
        }).Consumers.Single();
        Assert.AreEqual("service_OrderHandler", serviceOnly.Durable);
        Assert.AreEqual("service", serviceOnly.Owner);

        var error = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o => o.Bind<OrderHandler>(Attribute())));
        StringAssert.Contains(error.Message, "queue group");
    }

    [TestMethod]
    public void ExplicitDurable_IsUsedAndCleaned()
    {
        var durable = Register(o => o.Bind<OrderHandler>(
            new NatsConsumerAttribute { Stream = "S", Subject = "s.orders", QueueGroup = "g", Durable = "orders.v2 worker" })).Consumers.Single();

        Assert.AreEqual("orders_v2_worker", durable.Durable);
    }

    [TestMethod]
    public void DuplicateDurableOnOneStream_Throws()
    {
        var error = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o =>
        {
            o.Bind<Handlers.V1.ActivityHandler>(new NatsConsumerAttribute { Stream = "S", Subject = "s.a", QueueGroup = "g" });
            o.Bind<Handlers.V2.ActivityHandler>(
                new NatsConsumerAttribute { Stream = "S", Subject = "s.b", MessageType = typeof(OrderEvent), QueueGroup = "g" },
                new NatsConsumerAttribute { Stream = "S", Subject = "s.c", MessageType = typeof(PaymentEvent) });
        }));
        StringAssert.Contains(error.Message, "g_ActivityHandler");

        Assert.ThrowsExactly<InvalidOperationException>(() => Register(o =>
        {
            o.Bind<OrderHandler>(new NatsConsumerAttribute { Stream = "S", Subject = "s.a", QueueGroup = "g", Durable = "same" });
            o.Bind<LegacyOrderConsumer>(new NatsConsumerAttribute { Stream = "S", Subject = "s.b", QueueGroup = "g", Durable = "same" });
        }));
    }

    [TestMethod]
    public void OneClassOnTwoStreams_GetsADurableOnEach()
    {
        var durables = Register(o => o.Bind<OrderHandler>(
            new NatsConsumerAttribute { Stream = "A", Subject = "a.orders", QueueGroup = "g" },
            new NatsConsumerAttribute { Stream = "B", Subject = "b.orders", QueueGroup = "g" })).Consumers;

        CollectionAssert.AreEquivalent(new[] { "consumer:A/g_OrderHandler", "consumer:B/g_OrderHandler" },
            durables.Select(d => d.ComponentName).ToArray());
    }

    [TestMethod]
    public void ScheduleStream_FilterMatchingTheHolders_OrEmpty_Throws()
    {
        foreach (var subject in new[] { ">", "_sched.>", "*.>", "" })
        {
            var error = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o =>
            {
                o.StreamDefinitions = [StreamDefinition.Scheduled("TIMERS", "timers.>")];
                o.Bind<OrderHandler>(new NatsConsumerAttribute { Stream = "TIMERS", Subject = subject, QueueGroup = "g" });
            }), $"Subject '{subject}'");
            StringAssert.Contains(error.Message, "schedule", $"Subject '{subject}'");
        }

        Register(o =>
        {
            o.StreamDefinitions = [StreamDefinition.Scheduled("TIMERS", "timers.>")];
            o.Bind<OrderHandler>(new NatsConsumerAttribute { Stream = "TIMERS", Subject = "timers.>", QueueGroup = "g" });
        });
    }

    [TestMethod]
    public void DeliverFromNew_OnADeclaredWorkQueueStream_Throws()
    {
        var error = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o =>
        {
            o.StreamDefinitions = [new StreamDefinition { Name = "WQ", Subjects = ["wq.>"], Retention = StreamConfigRetention.Workqueue }];
            o.Bind<OrderHandler>(new NatsConsumerAttribute { Stream = "WQ", Subject = "wq.orders", QueueGroup = "g", DeliverFrom = DeliverFrom.New });
        }));
        StringAssert.Contains(error.Message, "WorkQueue");
    }

    [TestMethod]
    public async Task DeliverFromNew_OnAnUndeclaredWorkQueueStream_IsFaulted()
    {
        var name = TestNames.Unique("WQ_UNDECLARED");
        var root = TestNames.Unique("wq");
        await CreateStreamByHandAsync(new StreamConfig(name, [$"{root}.>"]) { Retention = StreamConfigRetention.Workqueue });

        await using var host = await TestHost.StartAsync(o => o.Bind<OrderHandler>(new NatsConsumerAttribute
        {
            Stream = name, Subject = $"{root}.orders", QueueGroup = "g", DeliverFrom = DeliverFrom.New
        }), services => services.AddSingleton(new Probe<MessageContext<OrderEvent>>()));

        var status = await host.WaitForStateAsync($"consumer:{name}/g_OrderHandler", ComponentState.Faulted);
        StringAssert.Contains(status.Error, "WorkQueue");
    }

    [TestMethod]
    public void InactiveThreshold_OnADeclaredInterestStream_Throws()
    {
        var error = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o =>
        {
            o.StreamDefinitions = [new StreamDefinition { Name = "INT", Subjects = ["int.>"], Retention = StreamConfigRetention.Interest }];
            o.Bind<OrderHandler>(new NatsConsumerAttribute
            {
                Stream = "INT", Subject = "int.orders", QueueGroup = "g", DeliverFrom = DeliverFrom.All, InactiveThresholdSeconds = 60
            });
        }));
        StringAssert.Contains(error.Message, "Interest retention");

        var limits = Register(o =>
        {
            o.StreamDefinitions = [new StreamDefinition { Name = "LIM", Subjects = ["lim.>"] }];
            o.Bind<OrderHandler>(new NatsConsumerAttribute
            {
                Stream = "LIM", Subject = "lim.orders", QueueGroup = "g", InactiveThresholdSeconds = 60
            });
        }).Consumers.Single();
        Assert.AreEqual(TimeSpan.FromSeconds(60), limits.InactiveThreshold, "Allowed on a Limits stream");
    }

    [TestMethod]
    public async Task InactiveThreshold_OnAnUndeclaredInterestStream_IsFaulted_AndNoDurableIsCreated()
    {
        var name = TestNames.Unique("INT_UNDECLARED");
        var root = TestNames.Unique("int");
        await CreateStreamByHandAsync(new StreamConfig(name, [$"{root}.>"]) { Retention = StreamConfigRetention.Interest });

        await using var host = await TestHost.StartAsync(o => o.Bind<OrderHandler>(new NatsConsumerAttribute
        {
            Stream = name, Subject = $"{root}.orders", QueueGroup = "g", InactiveThresholdSeconds = 1
        }), services => services.AddSingleton(new Probe<MessageContext<OrderEvent>>()));

        var status = await host.WaitForStateAsync($"consumer:{name}/g_OrderHandler", ComponentState.Faulted);
        StringAssert.Contains(status.Error, "Interest retention");
        CollectionAssert.AreEqual(Array.Empty<string>(), await ConsumerNamesAsync(host, name));
    }

    [TestMethod]
    public async Task HolderFilter_OnAnUndeclaredScheduleStream_IsFaulted()
    {
        var name = TestNames.Unique("SCHED_UNDECLARED");
        var root = TestNames.Unique("timers");
        await CreateStreamByHandAsync(new StreamConfig(name, [$"{root}.>", $"_sched.{name}.>"]) { AllowMsgSchedules = true });

        await using var host = await TestHost.StartAsync(o => o.Bind<OrderHandler>(new NatsConsumerAttribute
        {
            Stream = name, Subject = ">", QueueGroup = "g"
        }), services => services.AddSingleton(new Probe<MessageContext<OrderEvent>>()));

        var status = await host.WaitForStateAsync($"consumer:{name}/g_OrderHandler", ComponentState.Faulted);
        StringAssert.Contains(status.Error, "_sched");
    }

    [TestMethod]
    public void RetryDelay_UsesTheEntryForTheDelivery_AndRepeatsTheLast()
    {
        var binding = Register(o => o.Bind<OrderHandler>(new NatsConsumerAttribute
        {
            Stream = "S", Subject = "s.orders", QueueGroup = "g", RetryDelaysSeconds = [1, 5]
        })).Consumers.Single().Bindings.Single();

        Assert.AreEqual(TimeSpan.FromSeconds(1), binding.RetryDelay(1));
        Assert.AreEqual(TimeSpan.FromSeconds(5), binding.RetryDelay(2));
        Assert.AreEqual(TimeSpan.FromSeconds(5), binding.RetryDelay(9));

        var defaults = Register(o => o.Bind<OrderHandler>(new NatsConsumerAttribute
        {
            Stream = "S", Subject = "s.orders", QueueGroup = "g"
        })).Consumers.Single().Bindings.Single();
        CollectionAssert.AreEqual(new[] { 1d, 5, 30, 120, 600 }, defaults.RetryDelays.Select(d => d.TotalSeconds).ToArray());
    }

    internal static NatsMessagingRegistration Register(Action<NatsMessagingOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNatsMessaging(o =>
        {
            o.NatsUrl = "nats://127.0.0.1:1";
            configure(o);
        });
        return services.BuildServiceProvider().GetRequiredService<NatsMessagingRegistration>();
    }

    private static async Task<string[]> ConsumerNamesAsync(TestHost host, string stream)
    {
        var names = new List<string>();
        await foreach (var name in host.JetStream.ListConsumerNamesAsync(stream))
        {
            names.Add(name);
        }

        return names.ToArray();
    }

    private static async Task PublishJsonAsync<T>(string subject, T message)
    {
        await using var connection = TestHost.CreateConnection();
        var ack = await new NatsJSContext(connection).PublishAsync(subject, JsonSerializer.SerializeToUtf8Bytes(message));
        ack.EnsureSuccess();
    }

    private static async Task CreateStreamByHandAsync(StreamConfig config)
    {
        await using var connection = TestHost.CreateConnection();
        await new NatsJSContext(connection).CreateStreamAsync(config);
    }
}
