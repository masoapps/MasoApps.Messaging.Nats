using Messaging.NATS.IntegrationTests.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.Serializers.Json;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class ProvisioningTests
{
    [TestMethod]
    public async Task CreateIfMissing_CreatesStream()
    {
        var definition = new StreamDefinition
        {
            Name = TestNames.Unique("PROV_CREATE"),
            Subjects = [$"{TestNames.Unique("create")}.>"],
            MaxAge = 3600,
            MaxMsgs = 1000,
            Storage = StreamConfigStorage.Memory,
            DuplicateWindow = TimeSpan.FromSeconds(30)
        };

        await using var host = await TestHost.StartAsync(o => o.StreamDefinitions = [definition]);
        await host.WaitUntilStartedAsync();

        var config = await GetConfig(host, definition.Name);
        CollectionAssert.AreEquivalent(definition.Subjects, config.Subjects!.ToArray());
        Assert.AreEqual(TimeSpan.FromHours(1), config.MaxAge);
        Assert.AreEqual(1000, config.MaxMsgs);
        Assert.AreEqual(StreamConfigStorage.Memory, config.Storage);
        Assert.AreEqual(TimeSpan.FromSeconds(30), config.DuplicateWindow);

        var component = host.Status.Get($"stream:{definition.Name}")!;
        Assert.AreEqual(ComponentState.Running, component.State);
        Assert.IsNull(component.Warning);
        Assert.AreEqual(HealthStatus.Healthy, (await host.CheckHealthAsync()).Status);
    }

    [TestMethod]
    public async Task CreateIfMissing_DriftedStream_StartsRunningWithWarning()
    {
        var name = TestNames.Unique("PROV_DRIFT");
        var subject = $"{TestNames.Unique("drift")}.>";
        await CreateByHand(new StreamConfig(name, [subject]) { MaxAge = TimeSpan.FromHours(1) });

        var definition = new StreamDefinition { Name = name, Subjects = [subject], MaxAge = 7200, MaxMsgs = 50 };
        await using var host = await TestHost.StartAsync(o => o.StreamDefinitions = [definition]);
        await host.WaitUntilStartedAsync();

        var component = host.Status.Get($"stream:{name}")!;
        Assert.AreEqual(ComponentState.Running, component.State);
        Assert.IsNotNull(component.Warning);
        StringAssert.Contains(component.Warning, "MaxAge");
        StringAssert.Contains(component.Warning, "MaxMsgs");

        // A component Warning is Degraded (design 1.6), which still passes a readiness probe.
        var health = await host.CheckHealthAsync();
        Assert.AreEqual(HealthStatus.Degraded, health.Status);

        var config = await GetConfig(host, name);
        Assert.AreEqual(TimeSpan.FromHours(1), config.MaxAge, "CreateIfMissing must not apply drift");
        Assert.AreEqual(-1, config.MaxMsgs);
    }

    [TestMethod]
    public async Task CreateIfMissing_EnablesSchedulesOnExistingStream_AndChangesNothingElse()
    {
        var name = TestNames.Unique("PROV_SCHED");
        var subject = $"{TestNames.Unique("sched")}.>";
        await CreateByHand(new StreamConfig(name, [subject])
        {
            Retention = StreamConfigRetention.Workqueue,
            MaxAge = TimeSpan.FromHours(1),
            AllowDirect = true,
            Description = "made by hand"
        });

        var definition = new StreamDefinition
        {
            Name = name,
            Subjects = [subject],
            Retention = StreamConfigRetention.Workqueue,
            MaxAge = 7200,
            AllowMsgSchedules = true
        };
        await using var host = await TestHost.StartAsync(o => o.StreamDefinitions = [definition]);
        await host.WaitUntilStartedAsync();

        var config = await GetConfig(host, name);
        Assert.IsTrue(config.AllowMsgSchedules);
        CollectionAssert.AreEquivalent(new[] { subject, $"_sched.{name}.>" }, config.Subjects!.ToArray());
        Assert.AreEqual(TimeSpan.FromHours(1), config.MaxAge, "Only schedules are enabled; MaxAge drift stays");
        Assert.IsTrue(config.AllowDirect);
        Assert.AreEqual("made by hand", config.Description);

        var component = host.Status.Get($"stream:{name}")!;
        Assert.AreEqual(ComponentState.Running, component.State);
        StringAssert.Contains(component.Warning, "MaxAge");
        Assert.DoesNotContain("AllowMsgSchedules", component.Warning!);
    }

    [TestMethod]
    public async Task VerifyOnly_MissingStream_RetriesUntilItExists()
    {
        var definition = new StreamDefinition
        {
            Name = TestNames.Unique("PROV_VERIFY"),
            Subjects = [$"{TestNames.Unique("verify")}.>"]
        };

        await using var host = await TestHost.StartAsync(o =>
        {
            o.StreamDefinitions = [definition];
            o.StreamProvisioning = StreamProvisioning.VerifyOnly;
        });

        var retrying = await host.WaitForStateAsync($"stream:{definition.Name}", ComponentState.Retrying);
        StringAssert.Contains(retrying.Error, "10059");
        Assert.AreEqual(HealthStatus.Unhealthy, (await host.CheckHealthAsync()).Status);

        await CreateByHand(definition.ToStreamConfig());

        await host.WaitForStateAsync($"stream:{definition.Name}", ComponentState.Running, TimeSpan.FromSeconds(30));
        await host.WaitUntilStartedAsync();
        Assert.AreEqual(HealthStatus.Healthy, (await host.CheckHealthAsync()).Status);
    }

    [TestMethod]
    public async Task VerifyOnly_DeclaredSchedulesMissingOnServer_IsFaulted()
    {
        var name = TestNames.Unique("PROV_VSCHED");
        var subject = $"{TestNames.Unique("vsched")}.>";
        await CreateByHand(new StreamConfig(name, [subject]) { Retention = StreamConfigRetention.Workqueue });

        await using var host = await TestHost.StartAsync(o =>
        {
            o.StreamDefinitions = [StreamDefinition.Scheduled(name, subject)];
            o.StreamProvisioning = StreamProvisioning.VerifyOnly;
        });

        var faulted = await host.WaitForStateAsync($"stream:{name}", ComponentState.Faulted);
        StringAssert.Contains(faulted.Error, "AllowMsgSchedules");
        Assert.IsFalse((await GetConfig(host, name)).AllowMsgSchedules, "VerifyOnly must not change the stream");
    }

    [TestMethod]
    public async Task CreateOrUpdate_ChangesDuplicateWindow_AndKeepsUnmodelledFields()
    {
        var name = TestNames.Unique("PROV_UPDATE");
        var subject = $"{TestNames.Unique("update")}.>";
        await CreateByHand(new StreamConfig(name, [subject])
        {
            DuplicateWindow = TimeSpan.FromMinutes(2),
            AllowDirect = true,
            AllowMsgTTL = true,
            MaxMsgsPerSubject = 5,
            Description = "owner settings"
        });

        var definition = new StreamDefinition
        {
            Name = name,
            Subjects = [subject],
            DuplicateWindow = TimeSpan.FromMinutes(10)
        };
        await using var host = await TestHost.StartAsync(o =>
        {
            o.StreamDefinitions = [definition];
            o.StreamProvisioning = StreamProvisioning.CreateOrUpdate;
        });
        await host.WaitUntilStartedAsync();

        var config = await GetConfig(host, name);
        Assert.AreEqual(TimeSpan.FromMinutes(10), config.DuplicateWindow);
        Assert.IsTrue(config.AllowDirect);
        Assert.IsTrue(config.AllowMsgTTL);
        Assert.AreEqual(5, config.MaxMsgsPerSubject);
        Assert.AreEqual("owner settings", config.Description);
        Assert.IsNull(host.Status.Get($"stream:{name}")!.Warning);
    }

    [TestMethod]
    public async Task CreateOrUpdate_StorageChange_IsFaulted_AndWaitUntilStartedThrowsAtOnce()
    {
        var name = TestNames.Unique("PROV_STORAGE");
        var subject = $"{TestNames.Unique("storage")}.>";
        await CreateByHand(new StreamConfig(name, [subject]) { Storage = StreamConfigStorage.File });

        var host = TestHost.Create(o =>
        {
            o.StreamDefinitions = [new StreamDefinition { Name = name, Subjects = [subject], Storage = StreamConfigStorage.Memory }];
            o.StreamProvisioning = StreamProvisioning.CreateOrUpdate;
            o.WaitUntilStarted = true;
            o.StartTimeout = TimeSpan.FromSeconds(60);
        });
        await using var _ = host;

        var started = DateTime.UtcNow;
        var error = await Assert.ThrowsAsync<InvalidOperationException>(host.StartHostAsync);

        Assert.IsTrue(DateTime.UtcNow - started < TimeSpan.FromSeconds(15), "Should throw on the fault, not at StartTimeout");
        StringAssert.Contains(error.Message, $"stream:{name}");
        var component = host.Status.Get($"stream:{name}")!;
        Assert.AreEqual(ComponentState.Faulted, component.State);
        Assert.AreEqual(StreamConfigStorage.File, (await GetConfig(host, name)).Storage);
    }

    [TestMethod]
    public async Task CreateOrUpdate_NeverTurnsOffSchedules_OrRemovesTheHolderSubject()
    {
        var name = TestNames.Unique("PROV_KEEP");
        var subject = $"{TestNames.Unique("keep")}.>";
        var holder = $"_sched.{name}.>";
        await CreateByHand(new StreamConfig(name, [subject, holder])
        {
            Retention = StreamConfigRetention.Workqueue,
            AllowMsgSchedules = true
        });

        var definition = new StreamDefinition
        {
            Name = name,
            Subjects = [subject],
            Retention = StreamConfigRetention.Workqueue,
            DuplicateWindow = TimeSpan.FromSeconds(30)
        };
        await using var host = await TestHost.StartAsync(o =>
        {
            o.StreamDefinitions = [definition];
            o.StreamProvisioning = StreamProvisioning.CreateOrUpdate;
        });
        await host.WaitUntilStartedAsync();

        var config = await GetConfig(host, name);
        Assert.AreEqual(TimeSpan.FromSeconds(30), config.DuplicateWindow, "The update itself is applied");
        Assert.IsTrue(config.AllowMsgSchedules);
        CollectionAssert.Contains(config.Subjects!.ToArray(), holder);
        StringAssert.Contains(host.Status.Get($"stream:{name}")!.Warning, "AllowMsgSchedules");
    }

    [TestMethod]
    public async Task CreateOrUpdate_DoesNotLowerTheMaxAgeOfAScheduleStream_SoPendingSchedulesSurvive()
    {
        var name = TestNames.Unique("PROV_MAXAGE");
        var root = TestNames.Unique("maxage");
        await CreateByHand(StreamDefinition.Scheduled(name, $"{root}.>").ToStreamConfig());

        await using (var connection = new NatsConnection(new NatsOpts
                     {
                         Url = NatsContainerFixture.NatsUrl,
                         SerializerRegistry = NatsJsonSerializerRegistry.Default
                     }))
        {
            var js = new NatsJSContext(connection);
            var publisher = new NatsPublisher(connection, js, new NatsMessagingOptions(), NullLogger<NatsPublisher>.Instance);
            await publisher.ScheduleAsync($"{root}.remind", new TestMessage("days ahead"), DateTimeOffset.UtcNow.AddSeconds(20),
                new ScheduleOptions { Key = "k" });
        }

        await Task.Delay(TimeSpan.FromSeconds(3));
        var lowered = StreamDefinition.Scheduled(name, $"{root}.>");
        lowered.MaxAge = 2;
        await using var host = await TestHost.StartAsync(o =>
        {
            o.StreamDefinitions = [lowered];
            o.StreamProvisioning = StreamProvisioning.CreateOrUpdate;
        });
        await host.WaitUntilStartedAsync();

        Assert.AreEqual(TimeSpan.Zero, (await GetConfig(host, name)).MaxAge, "MaxAge stays unlimited");
        var component = host.Status.Get($"stream:{name}")!;
        Assert.AreEqual(ComponentState.Running, component.State);
        StringAssert.Contains(component.Warning, "MaxAge kept at unlimited, not lowered to 00:00:02");
        StringAssert.Contains(component.Warning, "1 pending");

        await Task.Delay(TimeSpan.FromSeconds(1));
        var holders = await host.JetStream.GetStreamAsync(name, new StreamInfoRequest { SubjectsFilter = $"_sched.{name}.>" });
        Assert.AreEqual<long?>(1, holders.Info.State.Subjects?.Values.Single(), "The pending schedule is still there");
    }

    [TestMethod]
    [DataRow(StreamProvisioning.CreateIfMissing)]
    [DataRow(StreamProvisioning.CreateOrUpdate)]
    public async Task TurningOnSchedules_WithAnUnfilteredConsumerOnTheStream_IsFaulted_AndChangesNothing(
        StreamProvisioning mode)
    {
        var name = TestNames.Unique("PROV_HOLDERS");
        var subject = $"{TestNames.Unique("holders")}.>";
        var unfiltered = TestNames.Unique("unfiltered");
        await CreateByHand(new StreamConfig(name, [subject]) { Retention = StreamConfigRetention.Workqueue });
        await using (var connection = TestHost.CreateConnection())
        {
            await new NatsJSContext(connection).CreateOrUpdateConsumerAsync(name, new ConsumerConfig(unfiltered)
            {
                AckPolicy = ConsumerConfigAckPolicy.Explicit
            });
        }

        await using var host = await TestHost.StartAsync(o =>
        {
            o.StreamDefinitions = [StreamDefinition.Scheduled(name, subject)];
            o.StreamProvisioning = mode;
        });

        var faulted = await host.WaitForStateAsync($"stream:{name}", ComponentState.Faulted);
        StringAssert.Contains(faulted.Error, $"{unfiltered} (no filter)");
        var config = await GetConfig(host, name);
        Assert.IsFalse(config.AllowMsgSchedules, "An unfiltered consumer would take every holder at once");
        CollectionAssert.AreEqual(new[] { subject }, config.Subjects!.ToArray());
    }

    [TestMethod]
    public async Task TurningOnSchedules_WithFilteredConsumersOnTheStream_Works()
    {
        var name = TestNames.Unique("PROV_FILTERED");
        var root = TestNames.Unique("filtered");
        await CreateByHand(new StreamConfig(name, [$"{root}.>"]) { Retention = StreamConfigRetention.Workqueue });
        await using (var connection = TestHost.CreateConnection())
        {
            await new NatsJSContext(connection).CreateOrUpdateConsumerAsync(name, new ConsumerConfig(TestNames.Unique("filtered"))
            {
                FilterSubject = $"{root}.a",
                AckPolicy = ConsumerConfigAckPolicy.Explicit
            });
        }

        await using var host = await TestHost.StartAsync(o => o.StreamDefinitions = [StreamDefinition.Scheduled(name, $"{root}.>")]);
        await host.WaitUntilStartedAsync();

        Assert.IsTrue((await GetConfig(host, name)).AllowMsgSchedules);
    }

    [TestMethod]
    public async Task CallersStreamDefinitions_AreNotMutated()
    {
        var name = TestNames.Unique("PROV_SHARED");
        var subject = $"{TestNames.Unique("shared")}.>";
        var shared = new List<StreamDefinition> { StreamDefinition.Scheduled(name, subject) };
        var subjectsBefore = shared[0].Subjects;

        await using var host = await TestHost.StartAsync(o => o.StreamDefinitions = shared);
        await host.WaitUntilStartedAsync();

        Assert.HasCount(1, shared);
        Assert.AreSame(subjectsBefore, shared[0].Subjects);
        CollectionAssert.AreEqual(new[] { subject }, shared[0].Subjects);
        Assert.AreNotSame(shared, host.Options.StreamDefinitions);
        Assert.AreNotSame(shared[0], host.Options.StreamDefinitions[0]);

        var config = await GetConfig(host, name);
        CollectionAssert.Contains(config.Subjects!.ToArray(), $"_sched.{name}.>");
    }

    [TestMethod]
    public void ScheduleDefinition_WithInterestRetention_ThrowsAtRegistration()
    {
        var definition = StreamDefinition.Scheduled("BAD", "bad.>");
        definition.Retention = StreamConfigRetention.Interest;

        var error = Assert.ThrowsExactly<ArgumentException>(() => Register(o => o.StreamDefinitions = [definition]));
        StringAssert.Contains(error.Message, "Interest");
    }

    [TestMethod]
    public void ScheduleDefinition_WithMaxMsgs_ThrowsAtRegistration()
    {
        var definition = StreamDefinition.Scheduled("BAD", "bad.>");
        definition.MaxMsgs = 100;

        var error = Assert.ThrowsExactly<ArgumentException>(() => Register(o => o.StreamDefinitions = [definition]));
        StringAssert.Contains(error.Message, "MaxMsgs");
    }

    [TestMethod]
    public void ScheduleDefinition_WithMaxBytes_ThrowsAtRegistration()
    {
        var definition = StreamDefinition.Scheduled("BAD", "bad.>");
        definition.MaxBytes = 1_000_000;

        var error = Assert.ThrowsExactly<ArgumentException>(() => Register(o => o.StreamDefinitions = [definition]));
        StringAssert.Contains(error.Message, "MaxBytes");
    }

    [TestMethod]
    public void ScheduleDefinition_WithDiscardNew_ThrowsAtRegistration()
    {
        var definition = StreamDefinition.Scheduled("BAD", "bad.>");
        definition.Discard = StreamConfigDiscard.New;

        var error = Assert.ThrowsExactly<ArgumentException>(() => Register(o => o.StreamDefinitions = [definition]));
        StringAssert.Contains(error.Message, "Discard");
    }

    [TestMethod]
    public void DeadLetterPrefix_NotCoveredByDeadLetterStream_ThrowsAtRegistration()
    {
        var error = Assert.ThrowsExactly<ArgumentException>(() => Register(o =>
        {
            o.DeadLetterStream = StreamDefinition.DeadLetter("DEAD", "dead.>");
            o.DeadLetterSubjectPrefix = "dlq";
        }));
        StringAssert.Contains(error.Message, "dlq.>");
    }

    [TestMethod]
    public void ExplicitDeadLetterSubject_NotCoveredByDeadLetterStream_ThrowsAtRegistration()
    {
        var error = Assert.ThrowsExactly<ArgumentException>(() => Register(o =>
            o.HandlerTypes = [typeof(UncoveredDeadLetterConsumer)]));
        StringAssert.Contains(error.Message, "elsewhere.uncovered");
    }

    [TestMethod]
    public void DeadLetterStreamNull_SkipsTheCoverageCheck()
    {
        Register(o =>
        {
            o.DeadLetterStream = null;
            o.HandlerTypes = [typeof(UncoveredDeadLetterConsumer)];
        });
    }

    [TestMethod]
    public void ConsumerWithoutQueueGroup_ThrowsAtRegistration()
    {
        var error = Assert.ThrowsExactly<InvalidOperationException>(() => Register(o =>
            o.HandlerTypes = [typeof(NoQueueGroupConsumer)]));
        StringAssert.Contains(error.Message, "queue group");
    }

    [TestMethod]
    public void ServiceName_IsTheQueueGroupFallback()
    {
        var services = Register(o =>
        {
            o.ServiceName = "billing";
            o.HandlerTypes = [typeof(NoQueueGroupConsumer)];
        });

        var registration = services.BuildServiceProvider().GetRequiredService<NatsMessagingRegistration>();
        Assert.AreEqual("billing_NoQueueGroupConsumer", registration.Consumers.Single().Durable);
    }

    [TestMethod]
    public void OverlappingDeclaredStreams_ThrowAtRegistration()
    {
        Assert.ThrowsExactly<ArgumentException>(() => Register(o => o.StreamDefinitions =
        [
            new StreamDefinition { Name = "A", Subjects = ["orders.>"] },
            new StreamDefinition { Name = "B", Subjects = ["orders.created"] }
        ]));
    }

    private static IServiceCollection Register(Action<NatsMessagingOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNatsMessaging(o =>
        {
            o.NatsUrl = "nats://127.0.0.1:1";
            configure(o);
        });
        return services;
    }

    private static async Task CreateByHand(StreamConfig config)
    {
        await using var connection = TestHost.CreateConnection();
        await new NatsJSContext(connection).CreateStreamAsync(config);
    }

    private static async Task<StreamConfig> GetConfig(TestHost host, string name) =>
        (await host.JetStream.GetStreamAsync(name)).Info.Config;
}
