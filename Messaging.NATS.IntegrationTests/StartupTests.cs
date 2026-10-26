using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class StartupTests
{
    [TestMethod]
    public async Task ServerStartedAfterTheHost_ComponentsRunAndHealthIsHealthy()
    {
        var port = TestEnvironment.RequireSparePort();
        var definition = new StreamDefinition { Name = TestNames.Unique("LATE"), Subjects = [$"{TestNames.Unique("late")}.>"] };

        await using var host = await TestHost.StartAsync(o =>
        {
            o.NatsUrl = $"nats://127.0.0.1:{port}";
            o.StreamDefinitions = [definition];
        });

        await Task.Delay(1000);
        Assert.AreEqual(ComponentState.Starting, host.Status.Get("connection")!.State);
        Assert.IsFalse(host.Status.IsStarted);
        Assert.AreEqual(HealthStatus.Unhealthy, (await host.CheckHealthAsync()).Status);

        await using var server = await NatsServerProcess.StartAsync(port);

        await host.WaitUntilStartedAsync(TimeSpan.FromSeconds(30));
        Assert.IsTrue(host.Status.Components.All(c => c.State == ComponentState.Running));
        Assert.AreEqual(HealthStatus.Healthy, (await host.CheckHealthAsync()).Status);
    }

    [TestMethod]
    public async Task RejectedCredentials_OnTheFirstConnect_FaultTheConnection_AndStopTheApplication()
    {
        await using var server = await NatsServerProcess.StartAsync(TestEnvironment.UnusedPort(), null, true,
            "--user", "good", "--pass", "good");
        var host = TestHost.Create(o =>
        {
            o.NatsUrl = $"nats://bad:bad@127.0.0.1:{server.Port}";
            o.OnComponentFaulted = FaultAction.StopApplication;
        });
        await using var _ = host;
        var stopping = new TaskCompletionSource();
        host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(() => stopping.TrySetResult());

        await host.StartHostAsync();

        // NATS.Net gives up after the same authentication error twice; every later connect fails at once.
        await stopping.Task.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.AreEqual(NatsConnectionState.Failed, host.Services.GetRequiredService<INatsConnection>().ConnectionState);
        Assert.AreEqual(ComponentState.Faulted, host.Status.Get("connection")!.State);
        StringAssert.Contains(host.Status.Get("connection")!.Error, "Failed state");
    }

    [TestMethod]
    public async Task ConnectionThatFailsForGood_AfterStart_IsFaulted_AndUnhealthy()
    {
        var port = TestEnvironment.UnusedPort();
        var server = await NatsServerProcess.StartAsync(port, null, true, "--user", "app", "--pass", "p1");
        try
        {
            await using var host = await TestHost.StartAsync(o => o.NatsUrl = $"nats://app:p1@127.0.0.1:{port}");
            await host.WaitUntilStartedAsync();

            // The password is rotated: every reconnect is rejected, and NATS.Net stops trying.
            await server.DisposeAsync();
            server = await NatsServerProcess.StartAsync(port, null, true, "--user", "app", "--pass", "p2");

            var faulted = await host.WaitForStateAsync("connection", ComponentState.Faulted, TimeSpan.FromSeconds(40));
            StringAssert.Contains(faulted.Error, "Failed state");
            var health = await host.CheckHealthAsync();
            Assert.AreEqual(HealthStatus.Unhealthy, health.Status, health.Description);
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [TestMethod]
    public async Task WaitUntilStarted_WithNoServer_ThrowsAfterStartTimeout()
    {
        var port = TestEnvironment.UnusedPort();
        var host = TestHost.Create(o =>
        {
            o.NatsUrl = $"nats://127.0.0.1:{port}";
            o.WaitUntilStarted = true;
            o.StartTimeout = TimeSpan.FromSeconds(2);
        });
        await using var _ = host;

        var started = DateTime.UtcNow;
        var error = await Assert.ThrowsAsync<TimeoutException>(host.StartHostAsync);
        var elapsed = DateTime.UtcNow - started;

        Assert.IsTrue(elapsed >= TimeSpan.FromSeconds(1.9) && elapsed < TimeSpan.FromSeconds(10), elapsed.ToString());
        StringAssert.Contains(error.Message, "connection");
    }

    [TestMethod]
    public async Task StopApplication_FaultedComponent_StopsTheHost()
    {
        var name = await CreateFileStream();
        var host = TestHost.Create(o =>
        {
            o.StreamDefinitions = [MemoryDefinition(name)];
            o.StreamProvisioning = StreamProvisioning.CreateOrUpdate;
            o.OnComponentFaulted = FaultAction.StopApplication;
        });
        await using var _ = host;

        var stopping = new TaskCompletionSource();
        host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(() => stopping.TrySetResult());

        await host.StartHostAsync();

        await stopping.Task.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.AreEqual(ComponentState.Faulted, host.Status.Get($"stream:{name}")!.State);
    }

    [TestMethod]
    public async Task Report_FaultedComponent_RetriesAfterFaultedRetryInterval_AndRecovers()
    {
        var name = await CreateFileStream();
        await using var host = await TestHost.StartAsync(o =>
        {
            o.StreamDefinitions = [MemoryDefinition(name)];
            o.StreamProvisioning = StreamProvisioning.CreateOrUpdate;
            o.OnComponentFaulted = FaultAction.Report;
            o.FaultedRetryInterval = TimeSpan.FromSeconds(1);
        });

        var faulted = await host.WaitForStateAsync($"stream:{name}", ComponentState.Faulted);
        StringAssert.Contains(faulted.Error, "err_code");
        Assert.IsFalse(host.Host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.IsCancellationRequested);

        await using (var connection = TestHost.CreateConnection())
        {
            await new NatsJSContext(connection).DeleteStreamAsync(name);
        }

        await host.WaitForStateAsync($"stream:{name}", ComponentState.Running, TimeSpan.FromSeconds(15));
        Assert.AreEqual(StreamConfigStorage.Memory, (await host.JetStream.GetStreamAsync(name)).Info.Config.Storage);
    }

    [TestMethod]
    public async Task Publisher_IsOneInstanceAcrossScopes()
    {
        await using var host = TestHost.Create();

        using var first = host.Services.CreateScope();
        using var second = host.Services.CreateScope();

        Assert.AreSame(
            first.ServiceProvider.GetRequiredService<IPublisher>(),
            second.ServiceProvider.GetRequiredService<IPublisher>());
    }

    [TestMethod]
    public async Task Health_IsUnhealthyBeforeStart_AndHealthyAfter()
    {
        var definition = new StreamDefinition { Name = TestNames.Unique("HEALTH"), Subjects = [$"{TestNames.Unique("health")}.>"] };
        await using var host = TestHost.Create(o => o.StreamDefinitions = [definition]);

        Assert.AreEqual(HealthStatus.Unhealthy, (await host.CheckHealthAsync()).Status);

        await host.StartHostAsync();
        await host.WaitUntilStartedAsync();

        var health = await host.CheckHealthAsync();
        Assert.AreEqual(HealthStatus.Healthy, health.Status);
        Assert.IsTrue(health.Data.ContainsKey("connection"));
        Assert.IsTrue(health.Data.ContainsKey($"stream:{definition.Name}"));
    }

    [TestMethod]
    public async Task ConnectionOnlyFilter_StaysHealthy_WhileAStreamIsFaulted()
    {
        var name = await CreateFileStream();
        await using var host = await TestHost.StartAsync(
            o =>
            {
                o.StreamDefinitions = [MemoryDefinition(name)];
                o.StreamProvisioning = StreamProvisioning.CreateOrUpdate;
            },
            services => services.AddHealthChecks()
                .AddNatsMessagingCheck("nats-connection", componentFilter: NatsHealthFilters.ConnectionOnly)
                .AddNatsMessagingCheck("nats-no-consumers", componentFilter: NatsHealthFilters.ExcludeConsumers));

        await host.WaitForStateAsync($"stream:{name}", ComponentState.Faulted);

        var connectionOnly = await host.CheckHealthAsync("nats-connection");
        Assert.AreEqual(HealthStatus.Healthy, connectionOnly.Status);
        Assert.IsFalse(connectionOnly.Data.ContainsKey($"stream:{name}"));

        Assert.AreEqual(HealthStatus.Unhealthy, (await host.CheckHealthAsync()).Status);
        Assert.AreEqual(HealthStatus.Unhealthy, (await host.CheckHealthAsync("nats-no-consumers")).Status);
    }

    [TestMethod]
    public async Task NatsConnection_KeepsTheHostLoggerFactory_AndTakesNameAndRetryOnInitialConnect()
    {
        await using var host = TestHost.Create(o =>
        {
            o.ServiceName = "billing-worker";
            o.RetryOnInitialConnect = false;
        });

        var connection = host.Services.GetRequiredService<NatsConnection>();
        Assert.AreSame(connection, host.Services.GetRequiredService<INatsConnection>());
        Assert.AreSame(host.Services.GetRequiredService<ILoggerFactory>(), connection.Opts.LoggerFactory);
        Assert.AreEqual("billing-worker", connection.Opts.Name);
        Assert.IsFalse(connection.Opts.RetryOnInitialConnect);
    }

    [TestMethod]
    public async Task ConfigureNatsOpts_RunsLast_AndKeepsTheLibrarySettings()
    {
        NatsOpts? seen = null;
        await using var host = TestHost.Create(o =>
        {
            o.ServiceName = "billing-worker";
            o.ConfigureNatsOpts = opts =>
            {
                seen = opts;
                return opts with { ReconnectWaitMin = TimeSpan.FromMilliseconds(100) };
            };
        });

        var connection = host.Services.GetRequiredService<NatsConnection>();
        Assert.AreEqual("billing-worker", seen!.Name);
        Assert.AreEqual(TimeSpan.FromMilliseconds(100), connection.Opts.ReconnectWaitMin);
        Assert.AreEqual("billing-worker", connection.Opts.Name);
        Assert.AreSame(host.Services.GetRequiredService<ILoggerFactory>(), connection.Opts.LoggerFactory);
    }

    [TestMethod]
    public async Task StopBeforeTheFirstConnect_LeavesNoUnobservedTaskException()
    {
        var disposedConnects = new List<Exception>();
        void OnUnobserved(object? sender, UnobservedTaskExceptionEventArgs e)
        {
            lock (disposedConnects)
            {
                disposedConnects.AddRange(e.Exception.InnerExceptions.Where(x =>
                    x is ObjectDisposedException && x.StackTrace?.Contains("NatsConnection") == true));
            }
        }

        TaskScheduler.UnobservedTaskException += OnUnobserved;
        try
        {
            var port = TestEnvironment.UnusedPort();
            for (var i = 0; i < 3; i++)
            {
                await using var host = await TestHost.StartAsync(o => o.NatsUrl = $"nats://127.0.0.1:{port}");
                await Task.Delay(300);
            }

            for (var i = 0; i < 5; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                await Task.Delay(200);
            }

            lock (disposedConnects)
            {
                Assert.IsEmpty(disposedConnects, string.Join("\n", disposedConnects));
            }
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= OnUnobserved;
        }
    }

    [TestMethod]
    public async Task HostedService_DisposedBeforeItIsStopped_StopsWithoutThrowing()
    {
        var port = TestEnvironment.UnusedPort();
        await using var host = await TestHost.StartAsync(o => o.NatsUrl = $"nats://127.0.0.1:{port}");
        var hosted = host.Services.GetServices<IHostedService>().OfType<NatsMessagingHostedService>().Single();

        // WebApplicationFactory can dispose the services before it stops the host
        hosted.Dispose();
        await hosted.StopAsync(CancellationToken.None);
        hosted.Dispose();

        Assert.IsTrue(host.Status.Components.All(c => c.State == ComponentState.Stopped));
    }

    [TestMethod]
    public async Task RetryOnInitialConnectFalse_WithNoServer_FaultsTheConnection()
    {
        var port = TestEnvironment.UnusedPort();
        await using var host = await TestHost.StartAsync(o =>
        {
            o.NatsUrl = $"nats://127.0.0.1:{port}";
            o.RetryOnInitialConnect = false;
        });

        var connection = await host.WaitForStateAsync("connection", ComponentState.Faulted);
        StringAssert.Contains(connection.Error, "RetryOnInitialConnect");
    }

    private static StreamDefinition MemoryDefinition(string name) =>
        new() { Name = name, Subjects = [$"{name.ToLowerInvariant()}.>"], Storage = StreamConfigStorage.Memory };

    private static async Task<string> CreateFileStream()
    {
        var name = TestNames.Unique("STORAGE");
        await using var connection = TestHost.CreateConnection();
        await new NatsJSContext(connection).CreateStreamAsync(
            new StreamConfig(name, [$"{name.ToLowerInvariant()}.>"]) { Storage = StreamConfigStorage.File });
        return name;
    }
}
