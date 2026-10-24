using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// A real <see cref="IHost"/> with <c>AddNatsMessaging</c> against the test server. Defaults that differ from the
/// library: <see cref="FaultAction.Report"/>, a unique dead-letter stream and prefix, and a short shutdown.
/// A health check named "nats" covers every component.
/// </summary>
public sealed class TestHost : IAsyncDisposable
{
    public const string HealthCheckName = "nats";

    private bool _started;

    private TestHost(IHost host)
    {
        Host = host;
    }

    public IHost Host { get; }

    public IServiceProvider Services => Host.Services;

    public NatsMessagingStatus Status => Services.GetRequiredService<NatsMessagingStatus>();

    public NatsMessagingOptions Options => Services.GetRequiredService<NatsMessagingOptions>();

    public NatsJSContext JetStream => Services.GetRequiredService<NatsJSContext>();

    public IPublisher Publisher => Services.GetRequiredService<IPublisher>();

    /// <summary>
    /// The unique dead-letter stream TestHost gives every host.
    /// </summary>
    public string DeadLetterStream => Options.DeadLetterStream!.Name;

    /// <summary>
    /// The default dead-letter subject of a message published to <paramref name="subject"/>.
    /// </summary>
    public string DeadLetterSubject(string subject) => $"{Options.DeadLetterSubjectPrefix}.{subject}";

    /// <summary>
    /// Collects everything written to this host's dead-letter stream.
    /// </summary>
    public Task<StreamCollector> CollectDeadLettersAsync() =>
        StreamCollector.StartAsync(JetStream, DeadLetterStream, $"{Options.DeadLetterSubjectPrefix}.>");

    public async Task<ConsumerInfo> ConsumerInfoAsync(string stream, string durable) =>
        (await JetStream.GetConsumerAsync(stream, durable)).Info;

    public static TestHost Create(Action<NatsMessagingOptions>? configure = null,
        Action<IServiceCollection>? configureServices = null)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddLogging();
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(10));

        builder.Services.AddNatsMessaging(o =>
        {
            var prefix = TestNames.Unique("dlq");
            o.NatsUrl = NatsContainerFixture.NatsUrl;
            o.OnComponentFaulted = FaultAction.Report;
            o.ShutdownTimeout = TimeSpan.FromSeconds(5);
            o.DeadLetterSubjectPrefix = prefix;
            o.DeadLetterStream = StreamDefinition.DeadLetter(TestNames.Unique("DLQ"), $"{prefix}.>");
            configure?.Invoke(o);
        });

        builder.Services.AddHealthChecks().AddNatsMessagingCheck(HealthCheckName);
        configureServices?.Invoke(builder.Services);

        return new TestHost(builder.Build());
    }

    public static async Task<TestHost> StartAsync(Action<NatsMessagingOptions>? configure = null,
        Action<IServiceCollection>? configureServices = null)
    {
        var host = Create(configure, configureServices);
        await host.StartHostAsync();
        return host;
    }

    public async Task StartHostAsync()
    {
        _started = true;
        await Host.StartAsync();
    }

    public Task WaitUntilStartedAsync(TimeSpan? timeout = null) =>
        Status.WaitUntilStartedAsync().WaitAsync(timeout ?? TimeSpan.FromSeconds(20));

    /// <summary>
    /// Waits until <paramref name="component"/> is in <paramref name="state"/>, and returns its status.
    /// </summary>
    public async Task<ComponentStatus> WaitForStateAsync(string component, ComponentState state, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(20));
        while (true)
        {
            var current = Status.Get(component) ?? throw new InvalidOperationException($"No component {component}");
            if (current.State == state)
            {
                return current;
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException(
                    $"{component} is {current.State} ({current.Error}), expected {state}");
            }

            await Task.Delay(50);
        }
    }

    public async Task<HealthReportEntry> CheckHealthAsync(string name = HealthCheckName)
    {
        var report = await Services.GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(r => r.Name == name);
        return report.Entries[name];
    }

    public async ValueTask DisposeAsync()
    {
        if (_started)
        {
            try
            {
                await Host.StopAsync();
            }
            catch (OperationCanceledException)
            {
            }
        }

        if (Host is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync();
        }
        else
        {
            Host.Dispose();
        }
    }

    /// <summary>
    /// A connection to the test server for setting up streams by hand.
    /// </summary>
    public static NatsConnection CreateConnection(string? url = null) => new(new NatsOpts
    {
        Url = url ?? NatsContainerFixture.NatsUrl
    });
}
