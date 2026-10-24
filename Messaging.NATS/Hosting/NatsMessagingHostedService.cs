using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// Starts messaging in the background: connect, provision the declared streams and the dead-letter stream,
/// then start consumers and reply handlers. Each component runs in its own <see cref="RetryLoop"/> and reports
/// to <see cref="NatsMessagingStatus"/>.
/// </summary>
internal sealed class NatsMessagingHostedService : IHostedService, IDisposable
{
    // Drain waits ShutdownTimeout for handlers, then cancels them and waits for their hand-off NAKs.
    private static readonly TimeSpan DrainGrace = ConsumerWorker.HandoffGrace + TimeSpan.FromSeconds(1);

    // How often the connection is checked for the Failed state, which NATS.Net never leaves.
    internal static readonly TimeSpan ConnectionWatchInterval = TimeSpan.FromSeconds(1);

    private readonly NatsMessagingOptions _options;
    private readonly NatsMessagingRegistration _registration;
    private readonly NatsConnection _connection;
    private readonly NatsJSContext _js;
    private readonly NatsMessagingStatus _status;
    private readonly StreamProvisioner _provisioner;
    private readonly IServiceScopeFactory _scopes;
    private readonly DeadLetterWriter _deadLetters;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<NatsMessagingHostedService> _logger;
    private readonly IHostApplicationLifetime? _lifetime;
    private readonly Dictionary<string, StreamDefinition> _declaredStreams;

    private readonly List<ConsumerWorker> _workers = new();

    // Declared streams a consumer found missing after they were provisioned, with one gate per stream so the
    // consumers on it provision it once.
    private readonly ConcurrentDictionary<string, bool> _lostStreams = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _recoveryGates = new();

    private CancellationTokenSource? _stopping;
    private Task _run = Task.CompletedTask;

    public NatsMessagingHostedService(
        NatsMessagingOptions options,
        NatsMessagingRegistration registration,
        NatsConnection connection,
        NatsJSContext js,
        NatsMessagingStatus status,
        StreamProvisioner provisioner,
        ILoggerFactory loggerFactory,
        IServiceProvider services)
    {
        _options = options;
        _registration = registration;
        _connection = connection;
        _js = js;
        _status = status;
        _provisioner = provisioner;
        _scopes = services.GetRequiredService<IServiceScopeFactory>();
        _loggerFactory = loggerFactory;
        _deadLetters = new DeadLetterWriter(connection, options, loggerFactory.CreateLogger<DeadLetterWriter>());
        _logger = loggerFactory.CreateLogger<NatsMessagingHostedService>();
        _lifetime = services.GetService<IHostApplicationLifetime>();
        _declaredStreams = options.StreamDefinitions.ToDictionary(d => d.Name);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _stopping = new CancellationTokenSource();
        var token = _stopping.Token;
        _run = Task.Run(() => RunAsync(token), CancellationToken.None);

        if (!_options.WaitUntilStarted)
        {
            return;
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_options.StartTimeout);

        try
        {
            await _status.WaitUntilStartedAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            await _stopping.CancelAsync();
            var pending = _status.Components.Where(c => !c.HasStarted)
                .Select(c => c.Error == null ? $"{c.Name} ({c.State})" : $"{c.Name} ({c.State}: {c.Error})");
            throw new TimeoutException(
                $"NATS messaging did not start within {_options.StartTimeout}. Not running: {string.Join(", ", pending)}");
        }
        catch
        {
            await _stopping.CancelAsync();
            throw;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_stopping == null)
        {
            return;
        }

        await _stopping.CancelAsync();

        try
        {
            await _run.WaitAsync(_options.ShutdownTimeout + DrainGrace, cancellationToken);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("NATS messaging did not stop within {ShutdownTimeout}", _options.ShutdownTimeout);
        }
        catch (OperationCanceledException)
        {
            // The host's own shutdown budget ended first. Cancel the running handlers now and wait a short grace
            // without the host token, so their hand-off NAKs reach the server before the connection is disposed.
            _logger.LogWarning("NATS messaging shutdown was cut short by the host; handing off the running messages");
            CancelHandlers();
            try
            {
                await _run.WaitAsync(DrainGrace);
            }
            catch (TimeoutException)
            {
                _logger.LogWarning("NATS messaging did not hand off its running messages within {Grace}", DrainGrace);
            }
        }

        foreach (var component in _status.Components)
        {
            _status.SetStopped(component.Name);
        }

        NatsMessagingTelemetry.ClearOrphans(this);
    }

    public void Dispose()
    {
        _stopping?.Cancel();
        _stopping?.Dispose();
    }

    private async Task RunAsync(CancellationToken token)
    {
        try
        {
            await Loop(NatsMessagingRegistration.ConnectionComponent).RunAsync(ConnectAsync, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            var loops = new List<Task> { Task.Run(() => WatchConnectionAsync(token), CancellationToken.None) };

            foreach (var definition in _options.StreamDefinitions)
            {
                loops.Add(Start(NatsMessagingRegistration.StreamComponent(definition.Name),
                    (attempt, t) => ProvisionAsync(definition, _options.StreamProvisioning, attempt, t), token));
            }

            if (_registration.Consumers.Count > 0)
            {
                loops.Add(Start(NatsMessagingRegistration.DeadLetterComponent, EnsureDeadLetterStreamAsync, token));
            }

            foreach (var consumer in _registration.Consumers)
            {
                loops.Add(Task.Run(() => RunConsumerLoopAsync(consumer, token), CancellationToken.None));
            }

            foreach (var stream in _registration.Consumers.Select(c => c.Stream).Distinct())
            {
                loops.Add(Task.Run(() => ReportOrphansAsync(stream, token), CancellationToken.None));
            }

            foreach (var reply in _registration.ReplyHandlers)
            {
                var worker = new ReplyHandlerWorker(reply, _connection, _scopes,
                    _loggerFactory.CreateLogger<ReplyHandlerWorker>());
                loops.Add(Start(reply.ComponentName, worker.RunAsync, token));
            }

            await Task.WhenAll(loops);
        }
        catch (Exception e) when (!token.IsCancellationRequested)
        {
            _logger.LogCritical(e, "NATS messaging startup pipeline failed");
        }
    }

    private Task Start(string component, Func<RetryAttempt, CancellationToken, Task> attempt, CancellationToken token) =>
        Task.Run(() => Loop(component).RunAsync(attempt, token), CancellationToken.None);

    private RetryLoop Loop(string component) =>
        new(component, _status, _options.FaultedRetryInterval, OnFaulted, _loggerFactory.CreateLogger<RetryLoop>());

    private async Task ConnectAsync(RetryAttempt attempt, CancellationToken token)
    {
        if (_connection.ConnectionState == NatsConnectionState.Open)
        {
            return;
        }

        try
        {
            await _connection.ConnectAsync().AsTask().WaitAsync(token);
        }
        catch (NatsException e) when (_connection.ConnectionState == NatsConnectionState.Failed)
        {
            // NATS.Net stops for good after the same authentication error twice in a row: every later ConnectAsync
            // throws at once, even when the credentials become valid again. Only a new process can connect.
            throw new NatsMessagingConfigurationException(FailedConnectionError(e), e);
        }
        catch (NatsException e) when (!_options.RetryOnInitialConnect)
        {
            throw new NatsMessagingConfigurationException(
                $"Could not connect to {_options.NatsUrl}, and RetryOnInitialConnect is false", e);
        }
    }

    private string FailedConnectionError(Exception? cause) =>
        $"The NATS connection to {_options.NatsUrl} is in the Failed state ({cause?.Message ?? "it stopped reconnecting"}). " +
        "NATS.Net gives up after the server rejects the credentials twice in a row and never reconnects, so the " +
        "process must be restarted with valid credentials";

    /// <summary>
    /// After the first connect, a connection that NATS.Net gives up on (the Failed state) never comes back, and every
    /// consumer and publish stalls. It marks the connection component Faulted and runs the fault action once, so
    /// <see cref="FaultAction.StopApplication"/> restarts the process with a new connection.
    /// </summary>
    private async Task WatchConnectionAsync(CancellationToken token)
    {
        try
        {
            while (_connection.ConnectionState != NatsConnectionState.Failed)
            {
                await Task.Delay(ConnectionWatchInterval, token);
            }
        }
        catch (OperationCanceledException)
        {
            return;
        }

        var error = FailedConnectionError(null);
        _status.SetFaulted(NatsMessagingRegistration.ConnectionComponent, error);
        _logger.LogError("NATS messaging component {Component} is faulted: {Error}",
            NatsMessagingRegistration.ConnectionComponent, error);
        OnFaulted(NatsMessagingRegistration.ConnectionComponent, new NatsMessagingConfigurationException(error));
    }

    private async Task ProvisionAsync(StreamDefinition definition, StreamProvisioning mode, RetryAttempt attempt,
        CancellationToken token)
    {
        var warning = await _provisioner.ProvisionAsync(definition, mode, token);
        attempt.SetWarning(warning);
    }

    private async Task EnsureDeadLetterStreamAsync(RetryAttempt attempt, CancellationToken token)
    {
        string? stream = null;
        if (_options.DeadLetterStream is { } definition)
        {
            await ProvisionAsync(definition, StreamProvisioning.CreateIfMissing, attempt, token);
            stream = definition.Name;
        }
        else
        {
            var probe = $"{_options.DeadLetterSubjectPrefix}.probe";
            await foreach (var name in _js.ListStreamNamesAsync(probe, token))
            {
                stream = name;
                break;
            }

            if (stream == null)
            {
                throw new NatsMessagingTransientException(
                    $"No stream captures the dead-letter subjects '{_options.DeadLetterSubjectPrefix}.>'. " +
                    "Create one, or set DeadLetterStream so this service creates it.");
            }
        }

        await CheckDeadLetterStreamAsync(stream, token);
    }

    /// <summary>
    /// The server config of the dead-letter stream, found or provisioned. Interest retention keeps no dead letter
    /// (nothing consumes the subjects), so it is a configuration error. Limits that evict entries, and WorkQueue
    /// retention (a consumer's ack deletes the entry), are Warnings.
    /// A dead-letter write whose source ack is lost is written again after AckWait. Inside the DLQ stream's
    /// DuplicateWindow the second write is dropped as a duplicate; after it, the DLQ gets a second entry.
    /// </summary>
    private async Task CheckDeadLetterStreamAsync(string stream, CancellationToken token)
    {
        var info = await _js.GetStreamAsync(stream, cancellationToken: token);
        var config = info.Info.Config;
        if (DeadLetterWriter.RetentionProblem(stream, config.Retention) is { } problem)
        {
            throw new NatsMessagingConfigurationException(problem);
        }

        if (config.Retention == StreamConfigRetention.Workqueue)
        {
            _logger.LogWarning(
                "The dead-letter stream {Stream} has WorkQueue retention: any consumer that acks a dead letter (an " +
                "inspection tool, for example) deletes it before it is replayed. Use Limits retention.", stream);
        }

        var limits = new[]
        {
            config.MaxMsgs > 0 ? $"MaxMsgs {config.MaxMsgs}" : null,
            config.MaxBytes > 0 ? $"MaxBytes {config.MaxBytes}" : null,
            config.MaxMsgsPerSubject > 0 ? $"MaxMsgsPerSubject {config.MaxMsgsPerSubject}" : null
        }.OfType<string>().ToList();
        if (limits.Count > 0)
        {
            _logger.LogWarning(
                "The dead-letter stream {Stream} has {Limits} (Discard {Discard}). At the limit it {Effect}.",
                stream, string.Join(", ", limits), config.Discard,
                config.Discard == StreamConfigDiscard.New
                    ? "rejects new dead letters, and the failed messages stay in their streams until it has room"
                    : "drops its oldest dead letters before anyone replays them");
        }

        var longest = _registration.Consumers.MaxBy(c => c.AckWait);
        if (longest == null)
        {
            return;
        }

        var window = config.DuplicateWindow;
        if (window > TimeSpan.Zero && longest.AckWait > window)
        {
            _logger.LogWarning(
                "Consumer {Durable} has AckWait {AckWait}, longer than the DuplicateWindow {DuplicateWindow} of the dead-letter stream {Stream}. " +
                "A dead-letter write whose ack is lost can then be stored twice. Raise the stream's DuplicateWindow.",
                longest.Durable, longest.AckWait, window, stream);
        }
    }

    /// <summary>
    /// Once every consumer on <paramref name="stream"/> runs, reports the consumers this service owns there that
    /// no handler uses (log and gauge, never a delete). A failed listing is tried again with backoff.
    /// </summary>
    private async Task ReportOrphansAsync(string stream, CancellationToken token)
    {
        var durables = _registration.Consumers;
        var reporter = new OrphanReporter(_js, _loggerFactory.CreateLogger<OrphanReporter>());
        try
        {
            foreach (var durable in durables.Where(d => d.Stream == stream))
            {
                await _status.WaitUntilRunningAsync(durable.ComponentName, token);
            }

            for (var failures = 1; ; failures++)
            {
                try
                {
                    var orphans = await reporter.ReportAsync(stream, durables, _options.ServiceName, token);
                    NatsMessagingTelemetry.SetOrphans(this, stream, orphans.Count);
                    return;
                }
                catch (Exception e) when (!token.IsCancellationRequested)
                {
                    var delay = RetryLoop.Backoff(failures);
                    _logger.LogWarning("Could not list the consumers of stream {Stream} for the orphan report: {Error}. Retrying in {Delay}",
                        stream, RetryLoop.Describe(e), delay);
                    await Task.Delay(delay, token);
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
    }

    private void CancelHandlers()
    {
        lock (_workers)
        {
            foreach (var worker in _workers)
            {
                worker.CancelHandlers();
            }
        }
    }

    private async Task RunConsumerLoopAsync(ConsumerDurable durable, CancellationToken token)
    {
        var worker = new ConsumerWorker(durable, _connection, _js, _options, _scopes, _deadLetters,
            _loggerFactory.CreateLogger<ConsumerWorker>(), token);
        lock (_workers)
        {
            _workers.Add(worker);
        }

        _declaredStreams.TryGetValue(durable.Stream, out var definition);
        await Loop(durable.ComponentName).RunAsync(async (attempt, t) =>
        {
            if (definition != null)
            {
                await _status.WaitUntilRunningAsync(NatsMessagingRegistration.StreamComponent(durable.Stream), t);
                await RecoverStreamAsync(definition, t);
            }

            try
            {
                await worker.RunAsync(attempt, t);
            }
            catch (NatsJSApiException e) when (definition != null && e.Error.ErrCode == ConsumerWorker.StreamNotFound)
            {
                MarkStreamLost(definition, e);
                throw;
            }
        }, token);

        await worker.DrainAsync();
    }

    /// <summary>
    /// A declared stream that disappeared after it was provisioned (a Memory stream after a server restart, a
    /// delete by hand). The stream component leaves Running, and the next consumer attempt provisions it again.
    /// </summary>
    private void MarkStreamLost(StreamDefinition definition, NatsJSApiException error)
    {
        if (_lostStreams.TryAdd(definition.Name, true))
        {
            _logger.LogWarning("Stream {Stream} is gone ({Error}); provisioning it again ({Mode})", definition.Name,
                error.Error.Description, _options.StreamProvisioning);
        }

        _status.SetRetrying(NatsMessagingRegistration.StreamComponent(definition.Name), RetryLoop.Describe(error));
    }

    /// <summary>
    /// Provisions a lost stream again in the configured mode (VerifyOnly only checks, so it waits for the owner).
    /// A failure throws, so the consumer attempt retries with backoff.
    /// </summary>
    private async Task RecoverStreamAsync(StreamDefinition definition, CancellationToken token)
    {
        if (!_lostStreams.ContainsKey(definition.Name))
        {
            return;
        }

        var gate = _recoveryGates.GetOrAdd(definition.Name, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(token);
        try
        {
            if (!_lostStreams.ContainsKey(definition.Name))
            {
                return;
            }

            var component = NatsMessagingRegistration.StreamComponent(definition.Name);
            var warning = await _provisioner.ProvisionAsync(definition, _options.StreamProvisioning, token);
            _status.SetWarning(component, warning);
            _status.SetRunning(component);
            _lostStreams.TryRemove(definition.Name, out _);
            _logger.LogInformation("Stream {Stream} is back", definition.Name);
        }
        finally
        {
            gate.Release();
        }
    }

    private void OnFaulted(string component, Exception exception)
    {
        if (_options.OnComponentFaulted != FaultAction.StopApplication)
        {
            return;
        }

        if (_lifetime == null)
        {
            _logger.LogCritical(exception,
                "NATS messaging component {Component} is faulted. OnComponentFaulted is StopApplication, but there is no host to stop",
                component);
            return;
        }

        _logger.LogCritical(exception,
            "NATS messaging component {Component} is faulted; stopping the application (OnComponentFaulted = StopApplication)",
            component);
        _lifetime.StopApplication();
    }
}
