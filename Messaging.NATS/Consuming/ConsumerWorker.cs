using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// Runs one durable consumer: makes sure it exists with the right config, pulls only for free handler slots,
/// dispatches each message to the binding whose filter matches its subject, and settles it by the decision table
/// of design 2.5. <see cref="RunAsync"/> is one attempt of a <see cref="RetryLoop"/>; the worker keeps its state
/// (ack floor, in-flight handlers, slots) across attempts.
/// </summary>
internal sealed class ConsumerWorker
{
    internal const int ConsumerNotFound = 10014;
    internal const int StreamNotFound = 10059;
    internal const int ConsumerAlreadyExists = 10148;

    internal static readonly TimeSpan FetchExpires = TimeSpan.FromSeconds(5);
    internal static readonly TimeSpan HandoffGrace = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MinFetchCycle = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MinHeartbeat = TimeSpan.FromMilliseconds(100);

    // Consecutive failed ConsumerInfo reads after which the consumer reports that it is stalled. After a failure
    // the next read comes sooner, so a stall is reported within about three RefreshRetry intervals.
    internal const int StallRefreshFailures = 3;
    private static readonly TimeSpan RefreshRetry = TimeSpan.FromSeconds(5);

    // A message no binding matches is NAKed back for another pod: during a rolling deploy the durable's filters
    // already include a newer pod's subjects, and another pod has the binding. Every hand-back is a delivery the
    // server counts, and the pod that gets it next cannot tell it from a failed attempt. So a pod hands a message
    // back only while its delivery count is at most MaxDeliverySlack: a pod with the binding then always runs the
    // handler (step 3 only acts past MaxDeliveryAttempts + MaxDeliverySlack). After a hand-back this pod pauses its
    // pulls for UnroutableYield, so the redelivery goes to a pod that is pulling, not back to this one.
    internal static readonly TimeSpan UnroutableRetryDelay = TimeSpan.FromSeconds(1);
    internal static readonly TimeSpan UnroutableYield = TimeSpan.FromSeconds(3);

    private readonly ConsumerDurable _durable;
    private readonly NatsJSContext _js;
    private readonly NatsMessagingOptions _options;
    private readonly IServiceScopeFactory _scopes;
    private readonly DeadLetterWriter _deadLetters;
    private readonly ILogger _logger;
    private readonly Dictionary<ConsumerBinding, IHandlerInvoker> _invokers;
    private readonly KeyValuePair<string, object?> _consumerTag;

    private readonly SemaphoreSlim _slots;
    private readonly ConcurrentDictionary<long, Task> _inFlight = new();
    private readonly CancellationTokenSource _handlerStop = new();
    private readonly SemaphoreSlim _refreshNow = new(0, 1);
    private long _nextId;
    private long _nextOverdueId;
    private volatile bool _stopping;

    // Stall tracking (design ground rule: no loop stops for good without a log line). The fetch and refresh loops
    // both report here; _stallLock guards every field below it.
    private readonly object _stallLock = new();
    private RetryAttempt? _attempt;
    private int _failedRefreshes;
    private string? _refreshError;
    private long _lastGoodFetch;
    private long _idleSince;
    private string? _fetchTrouble;
    private string? _stalled;
    private string? _configWarning;

    // Handlers that ran past MaxHandlerTime and have not returned yet: dispatch id -> description.
    private readonly ConcurrentDictionary<long, string> _overdue = new();
    private long _yieldUntil;

    private bool _seen;
    private ulong _ackFloor;
    private long _maxDeliver = -1;
    private long _heartbeatTicks;
    private DateTimeOffset? _streamCreated;

    public ConsumerWorker(ConsumerDurable durable, INatsConnection connection, NatsJSContext js,
        NatsMessagingOptions options, IServiceScopeFactory scopes, DeadLetterWriter deadLetters, ILogger logger,
        CancellationToken stoppingToken)
    {
        _durable = durable;
        _js = js;
        _options = options;
        _scopes = scopes;
        _deadLetters = deadLetters;
        _logger = logger;
        _invokers = new Dictionary<ConsumerBinding, IHandlerInvoker>(ReferenceEqualityComparer.Instance);
        foreach (var binding in durable.Bindings)
        {
            _invokers[binding] = HandlerInvoker.Create(binding, connection.Opts.SerializerRegistry);
        }

        _consumerTag = NatsMessagingTelemetry.Consumer(durable.Durable);
        _heartbeatTicks = HeartbeatFor(durable.AckWait, null).Ticks;
        _slots = new SemaphoreSlim(durable.MaxConcurrency, durable.MaxConcurrency);

        // From here on every settle waits for the server's confirmation, so none is lost when the connection closes.
        stoppingToken.Register(() => _stopping = true);
    }

    /// <summary>
    /// Ensures the durable, then consumes until <paramref name="stoppingToken"/> is cancelled. Throws when the
    /// durable cannot be ensured, or when it disappears while consuming (the next attempt recreates it).
    /// </summary>
    public async Task RunAsync(RetryAttempt attempt, CancellationToken stoppingToken)
    {
        var consumer = await EnsureConsumerAsync(attempt, stoppingToken).ConfigureAwait(false);
        attempt.Running();
        lock (_stallLock)
        {
            _attempt = attempt;
            _failedRefreshes = 0;
            _refreshError = null;
            _lastGoodFetch = Stopwatch.GetTimestamp();
            _idleSince = 0;
            _fetchTrouble = null;
            _stalled = null;
        }

        PublishWarning(attempt);
        await ConsumeAsync(consumer, stoppingToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Waits for the running handlers, up to ShutdownTimeout. Handlers still running then see their token cancelled,
    /// and their messages are NAKed for another process (design 2.10).
    /// </summary>
    public async Task DrainAsync()
    {
        _stopping = true;
        var running = _inFlight.Values.ToArray();
        if (running.Length == 0)
        {
            return;
        }

        _logger.LogInformation("Consumer {Durable} is waiting for {Count} running handler(s)", _durable.Durable,
            running.Length);
        var all = Task.WhenAll(running);
        if (await Task.WhenAny(all, Task.Delay(_options.ShutdownTimeout)).ConfigureAwait(false) == all)
        {
            return;
        }

        _logger.LogWarning(
            "Consumer {Durable}: {Count} handler(s) still running after {ShutdownTimeout}; cancelling them",
            _durable.Durable, _inFlight.Count, _options.ShutdownTimeout);
        await _handlerStop.CancelAsync().ConfigureAwait(false);
        await Task.WhenAny(all, Task.Delay(HandoffGrace)).ConfigureAwait(false);
    }

    /// <summary>
    /// Cancels the running handlers now (the host's shutdown budget has ended). Their messages are handed off.
    /// </summary>
    public void CancelHandlers()
    {
        _stopping = true;
        try
        {
            _handlerStop.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private async Task<INatsJSConsumer> EnsureConsumerAsync(RetryAttempt attempt, CancellationToken cancellationToken)
    {
        var stream = await _js.GetStreamAsync(_durable.Stream, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        var streamConfig = stream.Info.Config;
        NoteStreamCreated(stream.Info);

        var problem = ConsumerDiscovery.StreamProblem(_durable.Bindings.Select(b => b.Filter), _durable.DeliverFrom,
            _durable.Stream, streamConfig.AllowMsgSchedules, streamConfig.Retention, _durable.InactiveThreshold);
        if (problem != null)
        {
            throw new NatsMessagingConfigurationException(
                $"Consumer {_durable.Durable} ({_durable.HandlerType.FullName}): {problem}");
        }

        INatsJSConsumer consumer;
        string? warning = null;
        for (var tries = 0; ; tries++)
        {
            INatsJSConsumer? existing = null;
            try
            {
                existing = await _js.GetConsumerAsync(_durable.Stream, _durable.Durable, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (NatsJSApiException e) when (e.Error.ErrCode == ConsumerNotFound)
            {
            }

            if (existing != null)
            {
                (consumer, warning) = await UpdateAsync(existing, cancellationToken).ConfigureAwait(false);
                break;
            }

            try
            {
                consumer = await _js.CreateConsumerAsync(_durable.Stream, NewConfig(stream.Info),
                    cancellationToken).ConfigureAwait(false);
                break;
            }
            catch (NatsJSApiException e) when (e.Error.ErrCode == ConsumerAlreadyExists && tries == 0)
            {
                // Another process created it in between; update that one instead.
            }
        }

        ReadInfo(consumer.Info);
        _seen = true;

        var maxDeliver = Interlocked.Read(ref _maxDeliver);
        if (maxDeliver != -1)
        {
            var capWarning =
                $"The server MaxDeliver of {_durable.Durable} is {maxDeliver}, not -1. A message that is delivered " +
                $"{maxDeliver} times without being settled (for example because its handler crashes the process) " +
                "stays in the stream and is never dead-lettered. Fix the rejected update, or delete the durable so it " +
                "is recreated.";
            _logger.LogWarning("{Warning}", capWarning);
            warning = warning == null ? capWarning : $"{warning} {capWarning}";
        }

        if (AckWaitWarning(consumer.Info.Config) is { } ackWaitWarning)
        {
            _logger.LogWarning("{Warning}", ackWaitWarning);
            warning = warning == null ? ackWaitWarning : $"{warning} {ackWaitWarning}";
        }

        _configWarning = warning;
        PublishWarning(attempt);
        _logger.LogInformation(
            "Consumer {Durable} on stream {Stream} is consuming {Filters} (deliver policy {DeliverPolicy}, max concurrency {MaxConcurrency})",
            _durable.Durable, _durable.Stream, string.Join(", ", _durable.Bindings.Select(b => b.Filter)),
            consumer.Info.Config.DeliverPolicy, _durable.MaxConcurrency);
        return consumer;
    }

    /// <summary>
    /// A stream deleted and created again restarts its sequences at 1, so the ack floor read from the old stream
    /// would skip the new stream's first messages. The durable then resumes from the start of the new stream.
    /// </summary>
    private void NoteStreamCreated(StreamInfo info)
    {
        if (_streamCreated is { } known && known != info.Created && _seen)
        {
            _logger.LogWarning(
                "Stream {Stream} was created again ({Created}, before {Known}); durable {Durable} resumes from the start of the new stream",
                _durable.Stream, info.Created, known, _durable.Durable);
            _ackFloor = 0;
        }

        _streamCreated = info.Created;
    }

    private ConsumerConfig NewConfig(StreamInfo stream)
    {
        var retention = stream.Config.Retention;
        var config = new ConsumerConfig(_durable.Durable)
        {
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = _durable.AckWait,
            MaxDeliver = -1,
            MaxAckPending = _durable.MaxAckPending,
            InactiveThreshold = _durable.InactiveThreshold,
            Metadata = _durable.Metadata(null)
        };
        _durable.ApplyFilters(config);

        if (!_seen)
        {
            config.DeliverPolicy = _durable.DeliverFrom switch
            {
                DeliverFrom.All => ConsumerConfigDeliverPolicy.All,
                DeliverFrom.New => ConsumerConfigDeliverPolicy.New,
                _ => retention == StreamConfigRetention.Workqueue
                    ? ConsumerConfigDeliverPolicy.All
                    : ConsumerConfigDeliverPolicy.New
            };
            return config;
        }

        // This process had the durable and it has gone (InactiveThreshold, deleted by hand, lost on R3). Resume after
        // the last ack floor this worker read, so nothing is skipped; some messages may be handled twice.
        // A WorkQueue stream only keeps unacked messages and only allows All. A floor past the stream's last
        // sequence belongs to an earlier stream of the same name, so the durable starts at the beginning.
        if (_ackFloor > 0 && retention != StreamConfigRetention.Workqueue && _ackFloor <= stream.State.LastSeq)
        {
            config.DeliverPolicy = ConsumerConfigDeliverPolicy.ByStartSequence;
            config.OptStartSeq = _ackFloor + 1;
        }
        else
        {
            config.DeliverPolicy = ConsumerConfigDeliverPolicy.All;
        }

        _logger.LogWarning(
            "Durable {Durable} on stream {Stream} was gone and is recreated from {Start}. Messages after the last " +
            "ack floor may be handled twice.",
            _durable.Durable, _durable.Stream,
            config.DeliverPolicy == ConsumerConfigDeliverPolicy.All ? "the start of the stream" : $"sequence {config.OptStartSeq}");
        NatsMessagingTelemetry.ConsumerRecreated.Add(1, NatsMessagingTelemetry.Consumer(_durable.Durable),
            NatsMessagingTelemetry.Stream(_durable.Stream));
        return config;
    }

    /// <summary>
    /// Overlays the server-mutable fields on the existing config. DeliverPolicy, start sequence/time, AckPolicy and
    /// DeliverGroup stay as they are (4.x set DeliverGroup on pull consumers, and DeliverPolicy cannot change).
    /// A rejected update is a Warning: the worker consumes with the server's config.
    /// </summary>
    private async Task<(INatsJSConsumer Consumer, string? Warning)> UpdateAsync(INatsJSConsumer existing,
        CancellationToken cancellationToken)
    {
        var current = existing.Info.Config;
        ThrowOnOtherOwner(current);
        var desired = current with
        {
            AckWait = _durable.AckWait,
            MaxDeliver = -1,
            MaxAckPending = _durable.MaxAckPending,
            InactiveThreshold = _durable.InactiveThreshold,
            Metadata = _durable.Metadata(current.Metadata),
            Backoff = null
        };
        _durable.ApplyFilters(desired);

        if (SameMutableFields(current, desired))
        {
            return (existing, null);
        }

        try
        {
            var updated = await _js.CreateOrUpdateConsumerAsync(_durable.Stream, desired, cancellationToken)
                .ConfigureAwait(false);
            _logger.LogInformation("Updated durable {Durable} on stream {Stream}", _durable.Durable, _durable.Stream);
            return (updated, null);
        }
        catch (NatsJSApiException e) when (NatsErrorClassifier.ClassifyCode(e.Error.ErrCode) == ErrorClass.Permanent)
        {
            var warning =
                $"The server rejected the update of durable {_durable.Durable}: {e.Error.Description} " +
                $"(err_code {e.Error.ErrCode}). Consuming with the server's config.";
            _logger.LogWarning(e, "{Warning}", warning);
            NatsMessagingTelemetry.ConsumerConfigRejected.Add(1, NatsMessagingTelemetry.Consumer(_durable.Durable),
                NatsMessagingTelemetry.Stream(_durable.Stream));
            return (existing, warning);
        }
    }

    /// <summary>
    /// A durable whose owner metadata names another service is that service's: updating it would overwrite its
    /// filters and settings, and both services would take each other's messages. The plain queue group is accepted
    /// as an owner too, so setting ServiceName later keeps the durable.
    /// </summary>
    private void ThrowOnOtherOwner(ConsumerConfig current)
    {
        if (current.Metadata == null || !current.Metadata.TryGetValue(ConsumerDurable.OwnerMetadata, out var owner) ||
            owner == _durable.Owner || owner == _durable.QueueGroup)
        {
            return;
        }

        throw new NatsMessagingConfigurationException(
            $"Durable {_durable.Durable} on stream {_durable.Stream} belongs to '{owner}' (its {ConsumerDurable.OwnerMetadata} " +
            $"metadata), not to this service ('{_durable.Owner}'), so {_durable.HandlerType.FullName} would take it over. " +
            "Give one of the handlers its own Durable or QueueGroup on [NatsConsumer], or a distinct ServiceName. " +
            "If the durable really moved to this service, delete it so it is created again.");
    }

    /// <summary>
    /// The heartbeat interval for a server config: a third of the shortest wait the server applies (AckWait, or a
    /// Backoff entry, which the server uses as the wait for that delivery).
    /// </summary>
    internal static TimeSpan HeartbeatFor(TimeSpan ackWait, ICollection<TimeSpan>? backoff)
    {
        var interval = EffectiveAckWait(ackWait, backoff) / 3;
        return interval > MinHeartbeat ? interval : MinHeartbeat;
    }

    internal static TimeSpan EffectiveAckWait(TimeSpan ackWait, ICollection<TimeSpan>? backoff)
    {
        var effective = ackWait > TimeSpan.Zero ? ackWait : ConsumerDiscovery.DefaultAckWait;
        var shortest = backoff?.Where(b => b > TimeSpan.Zero).DefaultIfEmpty(effective).Min() ?? effective;
        return shortest < effective ? shortest : effective;
    }

    private string? AckWaitWarning(ConsumerConfig config)
    {
        var effective = EffectiveAckWait(config.AckWait, config.Backoff);
        if (effective >= _durable.AckWait)
        {
            return null;
        }

        var backoff = config.Backoff is { Count: > 0 } ? $" and Backoff [{string.Join(", ", config.Backoff)}]" : string.Empty;
        return $"The server AckWait of {_durable.Durable} is {config.AckWait}{backoff}, shorter than the " +
               $"{_durable.AckWait} it is declared with. The heartbeat follows the server and runs every {HeartbeatFor(config.AckWait, config.Backoff)}.";
    }

    private static bool SameMutableFields(ConsumerConfig current, ConsumerConfig desired) =>
        current.AckWait == desired.AckWait &&
        current.MaxDeliver == desired.MaxDeliver &&
        current.MaxAckPending == desired.MaxAckPending &&
        current.InactiveThreshold == desired.InactiveThreshold &&
        (current.Backoff == null || current.Backoff.Count == 0) &&
        string.Equals(current.FilterSubject ?? string.Empty, desired.FilterSubject ?? string.Empty, StringComparison.Ordinal) &&
        (current.FilterSubjects ?? []).SequenceEqual(desired.FilterSubjects ?? []) &&
        desired.Metadata!.All(kv => current.Metadata != null &&
                                    current.Metadata.TryGetValue(kv.Key, out var value) && value == kv.Value);

    private void ReadInfo(ConsumerInfo info)
    {
        Interlocked.Exchange(ref _maxDeliver, info.Config.MaxDeliver <= 0 ? -1 : info.Config.MaxDeliver);
        Interlocked.Exchange(ref _heartbeatTicks, HeartbeatFor(info.Config.AckWait, info.Config.Backoff).Ticks);

        // With nothing pending, everything up to the last delivered sequence is settled. A new DeliverPolicy New
        // durable reports its start there while its ack floor is still 0, so this is also where it resumes.
        var floor = info.AckFloor.StreamSeq;
        if (info.NumAckPending == 0 && info.Delivered.StreamSeq > floor)
        {
            floor = info.Delivered.StreamSeq;
        }

        if (floor > _ackFloor)
        {
            _ackFloor = floor;
        }
    }

    private async Task ConsumeAsync(INatsJSConsumer consumer, CancellationToken stoppingToken)
    {
        using var attempt = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        Exception? gone = null;
        var refresh = RefreshLoopAsync(consumer, e =>
        {
            gone = e;
            attempt.Cancel();
        }, attempt.Token);

        try
        {
            while (true)
            {
                await PullAsync(consumer, attempt.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (gone != null && !stoppingToken.IsCancellationRequested)
        {
            ExceptionDispatchInfo.Throw(gone);
        }
        finally
        {
            await attempt.CancelAsync().ConfigureAwait(false);
            await refresh.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads ConsumerInfo every InfoRefreshInterval, at once when a fetch suggests the durable is gone, and within
    /// RefreshRetry after a failed read. Keeps the ack floor and MaxDeliver current; a missing durable or stream ends
    /// the attempt, and failed reads count towards a stall (<see cref="UpdateStall"/>).
    /// </summary>
    private async Task RefreshLoopAsync(INatsJSConsumer consumer, Action<Exception> onGone,
        CancellationToken cancellationToken)
    {
        var interval = _options.ConsumerTestHooks.InfoRefreshInterval;
        var wait = interval;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _refreshNow.WaitAsync(wait, cancellationToken).ConfigureAwait(false);
                await consumer.RefreshAsync(cancellationToken).ConfigureAwait(false);
                ReadInfo(consumer.Info);
                wait = interval;
                RefreshEnded(null);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (NatsJSApiException e) when (e.Error.ErrCode is ConsumerNotFound or StreamNotFound)
            {
                _logger.LogWarning("Durable {Durable} on stream {Stream} is gone: {Error}", _durable.Durable,
                    _durable.Stream, e.Error.Description);
                onGone(e);
                return;
            }
            catch (Exception e)
            {
                wait = interval < RefreshRetry ? interval : RefreshRetry;
                RefreshEnded(e);
            }
        }
    }

    /// <summary>
    /// Counts consecutive failed ConsumerInfo reads (no responders, JetStream temporarily unavailable, a timeout).
    /// The first is a Warning; <see cref="StallRefreshFailures"/> in a row mark the consumer stalled.
    /// </summary>
    private void RefreshEnded(Exception? error)
    {
        int failures;
        lock (_stallLock)
        {
            failures = error == null ? 0 : ++_failedRefreshes;
            _failedRefreshes = failures;
            _refreshError = error == null ? null : RetryLoop.Describe(error);
        }

        if (error != null)
        {
            _logger.Log(failures == 1 ? LogLevel.Warning : LogLevel.Debug, error,
                "Reading the info of durable {Durable} on stream {Stream} failed ({Failures} in a row): {Error}",
                _durable.Durable, _durable.Stream, failures, RetryLoop.Describe(error));
        }

        UpdateStall();
    }

    /// <summary>
    /// Records how a fetch ended. <paramref name="trouble"/> is the no-responders, protocol or heartbeat
    /// notification it got, or null; a fetch that delivered a message, or ended without trouble, is good.
    /// </summary>
    private void FetchEnded(int delivered, string? trouble)
    {
        lock (_stallLock)
        {
            if (delivered > 0 || trouble == null)
            {
                _lastGoodFetch = Stopwatch.GetTimestamp();
                _fetchTrouble = null;
            }
            else
            {
                _fetchTrouble = trouble;
            }
        }

        UpdateStall();
    }

    /// <summary>
    /// A consumer that is up but cannot fetch or read its info (a stream or consumer without quorum on R3, no
    /// JetStream responders) consumes nothing and throws nothing. It is reported as Retrying (health Degraded)
    /// with a Warning, and back to Running with an Information log once a fetch or a read works again.
    /// </summary>
    private void UpdateStall()
    {
        string? started = null;
        string? ended = null;
        RetryAttempt? attempt;
        lock (_stallLock)
        {
            attempt = _attempt;
            var stalledFor = Stopwatch.GetElapsedTime(_lastGoodFetch) -
                             (_idleSince == 0 ? TimeSpan.Zero : Stopwatch.GetElapsedTime(_idleSince));
            var reason = _failedRefreshes >= StallRefreshFailures
                ? $"reading the info of durable {_durable.Durable} failed {_failedRefreshes} times in a row ({_refreshError})"
                : _fetchTrouble != null && stalledFor >= _options.ConsumerTestHooks.StallAfter
                    ? $"no fetch of durable {_durable.Durable} has worked for {stalledFor:g} ({_fetchTrouble})"
                    : null;

            if (reason != null && _stalled == null)
            {
                started = reason;
            }
            else if (reason == null && _stalled != null)
            {
                ended = _stalled;
            }

            _stalled = reason;
        }

        if (attempt == null)
        {
            return;
        }

        if (started != null)
        {
            _logger.LogWarning(
                "Consumer {Durable} on stream {Stream} is stalled and receives no messages: {Reason}. It keeps trying",
                _durable.Durable, _durable.Stream, started);
            attempt.SetStalled($"Stalled: {started}");
        }
        else if (ended != null)
        {
            _logger.LogInformation("Consumer {Durable} on stream {Stream} is consuming again", _durable.Durable,
                _durable.Stream);
            attempt.ClearStalled();
        }
    }

    /// <summary>
    /// Marks the start and end of a wait in which this worker does not fetch on purpose: every handler slot is busy,
    /// or it yields after handing a message back. That time is not stall time, so a fetch that has one hiccup after
    /// a long handler is not reported as stalled.
    /// </summary>
    private void IdleStarted()
    {
        lock (_stallLock)
        {
            _idleSince = Stopwatch.GetTimestamp();
        }
    }

    private void IdleEnded()
    {
        lock (_stallLock)
        {
            if (_idleSince != 0)
            {
                _lastGoodFetch += Stopwatch.GetTimestamp() - _idleSince;
                _idleSince = 0;
            }
        }
    }

    private void RefreshSoon()
    {
        try
        {
            _refreshNow.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    /// <summary>
    /// One pull for the free slots only, so every delivered message starts at once and is covered by its heartbeat.
    /// </summary>
    private async Task PullAsync(INatsJSConsumer consumer, CancellationToken cancellationToken)
    {
        IdleStarted();
        try
        {
            var yieldFor = Stopwatch.GetElapsedTime(Stopwatch.GetTimestamp(), Interlocked.Read(ref _yieldUntil));
            if (yieldFor > TimeSpan.Zero)
            {
                await Task.Delay(yieldFor, cancellationToken).ConfigureAwait(false);
            }

            await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            IdleEnded();
        }

        var requested = 1;
        while (requested < _durable.BatchMax && _slots.Wait(0))
        {
            requested++;
        }

        var delivered = 0;
        string? trouble = null;
        var started = Stopwatch.GetTimestamp();
        try
        {
            var opts = new NatsJSFetchOpts
            {
                MaxMsgs = requested,
                Expires = FetchExpires,
                NotificationHandler = (notification, _) =>
                {
                    switch (notification)
                    {
                        case NatsJSNoRespondersNotification:
                            trouble = "no responders";
                            RefreshSoon();
                            break;
                        case NatsJSProtocolNotification protocol:
                            trouble = $"protocol message {protocol.HeaderCode} {protocol.HeaderMessageText}";
                            RefreshSoon();
                            break;
                        case NatsJSTimeoutNotification:
                            trouble = "missed idle heartbeats";
                            break;
                    }

                    return Task.CompletedTask;
                }
            };

            await foreach (var msg in consumer.FetchAsync(opts, NatsRawSerializer<byte[]>.Default, cancellationToken)
                               .ConfigureAwait(false))
            {
                delivered++;
                Dispatch(msg);
            }
        }
        finally
        {
            if (requested > delivered)
            {
                _slots.Release(requested - delivered);
            }
        }

        FetchEnded(delivered, trouble ?? _options.ConsumerTestHooks.FetchTrouble?.Invoke());

        // A fetch that ends at once (no responders while a leader moves, or a deleted durable) must not spin.
        var elapsed = Stopwatch.GetElapsedTime(started);
        if (delivered == 0 && elapsed < MinFetchCycle)
        {
            await Task.Delay(MinFetchCycle - elapsed, cancellationToken).ConfigureAwait(false);
        }
    }

    private void Dispatch(INatsJSMsg<byte[]> msg)
    {
        var id = Interlocked.Increment(ref _nextId);
        var task = Task.Run(async () =>
        {
            try
            {
                await ProcessAsync(msg).ConfigureAwait(false);
            }
            finally
            {
                _slots.Release();
            }
        });
        _inFlight[id] = task;
        task.ContinueWith(_ => _inFlight.TryRemove(id, out Task? _), TaskScheduler.Default);
    }

    private async Task ProcessAsync(INatsJSMsg<byte[]> msg)
    {
        if (msg.Metadata is not { } metadata)
        {
            _logger.LogError("Message on {Subject} for {Durable} has no JetStream metadata; ignored", msg.Subject,
                _durable.Durable);
            return;
        }

        // The handler span's parent is the publisher's trace from the headers, nothing else.
        Activity.Current = null;
        var binding = _durable.BindingFor(msg.Subject);
        var started = Stopwatch.GetTimestamp();
        NatsMessagingTelemetry.InFlight.Add(1, _consumerTag);
        using var activity = StartProcessActivity(msg, metadata, binding);
        var result = new Decision(ProcessOutcome.Retry);

        try
        {
            await using var settlement = new MessageSettlement(msg, () => _stopping, _logger);
            if (!_options.ConsumerTestHooks.DisableHeartbeat)
            {
                settlement.StartHeartbeat(TimeSpan.FromTicks(Interlocked.Read(ref _heartbeatTicks)));
            }

            try
            {
                result = await DecideAsync(msg, metadata, binding, settlement).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Processing {Stream}:{Sequence} on {Subject} for {Durable} failed unexpectedly",
                    metadata.Stream, metadata.Sequence.Stream, msg.Subject, _durable.Durable);
                await settlement.NakAsync(_options.ConsumerTestHooks.DeadLetterNakDelay).ConfigureAwait(false);
                result = new Decision(ProcessOutcome.Retry, e, TypeName(e));
            }
        }
        finally
        {
            NatsMessagingTelemetry.InFlight.Add(-1, _consumerTag);
            Record(activity, result, started);
        }
    }

    private Activity? StartProcessActivity(INatsJSMsg<byte[]> msg, NatsJSMsgMetadata metadata, ConsumerBinding? binding)
    {
        if (!NatsMessagingTelemetry.ActivitySource.HasListeners())
        {
            return null;
        }

        var filter = binding is { Filter.Length: > 0 } ? binding.Filter : msg.Subject;
        var activity = NatsMessagingTelemetry.ActivitySource.StartActivity($"process {filter}", ActivityKind.Consumer,
            NatsMessagingTelemetry.ParentContext(msg.Headers));
        if (activity is not { IsAllDataRequested: true })
        {
            return activity;
        }

        activity.SetTag(NatsMessagingTelemetry.System, "nats");
        activity.SetTag(NatsMessagingTelemetry.OperationName, "process");
        activity.SetTag(NatsMessagingTelemetry.OperationType, "process");
        activity.SetTag(NatsMessagingTelemetry.Destination, msg.Subject);
        activity.SetTag(NatsMessagingTelemetry.ConsumerGroup, _durable.Durable);
        activity.SetTag(NatsMessagingTelemetry.MessageIdTag,
            MessageContext<object>.ResolveMessageId(msg.Headers, metadata.Stream, metadata.Sequence.Stream));
        activity.SetTag(NatsMessagingTelemetry.ConversationId,
            MessageContext<object>.Header(msg.Headers, HeaderSanitiser.CorrelationId));
        activity.SetTag(NatsMessagingTelemetry.BodySize, msg.Data?.Length ?? 0);
        activity.SetTag(NatsMessagingTelemetry.StreamTag, metadata.Stream);
        activity.SetTag(NatsMessagingTelemetry.StreamSequence, (long)metadata.Sequence.Stream);
        activity.SetTag(NatsMessagingTelemetry.DeliveryCount, (long)metadata.NumDelivered);
        activity.SetTag(NatsMessagingTelemetry.Handler, (binding?.HandlerType ?? _durable.HandlerType).FullName);
        activity.SetTag(NatsMessagingTelemetry.ScheduleIdTag,
            MessageContext<object>.Header(msg.Headers, HeaderSanitiser.ScheduleId));
        return activity;
    }

    private void Record(Activity? activity, Decision result, long started)
    {
        var tags = new TagList { _consumerTag, { NatsMessagingTelemetry.Outcome, result.Outcome } };
        NatsMessagingTelemetry.ProcessDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, tags);
        NatsMessagingTelemetry.ConsumedMessages.Add(1, tags);

        if (activity == null)
        {
            return;
        }

        activity.SetTag(NatsMessagingTelemetry.Outcome, result.Outcome);
        if (result.ErrorType != null)
        {
            NatsMessagingTelemetry.SetError(activity, result.Error, result.ErrorType, result.Description);
        }
    }

    /// <summary>
    /// The decision table of design 2.5. n = NumDelivered, r = the deliveries that count as attempts (n without a
    /// legacy deferral), A = MaxDeliveryAttempts, S = MaxDeliverySlack, M = the server MaxDeliver.
    /// </summary>
    private async Task<Decision> DecideAsync(INatsJSMsg<byte[]> msg, NatsJSMsgMetadata metadata,
        ConsumerBinding? binding, MessageSettlement settlement)
    {
        var n = (int)metadata.NumDelivered;
        var maxDeliver = Interlocked.Read(ref _maxDeliver);

        // 0. No binding matches: during a rolling deploy another process of this service may have it. Hand it back
        // only while a process with the binding would still run its handler (n <= S, see UnroutableRetryDelay).
        if (binding == null)
        {
            var limit = _options.MaxDeliverySlack;
            var reason = $"No binding of {_durable.Durable} matches subject '{msg.Subject}'";
            if (n <= limit && (maxDeliver == -1 || n < maxDeliver))
            {
                _logger.Log(n == 1 ? LogLevel.Information : LogLevel.Debug,
                    "{Reason} (delivery {Delivery}; handed back up to MaxDeliverySlack = {Limit} times); handing it " +
                    "back for another process and pausing pulls for {Yield}. A rolling deploy that adds a binding " +
                    "causes this; otherwise check the durable's filters",
                    reason, n, limit, UnroutableYield);
                Interlocked.Exchange(ref _yieldUntil,
                    Stopwatch.GetTimestamp() + (long)(UnroutableYield.TotalSeconds * Stopwatch.Frequency));
                await settlement.NakAsync(UnroutableRetryDelay).ConfigureAwait(false);
                return new Decision(ProcessOutcome.Retry, null, "UnroutableSubject", reason);
            }

            return await DeadLetterAsync(msg, settlement, null, "UnroutableSubject", reason, null).ConfigureAwait(false);
        }

        // 1. A targeted replay of a dead letter for another durable.
        var replayFor = MessageContext<object>.Header(msg.Headers, HeaderSanitiser.ReplayConsumer);
        if (replayFor != null && replayFor != _durable.Durable)
        {
            _logger.LogDebug("Skipped {Stream}:{Sequence}: a replay for {ReplayConsumer}", metadata.Stream,
                metadata.Sequence.Stream, replayFor);
            await settlement.AckAsync().ConfigureAwait(false);
            return new Decision(ProcessOutcome.Skipped);
        }

        // 2. A 4.x deferred message that is not due yet.
        if (LegacyDeferral(msg.Headers) is { } deferral)
        {
            _logger.LogDebug("Deferred {Stream}:{Sequence} on {Subject} for {Delay} (Msg-DeferredUntil)",
                metadata.Stream, metadata.Sequence.Stream, msg.Subject, deferral);
            await settlement.NakAsync(deferral).ConfigureAwait(false);
            return new Decision(ProcessOutcome.Deferred);
        }

        var attempts = binding.MaxDeliveryAttempts;
        var slack = _options.MaxDeliverySlack;
        var r = n - DeferralDeliveries(msg.Headers, metadata);

        // 3. Crash-loop guard: never run the handler again past A + S deliveries.
        if (maxDeliver == -1 && r > attempts + slack)
        {
            return await DeadLetterAsync(msg, settlement, binding, "MaxDeliveriesExceeded",
                $"Delivered {n} times; the limit is MaxDeliveryAttempts ({attempts}) + MaxDeliverySlack ({slack})",
                null).ConfigureAwait(false);
        }

        // 4-5. Run the handler (an INatsHandler never sees a null or unreadable payload), for at most MaxHandlerTime.
        Exception? undeliverable = null;
        Exception? failure = null;
        using var handlerToken = CancellationTokenSource.CreateLinkedTokenSource(_handlerStop.Token);
        var overdue = new HandlerOverdue();
        var dispatchId = Interlocked.Increment(ref _nextOverdueId);
        await using (handlerToken.Token.Register(() => OnHandlerCancelled(overdue, dispatchId, binding, metadata,
                         msg.Subject, settlement)).ConfigureAwait(false))
        {
            if (_durable.MaxHandlerTime > TimeSpan.Zero && _durable.MaxHandlerTime.TotalMilliseconds < int.MaxValue)
            {
                handlerToken.CancelAfter(_durable.MaxHandlerTime);
            }

            try
            {
                undeliverable = await _invokers[binding]
                    .InvokeAsync(_scopes, msg, settlement, _durable, handlerToken.Token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                failure = e;
            }
            finally
            {
                overdue.Returned = true;
            }
        }

        // After the registration is disposed, so no callback can add the handler again.
        if (_overdue.TryRemove(dispatchId, out _))
        {
            PublishWarning(CurrentAttempt());
        }

        // A handler that ran past MaxHandlerTime failed, whether it threw on its cancelled token or returned after it
        // (it may have stopped half way). Its heartbeat has stopped, so the message may already be redelivered.
        if (overdue.TimedOut && !_handlerStop.IsCancellationRequested && !settlement.IsSettled &&
            undeliverable == null && failure is null or OperationCanceledException)
        {
            failure = new TimeoutException(
                $"The handler ran longer than its MaxHandlerTime ({_durable.MaxHandlerTime})", failure);
        }

        if (undeliverable != null)
        {
            var type = undeliverable is NatsMessagingDeadLetterException d ? d.FailureType : TypeName(undeliverable);
            return await DeadLetterAsync(msg, settlement, binding, type, Describe(undeliverable), undeliverable)
                .ConfigureAwait(false);
        }

        // 6. Returned normally. A handler that returns after the drain cancelled its token may have stopped half
        // way (a catch-all, or a loop on the token), so its message is handed off like a cancelled one (step 8).
        if (failure == null)
        {
            if (!settlement.IsSettled && _handlerStop.IsCancellationRequested)
            {
                return await HandOffAsync(msg, metadata, settlement).ConfigureAwait(false);
            }

            if (!settlement.IsSettled)
            {
                await settlement.AckAsync().ConfigureAwait(false);
            }

            return new Decision(OutcomeOf(settlement.Kind));
        }

        if (settlement.IsSettled)
        {
            _logger.LogWarning(failure,
                "Handler {Handler} threw after settling {Stream}:{Sequence} on {Subject}; nothing more is done",
                binding.HandlerType.Name, metadata.Stream, metadata.Sequence.Stream, msg.Subject);
            return new Decision(OutcomeOf(settlement.Kind), failure, TypeName(failure));
        }

        // 7. Poison.
        if (failure is PoisonMessageException)
        {
            _logger.LogWarning(failure, "Poison message {Stream}:{Sequence} on {Subject} for {Durable}",
                metadata.Stream, metadata.Sequence.Stream, msg.Subject, _durable.Durable);
            return await DeadLetterAsync(msg, settlement, binding, TypeName(failure), Describe(failure), failure)
                .ConfigureAwait(false);
        }

        // 8. Cancelled because this process is stopping: hand the message to another process at once.
        if (failure is OperationCanceledException && _handlerStop.IsCancellationRequested)
        {
            return await HandOffAsync(msg, metadata, settlement).ConfigureAwait(false);
        }

        // 9. Out of attempts, or the server delivers no more (a finite M counts every delivery).
        if (r >= attempts || (maxDeliver != -1 && n >= maxDeliver))
        {
            _logger.LogError(failure,
                "Handler {Handler} failed on {Stream}:{Sequence} ({Subject}), attempt {Attempt} of {Attempts}; dead-lettering",
                binding.HandlerType.Name, metadata.Stream, metadata.Sequence.Stream, msg.Subject, r, attempts);
            return await DeadLetterAsync(msg, settlement, binding, TypeName(failure), Describe(failure), failure)
                .ConfigureAwait(false);
        }

        // 10. Retry later.
        var delay = binding.RetryDelay(r);
        _logger.LogWarning(failure,
            "Handler {Handler} failed on {Stream}:{Sequence} ({Subject}), attempt {Attempt} of {Attempts}; retrying in {Delay}",
            binding.HandlerType.Name, metadata.Stream, metadata.Sequence.Stream, msg.Subject, r, attempts, delay);
        await settlement.NakAsync(delay).ConfigureAwait(false);
        return new Decision(ProcessOutcome.Retry, failure, TypeName(failure));
    }

    /// <summary>
    /// Runs when a handler's token is cancelled. A drain cancels every handler (step 8); otherwise the handler ran past
    /// MaxHandlerTime: its heartbeat stops, so the server redelivers the message after AckWait even if the handler
    /// never returns, and the consumer shows a Warning until it does. The handler keeps its slot until it returns.
    /// </summary>
    private void OnHandlerCancelled(HandlerOverdue overdue, long dispatchId, ConsumerBinding binding,
        NatsJSMsgMetadata metadata, string subject, MessageSettlement settlement)
    {
        if (_handlerStop.IsCancellationRequested || settlement.IsSettled || overdue.Returned)
        {
            return;
        }

        overdue.TimedOut = true;
        settlement.StopHeartbeat();
        var description =
            $"Handler {binding.HandlerType.Name} has run longer than its MaxHandlerTime ({_durable.MaxHandlerTime}) on " +
            $"{metadata.Stream}:{metadata.Sequence.Stream} ({subject}); its token is cancelled and its heartbeat stopped";
        _logger.LogError(
            "{Description}. The message is redelivered after AckWait; the handler keeps one of the {MaxConcurrency} " +
            "slot(s) of {Durable} until it returns",
            description, _durable.MaxConcurrency, _durable.Durable);
        _overdue[dispatchId] = description;
        PublishWarning(CurrentAttempt());
    }

    private RetryAttempt? CurrentAttempt()
    {
        lock (_stallLock)
        {
            return _attempt;
        }
    }

    /// <summary>
    /// The component Warning: a rejected or limited server config (from the last ensure), and the handlers that are
    /// past MaxHandlerTime.
    /// </summary>
    private void PublishWarning(RetryAttempt? attempt)
    {
        if (attempt == null)
        {
            return;
        }

        var parts = new List<string>();
        if (_configWarning != null)
        {
            parts.Add(_configWarning);
        }

        parts.AddRange(_overdue.Values.Take(3));
        if (_overdue.Count > 3)
        {
            parts.Add($"{_overdue.Count - 3} more handler(s) past MaxHandlerTime");
        }

        attempt.SetWarning(parts.Count == 0 ? null : string.Join(" ", parts));
    }

    private async Task<Decision> HandOffAsync(INatsJSMsg<byte[]> msg, NatsJSMsgMetadata metadata,
        MessageSettlement settlement)
    {
        _logger.LogInformation("Handing off {Stream}:{Sequence} on {Subject}: shutting down", metadata.Stream,
            metadata.Sequence.Stream, msg.Subject);
        await settlement.NakAsync(TimeSpan.Zero).ConfigureAwait(false);
        return new Decision(ProcessOutcome.Handoff);
    }

    private static string OutcomeOf(SettleKind kind) => kind switch
    {
        SettleKind.Nak => ProcessOutcome.Retry,
        SettleKind.Term => ProcessOutcome.Terminated,
        _ => ProcessOutcome.Ack
    };

    /// <summary>
    /// Writes the message to the DLQ stream and acks it (DoubleAck) only after the PubAck. A failed write is tried
    /// again inline while the heartbeat holds the message; when every attempt fails, the message is NAKed and stays
    /// in its stream (the server cap is -1, so it comes back and the write is tried again).
    /// </summary>
    private async Task<Decision> DeadLetterAsync(INatsJSMsg<byte[]> msg, MessageSettlement settlement,
        ConsumerBinding? binding, string failureType, string reason, Exception? failure)
    {
        var subject = binding?.DeadLetterSubject ?? $"{_options.DeadLetterSubjectPrefix}.{msg.Subject}";
        var metadata = msg.Metadata;
        var delays = _options.ConsumerTestHooks.DeadLetterRetryDelays;
        Exception? writeError = null;

        for (var attempt = 0; ; attempt++)
        {
            try
            {
                var ack = await _deadLetters.WriteAsync(subject, msg, _durable.Durable, failureType, reason,
                    _handlerStop.Token).ConfigureAwait(false);
                await settlement.AckAsync(confirm: true).ConfigureAwait(false);
                _logger.LogWarning(
                    "Dead-lettered {Stream}:{Sequence} from {Subject} to {DeadLetterStream}:{DeadLetterSequence} ({DeadLetterSubject}{Duplicate}) after {Deliveries} delivery(ies): {FailureType}",
                    metadata?.Stream, metadata?.Sequence.Stream, msg.Subject, ack.Stream, ack.Seq, subject,
                    ack.Duplicate ? ", already stored" : string.Empty, metadata?.NumDelivered, failureType);
                return new Decision(ProcessOutcome.DeadLetter, failure, failureType, reason);
            }
            catch (Exception e)
            {
                writeError = e;
            }

            if (attempt >= delays.Length || _handlerStop.IsCancellationRequested)
            {
                break;
            }

            _logger.LogWarning(writeError,
                "Dead-letter write {Attempt} for {Stream}:{Sequence} to {DeadLetterSubject} failed; retrying in {Delay}",
                attempt + 1, metadata?.Stream, metadata?.Sequence.Stream, subject, delays[attempt]);
            try
            {
                await Task.Delay(delays[attempt], _handlerStop.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        var nakDelay = _options.ConsumerTestHooks.DeadLetterNakDelay;
        _logger.LogError(writeError,
            "Could not dead-letter {Stream}:{Sequence} from {Subject} to {DeadLetterSubject}; it stays in the stream and is retried in {Delay}",
            metadata?.Stream, metadata?.Sequence.Stream, msg.Subject, subject, nakDelay);
        NatsMessagingTelemetry.DeadLetterFailures.Add(1, _consumerTag, NatsMessagingTelemetry.Stream(_durable.Stream));
        await settlement.NakAsync(nakDelay).ConfigureAwait(false);
        return new Decision(ProcessOutcome.Retry, failure ?? writeError, failureType,
            $"{reason} (dead-letter write failed: {writeError?.Message})");
    }

    /// <summary>
    /// The delay until a 4.x Msg-DeferredUntil time, or null when the header is missing, unreadable or due.
    /// </summary>
    internal static TimeSpan? LegacyDeferral(NatsHeaders? headers)
    {
        var value = MessageContext<object>.Header(headers, HeaderSanitiser.LegacyDeferredUntil);
        if (value == null || !DateTime.TryParse(value, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var until))
        {
            return null;
        }

        var delay = until - DateTime.UtcNow;
        return delay > TimeSpan.Zero ? delay : null;
    }

    /// <summary>
    /// 1 when a 4.x Msg-DeferredUntil message was stored before it was due and is now on a later delivery: its first
    /// delivery was the deferral NAK (4.x or step 2), which is not a handler attempt. Otherwise 0.
    /// </summary>
    internal static int DeferralDeliveries(NatsHeaders? headers, NatsJSMsgMetadata metadata)
    {
        if (metadata.NumDelivered < 2)
        {
            return 0;
        }

        var value = MessageContext<object>.Header(headers, HeaderSanitiser.LegacyDeferredUntil);
        return value != null && DateTime.TryParse(value, CultureInfo.InvariantCulture,
                   DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var until) &&
               until > metadata.Timestamp.UtcDateTime
            ? 1
            : 0;
    }

    private static string TypeName(Exception e) => e.GetType().FullName ?? e.GetType().Name;

    private static string Describe(Exception e) =>
        e.InnerException == null ? e.Message : $"{e.Message}: {e.InnerException.Message}";

    private sealed class HandlerOverdue
    {
        public volatile bool TimedOut;
        public volatile bool Returned;
    }

    /// <summary>
    /// What happened to one delivery: the outcome tag, and the failure behind it, if any.
    /// </summary>
    private readonly record struct Decision(string Outcome, Exception? Error = null, string? ErrorType = null,
        string? Description = null);
}
