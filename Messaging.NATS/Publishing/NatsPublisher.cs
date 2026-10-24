using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// The <see cref="IPublisher"/> on a NATS connection. Publishes wait for the stream's acknowledgement inside a
/// budget (<see cref="NatsMessagingOptions.PublishTimeout"/>), and retry with the same message id. Schedules use
/// ADR-51 message schedules (nats-server 2.12+).
/// </summary>
public class NatsPublisher : IPublisher
{
    private const int NoMessageFound = 10037;
    private const int CreateIfAbsentRounds = 3;

    private static readonly ConcurrentDictionary<Type, Func<NatsPublisher, string, object, TimeSpan?, Task>> LegacyDispatch = new();

    private readonly INatsConnection _nats;
    private readonly NatsJSContext _js;
    private readonly NatsMessagingOptions _options;
    private readonly ILogger<NatsPublisher> _logger;
    private readonly JetStreamPublish _publish;
    private readonly ScheduleTargets _targets;

    public NatsPublisher(INatsConnection nats, NatsJSContext js, NatsMessagingOptions options, ILogger<NatsPublisher> logger)
    {
        _nats = nats;
        _js = js;
        _options = options;
        _logger = logger;
        _publish = new JetStreamPublish(nats, logger, options.PublisherTestHooks.DropPubAck);
        _targets = new ScheduleTargets(js, options.StreamDefinitions.ToList(), logger,
            options.PublisherTestHooks.ScheduleTargetRefreshInterval);
    }

    public Task PublishAsync(string subject, object message, TimeSpan? deferredBy)
    {
        ArgumentNullException.ThrowIfNull(message);
        var dispatch = LegacyDispatch.GetOrAdd(message.GetType(), CreateLegacyDispatch);
        return dispatch(this, subject, message, deferredBy);
    }

    public Task<PublishResult> PublishAsync<T>(string subject, T message) =>
        PublishAsync(subject, message, null, CancellationToken.None);

    public Task<PublishResult> PublishAsync<T>(string subject, T message, CancellationToken cancellationToken) =>
        PublishAsync(subject, message, null, cancellationToken);

    public Task<PublishResult> PublishAsync<T>(string subject, T message, PublishOptions? options) =>
        PublishAsync(subject, message, options, CancellationToken.None);

    public async Task<PublishResult> PublishAsync<T>(string subject, T message, PublishOptions? options,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(subject);
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();

        var retryAttempts = RetryAttempts(options?.RetryAttempts);
        var fireAndForget = options?.FireAndForget == true;
        var messageId = options?.MessageId ?? (retryAttempts > 0 && !fireAndForget ? NewId() : null);

        var payload = Serialize(message);
        var headers = HeaderSanitiser.Copy(options?.Headers);
        if (messageId != null)
        {
            headers[HeaderSanitiser.MessageId] = messageId;
        }

        HeaderSanitiser.AddCommon(headers, options?.CorrelationId, DateTimeOffset.UtcNow);

        using var activity = NatsMessagingTelemetry.StartProducer("publish", subject, messageId);
        activity?.SetTag(NatsMessagingTelemetry.BodySize, payload.Length);
        activity?.SetTag(NatsMessagingTelemetry.ConversationId, MessageContext<T>.Header(headers, HeaderSanitiser.CorrelationId));
        NatsMessagingTelemetry.Inject(headers, activity);

        var started = Stopwatch.GetTimestamp();
        try
        {
            var result = await PublishCoreAsync(subject, payload, headers, messageId, retryAttempts, fireAndForget,
                options?.Timeout, cancellationToken, activity).ConfigureAwait(false);
            NatsMessagingTelemetry.RecordOperation(activity, "publish", subject, started,
                result.Duplicate ? "duplicate" : "ok", null);
            return result;
        }
        catch (Exception e)
        {
            NatsMessagingTelemetry.RecordOperation(activity, "publish", subject, started,
                NatsMessagingTelemetry.PublishOutcome(e), e);
            throw;
        }
    }

    private async Task<PublishResult> PublishCoreAsync(string subject, byte[] payload, NatsHeaders headers,
        string? messageId, int retryAttempts, bool fireAndForget, TimeSpan? timeout,
        CancellationToken cancellationToken, Activity? activity)
    {
        using var budget = new PublishBudget(timeout ?? _options.PublishTimeout, cancellationToken);

        if (fireAndForget)
        {
            await Within(budget, subject, messageId, async () =>
            {
                await _nats.PublishAsync(subject, payload, headers, serializer: NatsRawSerializer<byte[]>.Default,
                    cancellationToken: budget.Token).ConfigureAwait(false);
                return true;
            }).ConfigureAwait(false);
            return new PublishResult(string.Empty, 0, false, messageId);
        }

        var ack = await _publish.PublishAsync(subject, payload, headers, messageId, retryAttempts, budget, activity)
            .ConfigureAwait(false);
        return new PublishResult(ack.Stream ?? string.Empty, ack.Seq, ack.Duplicate, messageId);
    }

    public Task<ScheduleResult> ScheduleAsync<T>(string subject, T message, DateTimeOffset deliverAt) =>
        ScheduleAsync(subject, message, deliverAt, null, CancellationToken.None);

    public Task<ScheduleResult> ScheduleAsync<T>(string subject, T message, DateTimeOffset deliverAt,
        ScheduleOptions? options) =>
        ScheduleAsync(subject, message, deliverAt, options, CancellationToken.None);

    public async Task<ScheduleResult> ScheduleAsync<T>(string subject, T message, DateTimeOffset deliverAt,
        ScheduleOptions? options, CancellationToken cancellationToken)
    {
        ScheduleTargets.ValidateSubject(subject, nameof(subject));
        ArgumentNullException.ThrowIfNull(message);
        var keyed = options?.Key != null;
        if (keyed)
        {
            ScheduleTargets.ValidateKey(options!.Key, nameof(options));
        }

        if (options?.ScheduleId != null)
        {
            ScheduleTargets.ValidateScheduleId(options.ScheduleId, nameof(options));
        }

        cancellationToken.ThrowIfCancellationRequested();

        using var activity = NatsMessagingTelemetry.StartProducer("schedule", subject, null);
        var started = Stopwatch.GetTimestamp();
        try
        {
            var result = await ScheduleCoreAsync(subject, message, deliverAt, options, keyed, activity,
                cancellationToken).ConfigureAwait(false);
            NatsMessagingTelemetry.RecordOperation(activity, "schedule", subject, started,
                result.Created ? "ok" : "duplicate", null);
            return result;
        }
        catch (Exception e)
        {
            NatsMessagingTelemetry.RecordOperation(activity, "schedule", subject, started,
                NatsMessagingTelemetry.PublishOutcome(e), e);
            throw;
        }
    }

    private async Task<ScheduleResult> ScheduleCoreAsync<T>(string subject, T message, DateTimeOffset deliverAt,
        ScheduleOptions? options, bool keyed, Activity? activity, CancellationToken cancellationToken)
    {
        // A caller's ScheduleId makes a retry of an unkeyed schedule land on the same holder subject with the same
        // Nats-Msg-Id, so the stream deduplicates it.
        var scheduleId = options?.ScheduleId ?? NewId();
        var key = options?.Key ?? (options?.ScheduleId != null ? scheduleId : NewId());
        var mode = keyed ? options!.Mode : ScheduleMode.Replace;
        var retryAttempts = RetryAttempts(options?.RetryAttempts);
        var payload = Serialize(message);

        using var budget = new PublishBudget(options?.Timeout ?? _options.PublishTimeout, cancellationToken);
        var target = await StepAsync(budget, subject, scheduleId, _ => _targets.ResolveAsync(subject, budget.Token))
            .ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        if (target.MaxAge > TimeSpan.Zero && deliverAt - now >= target.MaxAge)
        {
            throw new ArgumentOutOfRangeException(nameof(deliverAt), deliverAt,
                $"Stream '{target.Stream}' has MaxAge {target.MaxAge}, so a schedule {deliverAt - now} ahead would " +
                "expire before it fires");
        }

        var holder = ScheduleTargets.HolderSubject(target.Stream, subject, key, scheduleId);
        var holders = ScheduleTargets.HolderFilter(target.Stream, subject, key);
        var at = HeaderSanitiser.FormatTime(deliverAt);

        if (activity != null)
        {
            activity.SetTag(NatsMessagingTelemetry.MessageIdTag, scheduleId);
            activity.SetTag(NatsMessagingTelemetry.ScheduleIdTag, scheduleId);
            activity.SetTag(NatsMessagingTelemetry.ScheduleKey, keyed ? key : null);
            activity.SetTag(NatsMessagingTelemetry.ScheduleMode, keyed ? mode.ToString() : null);
            activity.SetTag(NatsMessagingTelemetry.ScheduleDeliverAt, at);
            activity.SetTag(NatsMessagingTelemetry.BodySize, payload.Length);
            activity.SetTag(NatsMessagingTelemetry.StreamTag, target.Stream);
        }

        var headers = HeaderSanitiser.Copy(options?.Headers);
        headers[HeaderSanitiser.Schedule] = $"@at {at}";
        headers[HeaderSanitiser.ScheduleTarget] = subject;
        headers[HeaderSanitiser.ScheduleId] = scheduleId;
        headers[HeaderSanitiser.ScheduledFor] = at;
        if (keyed)
        {
            headers[HeaderSanitiser.ScheduleKey] = key;
        }

        HeaderSanitiser.AddCommon(headers, options?.CorrelationId, now);

        // The scheduler copies traceparent onto the fired message, so its handler joins this trace.
        NatsMessagingTelemetry.Inject(headers, activity);

        // The scheduler strips Nats-Msg-Id from the fired message; it only deduplicates our own retries.
        headers[HeaderSanitiser.MessageId] = scheduleId;
        if (mode == ScheduleMode.CreateIfAbsent)
        {
            // No holder of this key may be pending: the check covers every holder subject of the key.
            headers[HeaderSanitiser.ExpectedLastSubjectSequence] = "0";
            headers[HeaderSanitiser.ExpectedLastSubjectSequenceSubject] = holders;
        }

        try
        {
            var ack = await _publish.PublishAsync(holder, payload, headers, scheduleId, retryAttempts, budget, activity)
                .ConfigureAwait(false);

            // Nothing is older than the first message of a stream, and the server reads a purge up to sequence 1 as
            // a purge of every matching message.
            if (keyed && mode == ScheduleMode.Replace && ack.Seq > 1)
            {
                await RemoveReplacedAsync(target.Stream, subject, key, holders, ack.Seq, scheduleId, budget)
                    .ConfigureAwait(false);
            }

            _logger.LogDebug("Scheduled {ScheduleId} for {Subject} at {DeliverAt} (key {Key}, {Mode})",
                scheduleId, subject, at, key, mode);
            return new ScheduleResult(scheduleId, subject, key, true, ack.Seq, deliverAt);
        }
        catch (NatsPublishException e) when (mode == ScheduleMode.CreateIfAbsent && IsWrongLastSequence(e))
        {
            return await PendingOrCreateAsync(target.Stream, subject, key, holder, holders, payload, headers,
                scheduleId, retryAttempts, deliverAt, budget, activity).ConfigureAwait(false);
        }
        catch (NatsMessagingConfigurationException)
        {
            _targets.Invalidate(target.Stream);
            throw;
        }
        catch (NatsPublishException e) when (e.Failure == PublishFailure.NoStream)
        {
            // The stream was deleted or replaced since it was resolved: the next call resolves the subject again.
            _targets.Invalidate(target.Stream);
            throw;
        }
    }

    private static bool IsWrongLastSequence(NatsPublishException e) =>
        e.ErrorCode is JetStreamPublish.WrongLastSequence or JetStreamPublish.WrongLastSequenceConstant;

    /// <summary>
    /// A Replace stores the new holder first, then purges the key's holders below it. Each holder has its own
    /// subject, so the server's purge after a fire never reaches the new one: near the fire time the worst case is
    /// one more delivery of the old payload (with its own X-Schedule-Id), never a lost schedule. A failed purge throws
    /// with an unknown outcome: the new schedule is pending, the old one may still fire, and a call again replaces
    /// both.
    /// </summary>
    private async Task RemoveReplacedAsync(string stream, string subject, string key, string holders, ulong sequence,
        string scheduleId, PublishBudget budget)
    {
        try
        {
            await StepAsync(budget, holders, scheduleId, async token =>
            {
                if (_options.PublisherTestHooks.BeforeReplacePurge is { } hook)
                {
                    await hook().ConfigureAwait(false);
                }

                return await _js.PurgeStreamAsync(stream, new StreamPurgeRequest { Filter = holders, Seq = sequence },
                    token).ConfigureAwait(false);
            }).ConfigureAwait(false);
        }
        catch (NatsPublishException e)
        {
            throw new NatsPublishException(PublishFailure.Timeout, holders, scheduleId, null,
                $"Schedule {scheduleId} for '{subject}' key '{key}' is stored, but the schedule it replaces could not " +
                $"be removed ({e.Message}), so both may fire. Call again to replace both.", e);
        }
    }

    /// <summary>
    /// CreateIfAbsent got 10071: a holder was pending. The expected-sequence check runs before deduplication, so a
    /// retry of our own stored attempt also ends here, and the pending holder then carries our id. When the holder
    /// fired before the lookup, the key is free: publish again with the same id. If the fired holder was ours, the
    /// stream answers with a duplicate; either way the result is Created with our id.
    /// </summary>
    private async Task<ScheduleResult> PendingOrCreateAsync(string stream, string subject, string key, string holder,
        string holders, byte[] payload, NatsHeaders headers, string scheduleId, int retryAttempts,
        DateTimeOffset deliverAt, PublishBudget budget, Activity? activity)
    {
        for (var round = 1; ; round++)
        {
            if (_options.PublisherTestHooks.BeforePendingLookup is { } hook)
            {
                await hook().ConfigureAwait(false);
            }

            var (pendingId, sequence) = await StepAsync(budget, holder, scheduleId,
                token => PendingScheduleAsync(stream, holders, token)).ConfigureAwait(false);
            if (sequence != 0)
            {
                return new ScheduleResult(pendingId, subject, key, pendingId == scheduleId, sequence, deliverAt);
            }

            if (round >= CreateIfAbsentRounds)
            {
                throw new NatsPublishException(PublishFailure.Timeout, holder, scheduleId, null,
                    $"CreateIfAbsent for '{subject}' key '{key}' kept meeting a holder that fired before it could be " +
                    $"read ({round} rounds). The outcome is unknown; call again with the same key.");
            }

            try
            {
                var ack = await _publish.PublishAsync(holder, payload, headers, scheduleId, retryAttempts, budget, activity)
                    .ConfigureAwait(false);
                return new ScheduleResult(scheduleId, subject, key, true, ack.Seq, deliverAt);
            }
            catch (NatsPublishException e) when (IsWrongLastSequence(e))
            {
            }
        }
    }

    public Task<bool> CancelScheduleAsync(string subject, string key) =>
        CancelScheduleAsync(subject, key, CancellationToken.None);

    public async Task<bool> CancelScheduleAsync(string subject, string key, CancellationToken cancellationToken)
    {
        ScheduleTargets.ValidateSubject(subject, nameof(subject));
        ScheduleTargets.ValidateKey(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();

        using var budget = new PublishBudget(_options.PublishTimeout, cancellationToken);
        var target = await StepAsync(budget, subject, null, _ => _targets.ResolveAsync(subject, budget.Token))
            .ConfigureAwait(false);
        var holders = ScheduleTargets.HolderFilter(target.Stream, subject, key);

        // Purge, never overwrite: a plain message on the holder subject orphans the pending schedule.
        var response = await StepAsync(budget, holders, null, async token =>
            await _js.PurgeStreamAsync(target.Stream, new StreamPurgeRequest { Filter = holders }, token)
                .ConfigureAwait(false)).ConfigureAwait(false);
        return response.Purged > 0;
    }

    public async Task<TResponse?> RequestAsync<TRequest, TResponse>(
        string subject,
        TRequest request,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var requestTimeout = timeout ?? TimeSpan.FromSeconds(5);

        _logger.LogDebug("Sending request to {Subject} with timeout {Timeout}", subject, requestTimeout);

        try
        {
            var response = await _nats.RequestAsync<TRequest, TResponse>(
                subject,
                request,
                replyOpts: new NatsSubOpts { Timeout = requestTimeout },
                cancellationToken: cancellationToken);

            _logger.LogDebug("Received response from {Subject}", subject);

            return response.Data;
        }
        catch (NatsNoRespondersException)
        {
            _logger.LogWarning("No responders available for subject {Subject}", subject);
            throw;
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Request to {Subject} timed out after {Timeout}", subject, requestTimeout);
            throw;
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Request to {Subject} was cancelled", subject);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error sending request to {Subject}", subject);
            throw;
        }
    }

    // Called through LegacyDispatch with the message's runtime type, so the serializer registry sees the real
    // type (a source-generated context has no entry for object).
    private Task LegacyAsync<T>(string subject, object message, TimeSpan? deferredBy) =>
        deferredBy is { } delay && delay > TimeSpan.Zero
            ? ScheduleAsync(subject, (T)message, DateTimeOffset.UtcNow + delay, null, CancellationToken.None)
            : PublishAsync(subject, (T)message, null, CancellationToken.None);

    private static Func<NatsPublisher, string, object, TimeSpan?, Task> CreateLegacyDispatch(Type type) =>
        typeof(NatsPublisher).GetMethod(nameof(LegacyAsync), BindingFlags.Instance | BindingFlags.NonPublic)!
            .MakeGenericMethod(type)
            .CreateDelegate<Func<NatsPublisher, string, object, TimeSpan?, Task>>();

    /// <summary>
    /// The newest pending holder under <paramref name="holders"/> (a wildcard over one key's holder subjects).
    /// </summary>
    private async Task<(string ScheduleId, ulong Sequence)> PendingScheduleAsync(string stream, string holders,
        CancellationToken cancellationToken)
    {
        try
        {
            var handle = await _js.GetStreamAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            var stored = await handle.GetAsync(new StreamMsgGetRequest { LastBySubj = holders }, cancellationToken)
                .ConfigureAwait(false);
            return (ReadHeader(stored.Message.Hdrs, HeaderSanitiser.ScheduleId) ?? string.Empty, stored.Message.Seq);
        }
        catch (NatsJSApiException e) when (e.Error.ErrCode == NoMessageFound)
        {
            // It fired in between.
            return (string.Empty, 0);
        }
    }

    /// <summary>
    /// One header value from a stored message's base64 header block ("NATS/1.0\r\nName: value\r\n...").
    /// </summary>
    internal static string? ReadHeader(string? base64Headers, string name)
    {
        if (string.IsNullOrEmpty(base64Headers))
        {
            return null;
        }

        var lines = Encoding.UTF8.GetString(Convert.FromBase64String(base64Headers)).Split("\r\n");
        foreach (var line in lines.Skip(1))
        {
            var colon = line.IndexOf(':');
            if (colon > 0 && line.AsSpan(0, colon).Trim().Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                return line[(colon + 1)..].Trim();
            }
        }

        return null;
    }

    private byte[] Serialize<T>(T message)
    {
        var writer = new ArrayBufferWriter<byte>();
        _nats.Opts.SerializerRegistry.GetSerializer<T>().Serialize(writer, message);
        return writer.WrittenSpan.ToArray();
    }

    private int RetryAttempts(int? requested)
    {
        var attempts = requested ?? _options.PublishRetryAttempts;
        if (attempts < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(PublishOptions.RetryAttempts), attempts, "must be zero or more");
        }

        return attempts;
    }

    private static string NewId() => Guid.NewGuid().ToString("N");

    /// <summary>
    /// Runs a JetStream API step of a schedule or cancel that is not the holder publish (the stream lookup and check,
    /// the CreateIfAbsent lookup, a purge) with the publish path's rules: no responders, no API response and HTTP 503
    /// answers are retried within the budget (250 ms, doubling), and what is left is a
    /// <see cref="NatsPublishException"/>: Rejected with the err_code for another API error or 503s to the end,
    /// Timeout (the outcome of a purge is unknown) otherwise. A <see cref="NatsMessagingConfigurationException"/> or
    /// a caller cancellation passes through.
    /// </summary>
    private static async Task<TResult> StepAsync<TResult>(PublishBudget budget, string subject, string? messageId,
        Func<CancellationToken, Task<TResult>> step)
    {
        var delay = StepRetryDelay;
        Exception? lastError = null;
        while (true)
        {
            try
            {
                return await step(budget.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException e) when (budget.Expired)
            {
                throw StepFailure(budget, subject, messageId, lastError ?? e);
            }
            catch (Exception e) when (IsRetryableStepError(e))
            {
                lastError = e;
            }
            catch (NatsJSApiException e)
            {
                throw new NatsPublishException(PublishFailure.Rejected, subject, messageId, e.Error.ErrCode,
                    $"'{subject}' failed: {e.Error.Description} (err_code {e.Error.ErrCode})", e);
            }
            catch (NatsException e)
            {
                throw new NatsPublishException(PublishFailure.Timeout, subject, messageId, null,
                    $"'{subject}' failed: {e.Message}. The outcome is unknown.", e);
            }

            var wait = budget.Remaining < delay ? budget.Remaining : delay;
            try
            {
                await Task.Delay(wait, budget.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (budget.Expired)
            {
                throw StepFailure(budget, subject, messageId, lastError);
            }

            delay *= 2;
        }
    }

    private static readonly TimeSpan StepRetryDelay = TimeSpan.FromMilliseconds(250);

    private static bool IsRetryableStepError(Exception e) => e switch
    {
        NatsJSApiException api => api.Error.Code == 503,
        NatsNoRespondersException or NatsJSApiNoResponseException or NatsNoReplyException => true,
        _ => false
    };

    // Like the publish path: 503s to the end are Rejected, no responders to the end means nothing answered
    // (NoStream), and anything else may have been applied (Timeout).
    private static NatsPublishException StepFailure(PublishBudget budget, string subject, string? messageId,
        Exception error) => error switch
    {
        NatsJSApiException { Error.Code: 503 } unavailable => new NatsPublishException(PublishFailure.Rejected,
            subject, messageId, unavailable.Error.ErrCode,
            $"'{subject}' failed within {budget.Timeout}: {unavailable.Error.Description} " +
            $"(err_code {unavailable.Error.ErrCode})", error),
        NatsNoRespondersException => new NatsPublishException(PublishFailure.NoStream, subject, messageId, null,
            $"No JetStream API answered for '{subject}' within {budget.Timeout}", error),
        _ => new NatsPublishException(PublishFailure.Timeout, subject, messageId, null,
            $"'{subject}' did not complete within {budget.Timeout}", error)
    };

    /// <summary>
    /// Runs a step of the call that is not a publish attempt, and turns the end of the budget into a
    /// <see cref="NatsPublishException"/> with <see cref="PublishFailure.Timeout"/>.
    /// </summary>
    private static async Task<TResult> Within<TResult>(PublishBudget budget, string subject, string? messageId,
        Func<Task<TResult>> step)
    {
        try
        {
            return await step().ConfigureAwait(false);
        }
        catch (OperationCanceledException e) when (budget.Expired)
        {
            throw new NatsPublishException(PublishFailure.Timeout, subject, messageId, null,
                $"'{subject}' did not complete within {budget.Timeout}", e);
        }
    }
}
