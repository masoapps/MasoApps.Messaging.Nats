using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// Which dead letters <see cref="NatsDeadLetters.ReplayAsync"/> replays.
/// </summary>
public sealed record ReplayOptions
{
    /// <summary>
    /// The original subject to replay, relative to the dead-letter prefix ("orders.created", "orders.>").
    /// Null replays every entry under the prefix.
    /// </summary>
    public string? SubjectFilter { get; init; }

    /// <summary>
    /// Only entries whose X-Consumer (the durable that failed) is this name.
    /// </summary>
    public string? Consumer { get; init; }

    /// <summary>
    /// True (the default): only the durable that failed runs its handler; every other library durable on the
    /// subject acks the replayed message without calling its handler. An entry is skipped and kept when no handler
    /// would run for it: its durable no longer exists, no longer filters the subject, has no pull waiting (no
    /// running handler, as for the orphan of a renamed handler), or is on another stream than the one that now
    /// captures the subject.
    /// False: every consumer on the subject receives the message again. On an Interest stream an entry is skipped and
    /// kept when no consumer filters the subject, because the stream would acknowledge the replay and keep nothing.
    /// </summary>
    public bool Targeted { get; init; } = true;

    /// <summary>
    /// The most entries to look at (replayed plus skipped) in one call.
    /// </summary>
    public int Max { get; init; } = int.MaxValue;
}

/// <summary>
/// The outcome of a replay. Skipped entries stay in the dead-letter stream.
/// </summary>
public sealed record ReplayResult(int Replayed, IReadOnlyList<ReplaySkip> Skipped);

/// <summary>
/// An entry that was not replayed, and why. It is kept in the dead-letter stream.
/// </summary>
public sealed record ReplaySkip(ulong DeadLetterSequence, string Reason);

/// <summary>
/// Replays dead letters to their original subject (design 2.6). The payload bytes and the original headers are
/// republished, X-Original-Msg-Id included, so the handler sees the message id it saw when the message failed.
/// An entry is deleted from the dead-letter stream only after its replay is acknowledged.
/// </summary>
public sealed class NatsDeadLetters
{
    public const string ReplayedFrom = "X-Replayed-From";

    private const int NoMessageFound = 10037;

    // A running worker keeps a pull open for its free slots. Between two fetches, or while every slot is busy,
    // there is none, so the durable is read a few times before it counts as having no running handler.
    private const int LiveChecks = 5;
    private static readonly TimeSpan LiveCheckInterval = TimeSpan.FromMilliseconds(400);

    // The failure headers the dead-letter writer adds. X-Original-Msg-Id is not one of them: a replay keeps it.
    private static readonly HashSet<string> FailureHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        HeaderSanitiser.OriginalSubject,
        HeaderSanitiser.Stream,
        HeaderSanitiser.Consumer,
        HeaderSanitiser.StreamSequence,
        HeaderSanitiser.DeliveryCount,
        HeaderSanitiser.FailureType,
        HeaderSanitiser.FailureReason,
        HeaderSanitiser.FailedAt,
        HeaderSanitiser.ReplayConsumer,
        ReplayedFrom
    };

    private readonly NatsJSContext _js;
    private readonly NatsMessagingOptions _options;
    private readonly ILogger _logger;
    private readonly JetStreamPublish _publish;

    internal NatsDeadLetters(INatsConnection connection, NatsJSContext js, NatsMessagingOptions options,
        ILogger<NatsDeadLetters> logger)
    {
        _js = js;
        _options = options;
        _logger = logger;
        _publish = new JetStreamPublish(connection, logger);
    }

    /// <summary>
    /// Replays the entries that match <paramref name="options"/>, oldest first. Entries written while the replay
    /// runs (a replayed message that fails again) are left for the next call.
    /// </summary>
    /// <exception cref="InvalidOperationException">No stream, or more than one, captures the dead-letter subjects.</exception>
    public async Task<ReplayResult> ReplayAsync(ReplayOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfNegative(options.Max, nameof(options));

        var filter = $"{_options.DeadLetterSubjectPrefix}.{options.SubjectFilter ?? ">"}";
        if (!SubjectMatcher.IsValid(filter))
        {
            throw new ArgumentException($"SubjectFilter '{options.SubjectFilter}' is not a valid subject", nameof(options));
        }

        var streamName = await ResolveStreamAsync(filter, cancellationToken).ConfigureAwait(false);
        var stream = await _js.GetStreamAsync(streamName, cancellationToken: cancellationToken).ConfigureAwait(false);
        var lastSequence = stream.Info.State.LastSeq;

        var replayed = 0;
        var skipped = new List<ReplaySkip>();
        var targets = new TargetChecks();
        var next = Math.Max(stream.Info.State.FirstSeq, 1);

        while (replayed + skipped.Count < options.Max && next <= lastSequence)
        {
            StoredMessage entry;
            try
            {
                var response = await stream.GetAsync(new StreamMsgGetRequest { Seq = next, NextBySubj = filter },
                    cancellationToken).ConfigureAwait(false);
                entry = response.Message;
            }
            catch (NatsJSApiException e) when (e.Error.ErrCode == NoMessageFound)
            {
                break;
            }

            if (entry.Seq > lastSequence)
            {
                break;
            }

            next = entry.Seq + 1;
            var headers = ParseHeaders(entry.Hdrs);
            if (options.Consumer != null && Header(headers, HeaderSanitiser.Consumer) != options.Consumer)
            {
                continue;
            }

            var skip = await ReplayEntryAsync(streamName, entry, headers, options.Targeted, targets, cancellationToken)
                .ConfigureAwait(false);
            if (skip == null)
            {
                replayed++;
            }
            else
            {
                _logger.LogWarning("Dead letter {Stream}:{Sequence} was not replayed and is kept: {Reason}", streamName,
                    skip.DeadLetterSequence, skip.Reason);
                skipped.Add(skip);
            }
        }

        _logger.LogInformation("Replayed {Replayed} dead letter(s) from {Stream} ({Filter}, targeted: {Targeted}); skipped {Skipped}",
            replayed, streamName, filter, options.Targeted, skipped.Count);
        return new ReplayResult(replayed, skipped);
    }

    private async Task<ReplaySkip?> ReplayEntryAsync(string deadLetterStream, StoredMessage entry, NatsHeaders headers,
        bool targeted, TargetChecks targets, CancellationToken cancellationToken)
    {
        var target = Header(headers, HeaderSanitiser.OriginalSubject);
        if (target == null)
        {
            return new ReplaySkip(entry.Seq, $"The entry has no {HeaderSanitiser.OriginalSubject} header");
        }

        var sourceStream = Header(headers, HeaderSanitiser.Stream);
        var durable = Header(headers, HeaderSanitiser.Consumer);
        try
        {
            if (await TargetProblemAsync(target, sourceStream, durable, targeted, targets, cancellationToken)
                    .ConfigureAwait(false) is { } problem)
            {
                return new ReplaySkip(entry.Seq, problem);
            }
        }
        catch (NatsException e) when (!cancellationToken.IsCancellationRequested)
        {
            return new ReplaySkip(entry.Seq, $"Checking where '{target}' would be delivered failed: {RetryLoop.Describe(e)}");
        }

        var messageId = $"replay:{deadLetterStream}:{entry.Seq}";
        var replay = new NatsHeaders();
        foreach (var (name, value) in headers)
        {
            if (!name.StartsWith("Nats-", StringComparison.OrdinalIgnoreCase) && !FailureHeaders.Contains(name))
            {
                replay[name] = value;
            }
        }

        replay[ReplayedFrom] = $"{deadLetterStream}:{entry.Seq}";
        if (targeted)
        {
            replay[HeaderSanitiser.ReplayConsumer] = durable!;
        }

        replay[HeaderSanitiser.MessageId] = messageId;

        var payload = entry.Data.ToArray();
        using var activity = NatsMessagingTelemetry.StartProducer("publish", target, messageId);
        var started = Stopwatch.GetTimestamp();
        try
        {
            using var budget = new PublishBudget(_options.PublishTimeout, cancellationToken);
            var ack = await _publish.PublishAsync(target, payload, replay, messageId, _options.PublishRetryAttempts,
                budget, activity).ConfigureAwait(false);
            NatsMessagingTelemetry.RecordOperation(activity, "publish", target, started,
                ack.Duplicate ? "duplicate" : "ok", null);
        }
        catch (Exception e) when (e is NatsPublishException or NatsMessagingConfigurationException)
        {
            NatsMessagingTelemetry.RecordOperation(activity, "publish", target, started,
                NatsMessagingTelemetry.PublishOutcome(e), e);
            return new ReplaySkip(entry.Seq, $"Publishing to {target} failed: {e.Message}");
        }

        try
        {
            await _js.DeleteMessageAsync(deadLetterStream, new StreamMsgDeleteRequest { Seq = entry.Seq, NoErase = true },
                cancellationToken).ConfigureAwait(false);
        }
        catch (NatsJSApiException e)
        {
            // The replay is stored; a second replay of this entry is dropped as a duplicate within the target
            // stream's DuplicateWindow.
            _logger.LogWarning("Replayed dead letter {Stream}:{Sequence}, but could not delete it: {Error}",
                deadLetterStream, entry.Seq, RetryLoop.Describe(e));
        }

        return null;
    }

    /// <summary>
    /// Why the replay of an entry would reach no handler, or null when one runs it. The entry is deleted once the
    /// replay is acknowledged, so it must not be replayed into a stream or durable that keeps nothing for a handler.
    /// </summary>
    private async Task<string?> TargetProblemAsync(string target, string? sourceStream, string? durable, bool targeted,
        TargetChecks targets, CancellationToken cancellationToken)
    {
        if (!targets.Streams.TryGetValue(target, out var capturedBy))
        {
            capturedBy = await StreamsCapturingAsync(target, cancellationToken).ConfigureAwait(false);
            targets.Streams[target] = capturedBy;
        }

        if (!targeted)
        {
            return capturedBy.Count == 1
                ? await InterestProblemAsync(capturedBy[0], target, targets, cancellationToken).ConfigureAwait(false)
                : null;
        }

        if (sourceStream == null || durable == null)
        {
            return $"The entry has no {HeaderSanitiser.Stream} or {HeaderSanitiser.Consumer} header, so a targeted " +
                   "replay cannot name the durable. Replay it with Targeted = false.";
        }

        if (capturedBy.Count != 1 || capturedBy[0] != sourceStream)
        {
            var now = capturedBy.Count == 0 ? "no stream" : string.Join(", ", capturedBy);
            return $"'{target}' is now captured by {now}, not by {sourceStream}, so durable {durable} would not see " +
                   "this replay and no handler would run. Replay with Targeted = false.";
        }

        if (!targets.Durables.TryGetValue((sourceStream, durable), out var check))
        {
            check = await DurableCheckAsync(sourceStream, durable, cancellationToken).ConfigureAwait(false);
            targets.Durables[(sourceStream, durable)] = check;
        }

        if (check.Problem != null)
        {
            return check.Problem;
        }

        return Filters(check.Filters, target)
            ? null
            : $"Durable {durable} on stream {sourceStream} no longer filters '{target}' (its filters: " +
              $"{string.Join(", ", check.Filters)}), so no handler would run this replay. Replay with Targeted = false.";
    }

    /// <summary>
    /// An Interest stream acknowledges a message that no consumer filters, and removes it at once. Null when
    /// <paramref name="stream"/> is not an Interest stream, or a consumer filters <paramref name="target"/>.
    /// </summary>
    private async Task<string?> InterestProblemAsync(string stream, string target, TargetChecks targets,
        CancellationToken cancellationToken)
    {
        if (!targets.InterestFilters.TryGetValue(stream, out var filters))
        {
            var info = await _js.GetStreamAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (info.Info.Config.Retention == StreamConfigRetention.Interest)
            {
                var all = new List<IReadOnlyList<string>>();
                await foreach (var consumer in _js.ListConsumersAsync(stream, cancellationToken).ConfigureAwait(false))
                {
                    all.Add(FiltersOf(consumer.Info.Config));
                }

                filters = all;
            }

            targets.InterestFilters[stream] = filters;
        }

        if (filters == null || filters.Any(f => Filters(f, target)))
        {
            return null;
        }

        return $"Stream {stream} has Interest retention and no consumer filters '{target}', so the stream would " +
               "acknowledge the replay and keep nothing. Start a consumer for the subject, then replay again.";
    }

    private static IReadOnlyList<string> FiltersOf(ConsumerConfig config) =>
        config.FilterSubjects is { Count: > 0 } several ? several.ToList()
        : string.IsNullOrEmpty(config.FilterSubject) ? [] : [config.FilterSubject];

    // No filter means the whole stream.
    private static bool Filters(IReadOnlyList<string> filters, string subject) =>
        filters.Count == 0 || filters.Any(f => SubjectMatcher.IsSubsetOf(subject, f));

    /// <summary>
    /// Why a targeted replay to <paramref name="durable"/> would run no handler (null when a handler runs it), and the
    /// durable's filters. Every other library durable on the subject acks a targeted replay without its handler, so
    /// a durable that exists but has no running handler (the orphan of a renamed or removed handler class) would take
    /// the only copy and leave it unconsumed. A running worker shows as a pull request waiting on the durable.
    /// </summary>
    private async Task<DurableCheck> DurableCheckAsync(string stream, string durable, CancellationToken cancellationToken)
    {
        for (var check = 1; ; check++)
        {
            ConsumerInfo info;
            try
            {
                info = (await _js.GetConsumerAsync(stream, durable, cancellationToken).ConfigureAwait(false)).Info;
            }
            catch (NatsJSApiException e) when (e.Error.ErrCode is ConsumerWorker.ConsumerNotFound or ConsumerWorker.StreamNotFound)
            {
                return new DurableCheck(
                    $"Durable {durable} does not exist on stream {stream}, so no handler would run for this " +
                    "replay. Recreate the durable, or replay with Targeted = false.", []);
            }

            if (info.Config.Metadata?.ContainsKey(ConsumerDurable.HandlerMetadata) != true)
            {
                return new DurableCheck(
                    $"Durable {durable} on stream {stream} is not a Messaging.NATS durable (no " +
                    $"{ConsumerDurable.HandlerMetadata} metadata), so it would not run a targeted replay. Replay " +
                    "with Targeted = false.", []);
            }

            if (info.NumWaiting > 0)
            {
                return new DurableCheck(null, FiltersOf(info.Config));
            }

            if (check >= LiveChecks)
            {
                return new DurableCheck(
                    $"Durable {durable} on stream {stream} has no running handler (no pull request waiting " +
                    $"over {LiveChecks * LiveCheckInterval.TotalSeconds:0.#} s), so no handler would run this " +
                    "replay. It may be the orphan of a renamed or removed handler, or every handler slot is busy. " +
                    "Start the service that owns it and replay again, or replay with Targeted = false.", []);
            }

            await Task.Delay(LiveCheckInterval, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<IReadOnlyList<string>> StreamsCapturingAsync(string subject, CancellationToken cancellationToken)
    {
        var names = new List<string>();
        await foreach (var name in _js.ListStreamNamesAsync(subject, cancellationToken).ConfigureAwait(false))
        {
            names.Add(name);
        }

        return names;
    }

    private async Task<string> ResolveStreamAsync(string filter, CancellationToken cancellationToken)
    {
        if (_options.DeadLetterStream is { } definition)
        {
            return definition.Name;
        }

        var names = new List<string>();
        await foreach (var name in _js.ListStreamNamesAsync(filter, cancellationToken).ConfigureAwait(false))
        {
            names.Add(name);
        }

        return names.Count switch
        {
            1 => names[0],
            0 => throw new InvalidOperationException($"No stream captures the dead-letter subjects '{filter}'"),
            _ => throw new InvalidOperationException(
                $"Several streams capture the dead-letter subjects '{filter}': {string.Join(", ", names)}")
        };
    }

    private static string? Header(NatsHeaders headers, string name) => MessageContext<object>.Header(headers, name);

    /// <summary>
    /// What one replay call found out about its targets: the streams capturing each subject, and why each durable
    /// would not run a replay (null when it would).
    /// </summary>
    private sealed class TargetChecks
    {
        public Dictionary<string, IReadOnlyList<string>> Streams { get; } = new();

        public Dictionary<(string Stream, string Durable), DurableCheck> Durables { get; } = new();

        /// <summary>
        /// Per stream: the filters of each consumer when it is an Interest stream, otherwise null.
        /// </summary>
        public Dictionary<string, List<IReadOnlyList<string>>?> InterestFilters { get; } = new();
    }

    private sealed record DurableCheck(string? Problem, IReadOnlyList<string> Filters);

    /// <summary>
    /// The headers of a stored message: a base64 block "NATS/1.0\r\nName: value\r\n...". A repeated name keeps
    /// every value.
    /// </summary>
    internal static NatsHeaders ParseHeaders(string? base64Headers)
    {
        var headers = new NatsHeaders();
        if (string.IsNullOrEmpty(base64Headers))
        {
            return headers;
        }

        var lines = Encoding.UTF8.GetString(Convert.FromBase64String(base64Headers)).Split("\r\n");
        foreach (var line in lines.Skip(1))
        {
            var colon = line.IndexOf(':');
            if (colon <= 0)
            {
                continue;
            }

            var name = line[..colon].Trim();
            var value = line[(colon + 1)..].Trim();
            headers[name] = headers.TryGetValue(name, out var existing)
                ? StringValues.Concat(existing, value)
                : new StringValues(value);
        }

        return headers;
    }
}
