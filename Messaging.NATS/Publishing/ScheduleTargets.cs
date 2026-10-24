using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;

namespace Messaging.NATS;

/// <summary>
/// A stream that holds schedules for a target subject.
/// </summary>
/// <param name="Stream">The stream name.</param>
/// <param name="MaxAge">The stream's MaxAge; zero when unlimited.</param>
internal sealed record ScheduleTarget(string Stream, TimeSpan MaxAge);

/// <summary>
/// Holder subject naming and the stream behind a schedule target. A subject resolves to a declared
/// <see cref="StreamDefinition"/> first (matched on every call, so subjects that carry ids add no cache entry), then
/// to the server's stream for it. A server answer is cached per subject for <paramref name="refreshInterval"/>, at
/// most <paramref name="maxCachedSubjects"/> subjects, and dropped when its stream is invalidated. Each stream's check
/// (AllowMsgSchedules, the schedule rules, the API level, MaxAge) is reused for <paramref name="refreshInterval"/>,
/// then read again, so a changed MaxAge or rule is seen. A failed check is not cached. Warnings are logged once per
/// stream.
/// </summary>
internal sealed partial class ScheduleTargets(NatsJSContext js, IReadOnlyList<StreamDefinition> definitions, ILogger logger,
    TimeSpan refreshInterval, int maxCachedSubjects = ScheduleTargets.DefaultMaxCachedSubjects)
{
    public const int MaxKeyLength = 128;
    public const int DefaultMaxCachedSubjects = 1024;

    private const int StreamNotFound = 10059;
    private const int RequiredApiLevel = 2;

    private readonly ConcurrentDictionary<string, Resolved> _streamBySubject = new();
    private readonly ConcurrentDictionary<string, Check> _checked = new();
    private readonly ConcurrentDictionary<string, byte> _warned = new();

    /// <summary>
    /// "_sched.{stream}.{subject}.{key}.{scheduleId}": inside the stream (which captures "_sched.{stream}.>") and
    /// outside every consumer filter. Each schedule has its own holder subject. Storing a fire purges its holder by
    /// subject (nats-server 2.12 does not check the sequence), so a Replace on a shared subject that lands while the
    /// old holder fires would be purged with it, and the new schedule would never fire.
    /// </summary>
    public static string HolderSubject(string stream, string subject, string key, string scheduleId) =>
        $"_sched.{stream}.{subject}.{key}.{scheduleId}";

    /// <summary>
    /// "_sched.{stream}.{subject}.{key}.*": every holder of one key. The schedule id is exactly one token, so '*'
    /// matches only this subject's holders. A '>' would also match the holders of a longer subject: the filter of
    /// subject "a" with key "b" would cover every holder of subject "a.b".
    /// </summary>
    public static string HolderFilter(string stream, string subject, string key) => $"_sched.{stream}.{subject}.{key}.*";

    /// <summary>
    /// Throws <see cref="ArgumentException"/> unless <paramref name="key"/> is one token of [A-Za-z0-9_-],
    /// at most <see cref="MaxKeyLength"/> characters.
    /// </summary>
    public static void ValidateKey(string? key, string parameter)
    {
        if (key == null || key.Length > MaxKeyLength || !KeyPattern().IsMatch(key))
        {
            throw new ArgumentException(
                $"Schedule key '{key}' is invalid: use 1 to {MaxKeyLength} characters of A-Z, a-z, 0-9, '_' and '-'",
                parameter);
        }
    }

    /// <summary>
    /// Throws <see cref="ArgumentException"/> unless <paramref name="scheduleId"/> is one token of [A-Za-z0-9_-], at
    /// most <see cref="MaxKeyLength"/> characters (it is the last token of the holder subject).
    /// </summary>
    public static void ValidateScheduleId(string scheduleId, string parameter)
    {
        if (scheduleId.Length > MaxKeyLength || !KeyPattern().IsMatch(scheduleId))
        {
            throw new ArgumentException(
                $"Schedule id '{scheduleId}' is invalid: use 1 to {MaxKeyLength} characters of A-Z, a-z, 0-9, '_' and '-'",
                parameter);
        }
    }

    /// <summary>
    /// Throws <see cref="ArgumentException"/> unless <paramref name="subject"/> is a literal subject outside the
    /// holder namespace.
    /// </summary>
    public static void ValidateSubject(string subject, string parameter)
    {
        if (!SubjectMatcher.IsValid(subject) || subject.Contains('*') || subject.Contains('>'))
        {
            throw new ArgumentException($"Schedule subject '{subject}' must be a valid subject without wildcards", parameter);
        }

        if (subject.StartsWith("_sched.", StringComparison.Ordinal))
        {
            throw new ArgumentException($"Schedule subject '{subject}' is inside the schedule holder namespace", parameter);
        }
    }

    /// <summary>
    /// The checked stream that holds schedules for <paramref name="subject"/>.
    /// </summary>
    /// <exception cref="NatsMessagingConfigurationException">No stream, several streams, or a stream that cannot
    /// hold schedules.</exception>
    public async Task<ScheduleTarget> ResolveAsync(string subject, CancellationToken cancellationToken)
    {
        var stream = await ResolveStreamAsync(subject, cancellationToken).ConfigureAwait(false);
        var check = _checked.AddOrUpdate(stream, NewCheck, (name, existing) => IsStale(existing) ? NewCheck(name) : existing);

        try
        {
            return await check.Target.Value.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OperationCanceledException || check.Target.Value.IsCompleted)
        {
            // A failure is not cached: the next call checks again.
            _checked.TryRemove(new KeyValuePair<string, Check>(stream, check));
            throw;
        }
    }

    private Check NewCheck(string stream) =>
        new(new Lazy<Task<ScheduleTarget>>(() => CheckAsync(stream, CancellationToken.None)), Stopwatch.GetTimestamp());

    // A check still running is shared; a completed one is read again once it is older than the refresh interval.
    private bool IsStale(Check check) =>
        check.Target.IsValueCreated && check.Target.Value.IsCompletedSuccessfully &&
        Stopwatch.GetElapsedTime(check.Started) >= refreshInterval;

    /// <summary>
    /// Forgets the check of <paramref name="stream"/> and the subjects resolved to it, after the server rejected a
    /// schedule on it or found no stream for its holder (the stream was deleted or replaced).
    /// </summary>
    public void Invalidate(string stream)
    {
        _checked.TryRemove(stream, out _);
        foreach (var (subject, resolved) in _streamBySubject)
        {
            if (resolved.Stream == stream)
            {
                _streamBySubject.TryRemove(new KeyValuePair<string, Resolved>(subject, resolved));
            }
        }
    }

    /// <summary>
    /// The number of subjects resolved through the server and still cached.
    /// </summary>
    internal int CachedSubjects => _streamBySubject.Count;

    private async Task<string> ResolveStreamAsync(string subject, CancellationToken cancellationToken)
    {
        var declared = definitions.FirstOrDefault(d => SubjectMatcher.IsCoveredBy(subject, d.Subjects));
        if (declared != null)
        {
            return declared.Name;
        }

        if (_streamBySubject.TryGetValue(subject, out var cached) &&
            Stopwatch.GetElapsedTime(cached.At) < refreshInterval)
        {
            return cached.Stream;
        }

        var names = new List<string>();
        await foreach (var name in js.ListStreamNamesAsync(subject, cancellationToken).ConfigureAwait(false))
        {
            names.Add(name);
        }

        return names.Count switch
        {
            1 => Remember(subject, names[0]),
            0 => throw new NatsMessagingConfigurationException(
                $"No stream captures '{subject}', so it cannot be scheduled. Declare a stream with " +
                "AllowMsgSchedules (StreamDefinition.Scheduled) that captures it."),
            _ => throw new NatsMessagingConfigurationException(
                $"Several streams capture '{subject}' ({string.Join(", ", names)}), so the schedule target is ambiguous")
        };
    }

    private async Task<ScheduleTarget> CheckAsync(string stream, CancellationToken cancellationToken)
    {
        INatsJSStream info;
        try
        {
            info = await js.GetStreamAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (NatsJSApiException e) when (e.Error.ErrCode == StreamNotFound)
        {
            throw new NatsMessagingConfigurationException(
                $"Stream '{stream}' does not exist (yet), so it cannot hold schedules", e);
        }

        var config = info.Info.Config;
        var problems = ScheduleStreamRules.Check(config);
        if (problems.Count > 0)
        {
            throw new NatsMessagingConfigurationException(
                $"Stream '{stream}' cannot hold message schedules: {string.Join("; ", problems)}.");
        }

        if (!_warned.TryAdd(stream, 0))
        {
            return new ScheduleTarget(stream, config.MaxAge > TimeSpan.Zero ? config.MaxAge : TimeSpan.Zero);
        }

        if (ScheduleStreamRules.Warning(config) is { } warning)
        {
            logger.LogWarning("Stream {Stream}: {Warning}", stream, warning);
        }

        string? level = null;
        config.Metadata?.TryGetValue("_nats.level", out level);
        if (!int.TryParse(level, out var apiLevel) || apiLevel < RequiredApiLevel)
        {
            logger.LogWarning(
                "Stream {Stream} reports JetStream API level {Level}; message schedules need level {Required} " +
                "(nats-server 2.12+) on every server that hosts it",
                stream, level ?? "(none)", RequiredApiLevel);
        }

        return new ScheduleTarget(stream, config.MaxAge > TimeSpan.Zero ? config.MaxAge : TimeSpan.Zero);
    }

    /// <summary>
    /// Caches a server answer. At the limit, expired answers go first, then everything: the cache is a shortcut, and
    /// a miss costs one STREAM.NAMES request.
    /// </summary>
    private string Remember(string subject, string stream)
    {
        if (_streamBySubject.Count >= maxCachedSubjects)
        {
            foreach (var (cachedSubject, resolved) in _streamBySubject)
            {
                if (Stopwatch.GetElapsedTime(resolved.At) >= refreshInterval)
                {
                    _streamBySubject.TryRemove(new KeyValuePair<string, Resolved>(cachedSubject, resolved));
                }
            }

            if (_streamBySubject.Count >= maxCachedSubjects)
            {
                _streamBySubject.Clear();
            }
        }

        _streamBySubject[subject] = new Resolved(stream, Stopwatch.GetTimestamp());
        return stream;
    }

    private sealed record Check(Lazy<Task<ScheduleTarget>> Target, long Started);

    private sealed record Resolved(string Stream, long At);

    [GeneratedRegex(@"^[A-Za-z0-9_-]+\z")]
    private static partial Regex KeyPattern();
}
