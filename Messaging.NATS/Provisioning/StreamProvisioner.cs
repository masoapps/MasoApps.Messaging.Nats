using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// Creates, checks or updates one declared stream. Every update is an overlay on the server's current config:
/// fields that <see cref="StreamDefinition"/> does not model are kept, schedules (AllowMsgSchedules, AllowMsgTTL and
/// the holder subject) are never turned off, and the MaxAge of a stream that holds schedules is never lowered, because
/// the server would delete the pending schedules older than the new MaxAge. Turning schedules on fails while a
/// consumer of the stream would receive the holders (no filter, or a filter that matches the holder subject).
/// </summary>
internal sealed class StreamProvisioner(NatsJSContext js, ILogger<StreamProvisioner> logger)
{
    private const int StreamNotFound = 10059;
    private const int StreamNameInUse = 10058;

    /// <summary>
    /// Provisions <paramref name="definition"/> and returns a warning for the component (drift, schedule advice),
    /// or null. Throws <see cref="NatsMessagingConfigurationException"/> for a stream that cannot work as declared,
    /// and lets server errors through for the retry loop to classify.
    /// </summary>
    public async Task<string?> ProvisionAsync(StreamDefinition definition, StreamProvisioning mode,
        CancellationToken cancellationToken)
    {
        StreamConfig current;
        try
        {
            current = (await js.GetStreamAsync(definition.Name, cancellationToken: cancellationToken)).Info.Config;
        }
        catch (NatsJSApiException e) when (e.Error.ErrCode == StreamNotFound && mode != StreamProvisioning.VerifyOnly)
        {
            var created = await CreateAsync(definition, cancellationToken);
            if (created != null)
            {
                return JoinWarnings([ScheduleStreamRules.Warning(created)]);
            }

            current = (await js.GetStreamAsync(definition.Name, cancellationToken: cancellationToken)).Info.Config;
        }

        string? kept = null;
        switch (mode)
        {
            case StreamProvisioning.CreateIfMissing:
                current = await EnableSchedulesAsync(definition, current, cancellationToken);
                break;
            case StreamProvisioning.VerifyOnly:
                current = Verify(definition, current);
                break;
            case StreamProvisioning.CreateOrUpdate:
                (current, kept) = await UpdateAsync(definition, current, cancellationToken);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
        }

        var drift = Drift(definition, current);
        if (drift.Count > 0)
        {
            logger.LogWarning(
                "Stream {Stream} differs from its definition ({Mode} leaves it as it is): {Drift}",
                definition.Name, mode, string.Join(", ", drift));
        }

        return JoinWarnings([
            kept,
            drift.Count > 0 ? $"differs from its definition: {string.Join(", ", drift)}" : null,
            ScheduleStreamRules.Warning(current)
        ]);
    }

    private async Task<StreamConfig?> CreateAsync(StreamDefinition definition, CancellationToken cancellationToken)
    {
        try
        {
            var stream = await js.CreateStreamAsync(definition.ToStreamConfig(), cancellationToken);
            logger.LogInformation("Created stream {Stream}", definition.Name);
            return stream.Info.Config;
        }
        catch (NatsJSApiException e) when (e.Error.ErrCode == StreamNameInUse)
        {
            // Created by someone else in the meantime, with a different config.
            return null;
        }
    }

    private async Task<StreamConfig> EnableSchedulesAsync(StreamDefinition definition, StreamConfig current,
        CancellationToken cancellationToken)
    {
        if (!definition.AllowMsgSchedules)
        {
            return current;
        }

        var holder = definition.HolderSubject;
        if (current.AllowMsgSchedules && HasSubject(current, holder))
        {
            ThrowOnRuleViolations(definition.Name, current);
            return current;
        }

        var enabled = current with
        {
            AllowMsgSchedules = true,
            Subjects = (current.Subjects ?? []).Append(holder).Distinct().ToList()
        };
        ThrowOnRuleViolations(definition.Name, enabled);
        await ThrowOnHolderConsumersAsync(definition.Name, cancellationToken);

        var updated = (await js.UpdateStreamAsync(enabled, cancellationToken)).Info.Config;
        logger.LogInformation("Turned on message schedules for stream {Stream} and added the subject {Holder}",
            definition.Name, holder);
        return updated;
    }

    private static StreamConfig Verify(StreamDefinition definition, StreamConfig current)
    {
        if (!definition.AllowMsgSchedules)
        {
            return current;
        }

        if (!current.AllowMsgSchedules || !HasSubject(current, definition.HolderSubject))
        {
            throw new NatsMessagingConfigurationException(
                $"Stream '{definition.Name}' is declared with AllowMsgSchedules, but the server stream " +
                (current.AllowMsgSchedules
                    ? $"does not capture the schedule holder subject '{definition.HolderSubject}'."
                    : "has schedules turned off.") +
                " The stream owner must turn them on (VerifyOnly never changes a stream).");
        }

        ThrowOnRuleViolations(definition.Name, current);
        return current;
    }

    private async Task<(StreamConfig Config, string? Warning)> UpdateAsync(StreamDefinition definition,
        StreamConfig current, CancellationToken cancellationToken)
    {
        var target = Overlay(current, definition);
        if (target.AllowMsgSchedules)
        {
            ThrowOnRuleViolations(definition.Name, target);
        }

        if (target.AllowMsgSchedules && !(current.AllowMsgSchedules && HasSubject(current, definition.HolderSubject)))
        {
            await ThrowOnHolderConsumersAsync(definition.Name, cancellationToken);
        }

        var warning = await KeptMaxAgeWarningAsync(definition, current, target, cancellationToken);

        var changes = Differences(target, current, ignoreHolder: false, compareDuplicateWindow: true);
        if (changes.Count == 0)
        {
            return (current, warning);
        }

        var updated = (await js.UpdateStreamAsync(target, cancellationToken)).Info.Config;
        logger.LogInformation("Updated stream {Stream}: {Changes}", definition.Name, string.Join(", ", changes));
        return (updated, warning);
    }

    /// <summary>
    /// When the overlay kept a schedule stream's MaxAge instead of lowering it, logs and returns why, with the
    /// number of pending schedules the lower MaxAge would put at risk.
    /// </summary>
    private async Task<string?> KeptMaxAgeWarningAsync(StreamDefinition definition, StreamConfig current,
        StreamConfig target, CancellationToken cancellationToken)
    {
        var declared = DeclaredMaxAge(definition);
        if (target.MaxAge == declared)
        {
            return null;
        }

        var holder = definition.HolderSubject;
        var info = await js.GetStreamAsync(definition.Name, new StreamInfoRequest { SubjectsFilter = holder },
            cancellationToken);
        var pending = info.Info.State.Subjects?.Values.Sum(count => (long)count) ?? 0;
        var warning =
            $"MaxAge kept at {Describe(current.MaxAge)}, not lowered to {Describe(declared)}: the stream holds message " +
            $"schedules ({pending} pending), and the server deletes every pending schedule older than the new MaxAge. " +
            "Lower it by hand (nats stream edit) once no schedule longer than the new MaxAge is pending.";
        logger.LogWarning("Stream {Stream}: {Warning}", definition.Name, warning);
        return warning;
    }

    /// <summary>
    /// Throws when a consumer of <paramref name="stream"/> would receive the schedule holders once schedules are on:
    /// one with no filter, or with a filter that matches "_sched.{stream}.>". Such a consumer takes each holder as
    /// soon as it is published, so the schedule is handled early and, on WorkQueue, never fires.
    /// </summary>
    private async Task ThrowOnHolderConsumersAsync(string stream, CancellationToken cancellationToken)
    {
        var holder = ScheduleStreamRules.HolderSubject(stream);
        var problems = new List<string>();
        await foreach (var consumer in js.ListConsumersAsync(stream, cancellationToken))
        {
            var config = consumer.Info.Config;
            var filters = (config.FilterSubjects ?? []).Append(config.FilterSubject ?? string.Empty)
                .Where(f => f.Length > 0).ToList();
            if (filters.Count == 0)
            {
                problems.Add($"{consumer.Info.Name} (no filter)");
            }
            else if (filters.FirstOrDefault(f => SubjectMatcher.Overlaps(f, holder)) is { } overlapping)
            {
                problems.Add($"{consumer.Info.Name} (filter '{overlapping}')");
            }
        }

        if (problems.Count > 0)
        {
            throw new NatsMessagingConfigurationException(
                $"Stream '{stream}' cannot turn on message schedules: these consumers would receive the pending " +
                $"schedules under '{holder}': {string.Join(", ", problems)}. Give each one a filter that only matches " +
                "the target subjects, or delete it, then start again.");
        }
    }

    /// <summary>
    /// The server config with every modelled field taken from the definition. Unmodelled fields are kept, schedules
    /// are never turned off, and the MaxAge of a stream that holds schedules is never lowered.
    /// </summary>
    internal static StreamConfig Overlay(StreamConfig current, StreamDefinition definition)
    {
        var holder = definition.HolderSubject;
        var keepHolder = definition.AllowMsgSchedules || (current.AllowMsgSchedules && HasSubject(current, holder));
        var subjects = definition.Subjects.Where(s => s != holder).ToList();
        if (keepHolder)
        {
            subjects.Add(holder);
        }

        var maxAge = DeclaredMaxAge(definition);
        if (current.AllowMsgSchedules && Lowers(current.MaxAge, maxAge))
        {
            maxAge = current.MaxAge;
        }

        var duplicateWindow = definition.DuplicateWindow ?? current.DuplicateWindow;
        if (definition.DuplicateWindow == null && maxAge > TimeSpan.Zero && duplicateWindow > maxAge)
        {
            duplicateWindow = maxAge;
        }

        return current with
        {
            Subjects = subjects,
            Retention = definition.Retention,
            MaxAge = maxAge,
            MaxBytes = definition.MaxBytes > 0 ? definition.MaxBytes : -1,
            MaxMsgs = definition.MaxMsgs > 0 ? definition.MaxMsgs : -1,
            NumReplicas = definition.Replicas,
            Storage = definition.Storage,
            Discard = definition.Discard,
            DuplicateWindow = duplicateWindow,
            AllowMsgSchedules = current.AllowMsgSchedules || definition.AllowMsgSchedules
        };
    }

    /// <summary>
    /// The modelled fields where the server stream differs from the definition. The holder subject is left out of
    /// the subject comparison; AllowMsgSchedules covers it.
    /// </summary>
    internal static IReadOnlyList<string> Drift(StreamDefinition definition, StreamConfig server) =>
        Differences(definition.ToStreamConfig(), server, ignoreHolder: true,
            compareDuplicateWindow: definition.DuplicateWindow != null);

    private static List<string> Differences(StreamConfig expected, StreamConfig actual, bool ignoreHolder,
        bool compareDuplicateWindow)
    {
        var holder = ScheduleStreamRules.HolderSubject(expected.Name ?? string.Empty);
        var differences = new List<string>();

        var expectedSubjects = SubjectSet(expected, ignoreHolder ? holder : null);
        var actualSubjects = SubjectSet(actual, ignoreHolder ? holder : null);
        if (!expectedSubjects.SetEquals(actualSubjects))
        {
            differences.Add($"Subjects (server [{string.Join(", ", actualSubjects.Order())}], " +
                            $"definition [{string.Join(", ", expectedSubjects.Order())}])");
        }

        Compare("Retention", expected.Retention, actual.Retention);
        Compare("MaxAge", expected.MaxAge, actual.MaxAge);
        Compare("MaxBytes", Unlimited(expected.MaxBytes), Unlimited(actual.MaxBytes));
        Compare("MaxMsgs", Unlimited(expected.MaxMsgs), Unlimited(actual.MaxMsgs));
        Compare("Replicas", Math.Max(expected.NumReplicas, 1), Math.Max(actual.NumReplicas, 1));
        Compare("Storage", expected.Storage, actual.Storage);
        Compare("Discard", expected.Discard, actual.Discard);
        if (compareDuplicateWindow)
        {
            Compare("DuplicateWindow", expected.DuplicateWindow, actual.DuplicateWindow);
        }

        Compare("AllowMsgSchedules", expected.AllowMsgSchedules, actual.AllowMsgSchedules);
        return differences;

        void Compare<T>(string field, T definition, T server)
        {
            if (!EqualityComparer<T>.Default.Equals(definition, server))
            {
                differences.Add($"{field} (server {server}, definition {definition})");
            }
        }
    }

    private static TimeSpan DeclaredMaxAge(StreamDefinition definition) =>
        definition.MaxAge > 0 ? TimeSpan.FromSeconds(definition.MaxAge) : TimeSpan.Zero;

    // Zero is unlimited, so any limit lowers it.
    private static bool Lowers(TimeSpan current, TimeSpan next) =>
        next > TimeSpan.Zero && (current <= TimeSpan.Zero || next < current);

    private static string Describe(TimeSpan maxAge) => maxAge > TimeSpan.Zero ? maxAge.ToString() : "unlimited";

    private static HashSet<string> SubjectSet(StreamConfig config, string? without) =>
        (config.Subjects ?? []).Where(s => s != without).ToHashSet();

    private static long Unlimited(long value) => value > 0 ? value : -1;

    private static bool HasSubject(StreamConfig config, string subject) => config.Subjects?.Contains(subject) == true;

    private static void ThrowOnRuleViolations(string stream, StreamConfig config)
    {
        var problems = ScheduleStreamRules.Check(config);
        if (problems.Count > 0)
        {
            throw new NatsMessagingConfigurationException(
                $"Stream '{stream}' cannot hold message schedules: {string.Join("; ", problems)}.");
        }
    }

    private static string? JoinWarnings(IEnumerable<string?> warnings)
    {
        var list = warnings.Where(w => w != null).ToList();
        return list.Count == 0 ? null : string.Join("; ", list);
    }
}
