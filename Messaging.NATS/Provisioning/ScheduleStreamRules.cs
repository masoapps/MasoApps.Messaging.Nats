using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// The rules a stream must follow for message schedules (ADR-51) to work without losing pending schedules.
/// </summary>
internal static class ScheduleStreamRules
{
    public static string HolderSubject(string streamName) => $"_sched.{streamName}.>";

    /// <summary>
    /// Problems with a declared definition. Empty when the definition has no schedules or follows every rule.
    /// </summary>
    public static IReadOnlyList<string> Check(StreamDefinition definition)
    {
        if (!definition.AllowMsgSchedules)
        {
            return [];
        }

        var problems = new List<string>();
        CheckCommon(definition.Retention, definition.Discard, problems);

        if (definition.MaxMsgs > 0)
        {
            problems.Add("MaxMsgs must be unlimited, because evicting messages deletes pending schedules");
        }

        if (definition.MaxBytes > 0)
        {
            problems.Add("MaxBytes must be unlimited, because evicting messages deletes pending schedules");
        }

        var holder = definition.HolderSubject;
        foreach (var subject in definition.Subjects.Where(s => s != holder && SubjectMatcher.Overlaps(s, holder)))
        {
            problems.Add($"subject '{subject}' overlaps the schedule holder namespace '{holder}'");
        }

        return problems;
    }

    /// <summary>
    /// Problems with a server stream config that should hold schedules.
    /// </summary>
    public static IReadOnlyList<string> Check(StreamConfig config)
    {
        var problems = new List<string>();

        if (!config.AllowMsgSchedules)
        {
            problems.Add("AllowMsgSchedules is off");
        }

        CheckCommon(config.Retention, config.Discard, problems);

        if (config.MaxMsgs > 0)
        {
            problems.Add($"MaxMsgs is {config.MaxMsgs}; it must be unlimited, because evicting messages deletes pending schedules");
        }

        if (config.MaxBytes > 0)
        {
            problems.Add($"MaxBytes is {config.MaxBytes}; it must be unlimited, because evicting messages deletes pending schedules");
        }

        if (config.MaxMsgsPerSubject > 0)
        {
            problems.Add($"MaxMsgsPerSubject is {config.MaxMsgsPerSubject}; it must be unlimited, because evicting messages deletes pending schedules");
        }

        var holder = HolderSubject(config.Name ?? string.Empty);
        if (config.Subjects == null || !config.Subjects.Contains(holder))
        {
            problems.Add($"the subjects do not include the schedule holder subject '{holder}'");
        }

        return problems;
    }

    /// <summary>
    /// A Limits stream without MaxAge keeps every fired message forever. This is allowed but worth a warning.
    /// </summary>
    public static string? Warning(StreamConfig config) =>
        config.AllowMsgSchedules && config.Retention == StreamConfigRetention.Limits && config.MaxAge <= TimeSpan.Zero
            ? "Limits retention with no MaxAge: fired schedules accumulate in the stream"
            : null;

    private static void CheckCommon(StreamConfigRetention retention, StreamConfigDiscard discard, List<string> problems)
    {
        if (retention == StreamConfigRetention.Interest)
        {
            problems.Add("Interest retention drops pending schedules, because no consumer is interested in the holder subjects; use WorkQueue or Limits");
        }

        if (discard == StreamConfigDiscard.New)
        {
            problems.Add("Discard New rejects new schedules when the stream is full; use Discard Old");
        }
    }
}
