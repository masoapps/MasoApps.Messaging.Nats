using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// Defines a NATS JetStream stream configuration
/// </summary>
public class StreamDefinition
{
    /// <summary>
    /// The name of the stream
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// The subjects that the stream should capture
    /// </summary>
    public string[] Subjects { get; set; } = [];

    /// <summary>
    /// The retention policy for the stream
    /// </summary>
    public StreamConfigRetention Retention { get; set; } = StreamConfigRetention.Limits;

    /// <summary>
    /// Maximum age of messages in the stream, in seconds (0 or less for no limit)
    /// </summary>
    public int MaxAge { get; set; } = -1;

    /// <summary>
    /// Maximum size of the stream in bytes (0 or less for no limit)
    /// </summary>
    public long MaxBytes { get; set; } = -1;

    /// <summary>
    /// Maximum number of messages in the stream (0 or less for no limit)
    /// </summary>
    public long MaxMsgs { get; set; } = -1;

    /// <summary>
    /// Number of replicas. Use 3 in production clusters.
    /// </summary>
    public int Replicas { get; set; } = 1;

    /// <summary>
    /// Storage backend. Cannot be changed once the stream exists.
    /// </summary>
    public StreamConfigStorage Storage { get; set; } = StreamConfigStorage.File;

    /// <summary>
    /// What happens when the stream reaches a limit.
    /// </summary>
    public StreamConfigDiscard Discard { get; set; } = StreamConfigDiscard.Old;

    /// <summary>
    /// The window in which a repeated message id is dropped as a duplicate. Null uses the server default (2 minutes).
    /// </summary>
    public TimeSpan? DuplicateWindow { get; set; }

    /// <summary>
    /// Whether to allow native message scheduling on this stream (requires NATS Server 2.12+).
    /// The stream then also captures "_sched.{Name}.>", where pending schedules are held.
    /// </summary>
    public bool AllowMsgSchedules { get; set; }

    internal string HolderSubject => ScheduleStreamRules.HolderSubject(Name);

    /// <summary>
    /// Converts this definition to a NATS StreamConfig
    /// </summary>
    public StreamConfig ToStreamConfig()
    {
        var subjects = Subjects.ToList();
        if (AllowMsgSchedules && !subjects.Contains(HolderSubject))
        {
            subjects.Add(HolderSubject);
        }

        var config = new StreamConfig(Name, subjects)
        {
            Retention = Retention,
            MaxMsgs = MaxMsgs > 0 ? MaxMsgs : -1,
            MaxBytes = MaxBytes > 0 ? MaxBytes : -1,
            NumReplicas = Replicas,
            Storage = Storage,
            Discard = Discard,
            AllowMsgSchedules = AllowMsgSchedules
        };

        if (MaxAge > 0)
        {
            config.MaxAge = TimeSpan.FromSeconds(MaxAge);
        }

        if (DuplicateWindow is { } window)
        {
            config.DuplicateWindow = window;
        }

        return config;
    }

    /// <summary>
    /// A stream for scheduled messages: WorkQueue retention, schedules on, and no age, count or byte limits.
    /// Timer subjects should live in this stream only.
    /// </summary>
    public static StreamDefinition Scheduled(string name, params string[] subjects) => new()
    {
        Name = name,
        Subjects = subjects,
        Retention = StreamConfigRetention.Workqueue,
        AllowMsgSchedules = true
    };

    /// <summary>
    /// The dead-letter stream: Limits retention, 30 days MaxAge and a 1 hour duplicate window.
    /// </summary>
    public static StreamDefinition DeadLetter(string name = "DLQ", string subjects = "dlq.>") => new()
    {
        Name = name,
        Subjects = [subjects],
        Retention = StreamConfigRetention.Limits,
        MaxAge = (int)TimeSpan.FromDays(30).TotalSeconds,
        DuplicateWindow = TimeSpan.FromHours(1)
    };

    internal StreamDefinition Clone()
    {
        var copy = (StreamDefinition)MemberwiseClone();
        copy.Subjects = Subjects.ToArray();
        return copy;
    }
}
