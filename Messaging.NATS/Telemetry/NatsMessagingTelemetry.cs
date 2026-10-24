using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using NATS.Client.Core;

namespace Messaging.NATS;

/// <summary>
/// The OpenTelemetry names of this library. Register <see cref="SourceName"/> as both an activity source and a
/// meter, together with "NATS.Net". The README lists every span, attribute and metric.
/// </summary>
public static class NatsMessagingTelemetry
{
    public const string SourceName = "Messaging.NATS";

    private static readonly string? Version =
        typeof(NatsMessagingTelemetry).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
            ?.InformationalVersion;

    internal static readonly ActivitySource ActivitySource = new(SourceName, Version);

    internal static readonly Meter Meter = new(SourceName, Version);

    /// <summary>
    /// Time from dispatch to settle of one delivery, in seconds. Tags: consumer group (durable), outcome.
    /// </summary>
    internal static readonly Histogram<double> ProcessDuration =
        Meter.CreateHistogram<double>("messaging.process.duration", "s", "Time to process one delivery");

    /// <summary>
    /// Deliveries processed. Tags: consumer group (durable), outcome.
    /// </summary>
    internal static readonly Counter<long> ConsumedMessages =
        Meter.CreateCounter<long>("messaging.client.consumed.messages", "{message}", "Deliveries processed");

    /// <summary>
    /// Duration of one publish, schedule or dead-letter write, retries included, in seconds.
    /// Tags: operation, destination, outcome.
    /// </summary>
    internal static readonly Histogram<double> OperationDuration =
        Meter.CreateHistogram<double>("messaging.client.operation.duration", "s", "Duration of a publish");

    /// <summary>
    /// Handlers running now. Tag: consumer group (durable).
    /// </summary>
    internal static readonly UpDownCounter<long> InFlight =
        Meter.CreateUpDownCounter<long>("messaging.nats.inflight", "{message}", "Deliveries being processed");

    /// <summary>
    /// A dead-letter write failed; the message was NAKed and stays in its stream.
    /// </summary>
    internal static readonly Counter<long> DeadLetterFailures =
        Meter.CreateCounter<long>("messaging.nats.dead_letter.failures", "{message}", "Failed dead-letter writes");

    /// <summary>
    /// A durable this process had seen was gone and has been recreated.
    /// </summary>
    internal static readonly Counter<long> ConsumerRecreated =
        Meter.CreateCounter<long>("messaging.nats.consumer.recreated", "{consumer}", "Durables recreated");

    /// <summary>
    /// The server rejected the update of an existing durable; the worker consumes with the server's config.
    /// </summary>
    internal static readonly Counter<long> ConsumerConfigRejected =
        Meter.CreateCounter<long>("messaging.nats.consumer.config_rejected", "{consumer}", "Rejected durable updates");

    /// <summary>
    /// The last orphan report of each running host, per stream. Read by the orphans gauge.
    /// </summary>
    private static readonly ConcurrentDictionary<(object Owner, string Stream), int> OrphanCounts = new();

    internal static readonly ObservableGauge<int> Orphans = Meter.CreateObservableGauge(
        "messaging.nats.consumer.orphans",
        () => OrphanCounts.Select(e => new Measurement<int>(e.Value, Stream(e.Key.Stream))),
        "{consumer}",
        "Consumers this service owns that no handler uses any more");

    internal const string System = "messaging.system";
    internal const string OperationName = "messaging.operation.name";
    internal const string OperationType = "messaging.operation.type";
    internal const string Destination = "messaging.destination.name";
    internal const string ConsumerGroup = "messaging.consumer.group.name";
    internal const string MessageIdTag = "messaging.message.id";
    internal const string ConversationId = "messaging.message.conversation_id";
    internal const string BodySize = "messaging.message.body.size";
    internal const string StreamTag = "messaging.nats.stream";
    internal const string StreamSequence = "messaging.nats.stream_sequence";
    internal const string DeliveryCount = "messaging.nats.delivery_count";
    internal const string Handler = "messaging.nats.handler";
    internal const string ScheduleIdTag = "messaging.nats.schedule.id";
    internal const string ScheduleKey = "messaging.nats.schedule.key";
    internal const string ScheduleMode = "messaging.nats.schedule.mode";
    internal const string ScheduleDeliverAt = "messaging.nats.schedule.deliver_at";
    internal const string Outcome = "messaging.nats.outcome";
    internal const string PublishAttempts = "messaging.nats.publish.attempts";
    internal const string ErrorType = "error.type";

    internal const string TraceParent = "traceparent";
    internal const string TraceState = "tracestate";

    internal static KeyValuePair<string, object?> Consumer(string durable) => new(ConsumerGroup, durable);

    internal static KeyValuePair<string, object?> Stream(string stream) => new(StreamTag, stream);

    internal static void SetOrphans(object owner, string stream, int count) => OrphanCounts[(owner, stream)] = count;

    internal static void ClearOrphans(object owner)
    {
        foreach (var key in OrphanCounts.Keys.Where(k => ReferenceEquals(k.Owner, owner)))
        {
            OrphanCounts.TryRemove(key, out _);
        }
    }

    /// <summary>
    /// The trace context a publisher or the scheduler wrote into the headers, or default when there is none.
    /// </summary>
    internal static ActivityContext ParentContext(NatsHeaders? headers)
    {
        var traceParent = MessageContext<object>.Header(headers, TraceParent);
        return traceParent != null &&
               ActivityContext.TryParse(traceParent, MessageContext<object>.Header(headers, TraceState), true,
                   out var context)
            ? context
            : default;
    }

    /// <summary>
    /// Writes the current trace context (the publish span, else the caller's activity) as traceparent and
    /// tracestate. The caller's headers never carry one here: <see cref="HeaderSanitiser.Copy"/> drops a forwarded
    /// trace context, which would parent the next consumer on the first producer instead of this publish.
    /// </summary>
    internal static void Inject(NatsHeaders headers, Activity? activity)
    {
        var current = activity ?? Activity.Current;
        if (current == null || current.IdFormat != ActivityIdFormat.W3C)
        {
            return;
        }

        headers[TraceParent] = current.Id;
        if (!string.IsNullOrEmpty(current.TraceStateString))
        {
            headers[TraceState] = current.TraceStateString;
        }
    }

    /// <summary>
    /// Starts a Producer span "{operation} {subject}" (publish, schedule).
    /// </summary>
    internal static Activity? StartProducer(string operation, string subject, string? messageId)
    {
        if (!ActivitySource.HasListeners())
        {
            return null;
        }

        var activity = ActivitySource.StartActivity($"{operation} {subject}", ActivityKind.Producer);
        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag(System, "nats");
            activity.SetTag(OperationName, operation);
            activity.SetTag(OperationType, operation == "publish" ? "send" : "create");
            activity.SetTag(Destination, subject);
            activity.SetTag(MessageIdTag, messageId);
        }

        return activity;
    }

    /// <summary>
    /// Records one publish-like operation: the duration metric, and the outcome and error on the span.
    /// </summary>
    internal static void RecordOperation(Activity? activity, string operation, string subject, long startedTimestamp,
        string outcome, Exception? error)
    {
        var elapsed = Stopwatch.GetElapsedTime(startedTimestamp).TotalSeconds;
        var tags = new TagList
        {
            { System, "nats" },
            { OperationName, operation },
            { Destination, subject },
            { Outcome, outcome }
        };
        if (error != null)
        {
            tags.Add(ErrorType, ErrorTypeOf(error));
        }

        OperationDuration.Record(elapsed, tags);

        if (activity == null)
        {
            return;
        }

        activity.SetTag(Outcome, outcome);
        if (error != null)
        {
            SetError(activity, error, ErrorTypeOf(error));
        }
    }

    /// <summary>
    /// The outcome tag of a failed publish.
    /// </summary>
    internal static string PublishOutcome(Exception error) => error switch
    {
        NatsPublishException { Failure: PublishFailure.NoStream } => "no_stream",
        NatsPublishException { Failure: PublishFailure.Rejected } => "rejected",
        NatsPublishException { Failure: PublishFailure.Timeout } => "timeout",
        NatsMessagingConfigurationException => "rejected",
        OperationCanceledException => "cancelled",
        _ => "error"
    };

    internal static void SetError(Activity activity, Exception? error, string errorType, string? description = null)
    {
        activity.SetTag(ErrorType, errorType);
        activity.SetStatus(ActivityStatusCode.Error, description ?? error?.Message);
        if (error != null)
        {
            activity.AddException(error);
        }
    }

    internal static string ErrorTypeOf(Exception error) => error.GetType().FullName ?? error.GetType().Name;
}

/// <summary>
/// The value of <c>messaging.nats.outcome</c> for one delivery.
/// </summary>
internal static class ProcessOutcome
{
    public const string Ack = "ack";
    public const string Retry = "retry";
    public const string DeadLetter = "dead_letter";
    public const string Skipped = "skipped";
    public const string Deferred = "deferred";
    public const string Handoff = "handoff";

    /// <summary>
    /// A legacy handler called AckTerminate itself.
    /// </summary>
    public const string Terminated = "terminated";
}
