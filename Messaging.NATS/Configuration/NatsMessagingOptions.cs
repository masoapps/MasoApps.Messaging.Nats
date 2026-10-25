using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using NATS.Client.Core;

namespace Messaging.NATS;

public class NatsMessagingOptions
{
    /// <summary>
    /// The URL of the NATS server
    /// </summary>
    public string NatsUrl { get; set; } = string.Empty;

    /// <summary>
    /// Path to the NATS credentials file
    /// </summary>
    public string? CredsFilePath { get; set; }

    /// <summary>
    /// Optional JSON serializer context for source-generated serialization.
    /// When not provided, reflection-based JSON serialization is used automatically.
    /// </summary>
    public JsonSerializerContext? SerializerContext { get; set; }

    /// <summary>
    /// Optional JSON serializer options for reflection-based serialization.
    /// Only used when SerializerContext is not provided.
    /// </summary>
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// Assemblies to scan for message handlers
    /// </summary>
    public Assembly[] Assemblies { get; set; } = Array.Empty<Assembly>();

    /// <summary>
    /// Handler types to register explicitly, in addition to the ones found in <see cref="Assemblies"/>.
    /// </summary>
    public Type[] HandlerTypes { get; set; } = Array.Empty<Type>();

    /// <summary>
    /// Default queue group (durable name prefix) for consumers whose attribute does not set one.
    /// When null, <see cref="ServiceName"/> is used. Every consumer needs a queue group: from
    /// <see cref="NatsConsumerAttribute.QueueGroup"/>, from this, or from <see cref="ServiceName"/>.
    /// </summary>
    public string? DefaultQueueGroup { get; set; }

    /// <summary>
    /// Name of this service. Used as the NATS connection name, as the queue group fallback, and as the owner of this
    /// service's durables: new durables get it as their messaging-nats.owner metadata, a durable owned by another
    /// ServiceName is never updated (the consumer is Faulted instead of taking it over), and orphaned durables are
    /// reported only for this owner. When it is null, the owner is the queue group, so two services that share a
    /// queue group and a handler class name are not told apart: they share one durable and overwrite each other's
    /// filters. Set it on every service.
    /// </summary>
    public string? ServiceName { get; set; }

    /// <summary>
    /// Stream definitions to provision. The list is copied at registration and never mutated.
    /// </summary>
    public List<StreamDefinition> StreamDefinitions { get; set; } = new();

    /// <summary>
    /// How the declared <see cref="StreamDefinitions"/> are provisioned on startup.
    /// </summary>
    public StreamProvisioning StreamProvisioning { get; set; } = StreamProvisioning.CreateIfMissing;

    /// <summary>
    /// The dead-letter stream. It is always provisioned with <see cref="StreamProvisioning.CreateIfMissing"/>,
    /// whatever <see cref="StreamProvisioning"/> says. Set to null when another service owns it; a stream must
    /// then capture <see cref="DeadLetterSubjectPrefix"/>.
    /// </summary>
    public StreamDefinition? DeadLetterStream { get; set; } = StreamDefinition.DeadLetter();

    /// <summary>
    /// Prefix of the default dead-letter subject ("{prefix}.{subject}").
    /// </summary>
    public string DeadLetterSubjectPrefix { get; set; } = "dlq";

    /// <summary>
    /// Keep retrying the first connection until the server is reachable.
    /// Set to false for short-lived jobs that should fail fast.
    /// </summary>
    public bool RetryOnInitialConnect { get; set; } = true;

    /// <summary>
    /// Runs last on the NATS.Net connection options, for settings the library does not expose (for example
    /// ReconnectWaitMin). The input already has the library's URL, name, serializer and credentials; keep them.
    /// </summary>
    public Func<NatsOpts, NatsOpts>? ConfigureNatsOpts { get; set; }

    /// <summary>
    /// When true, host start blocks until every component is running, and throws when one is faulted or
    /// <see cref="StartTimeout"/> elapses. When false, start returns at once and readiness comes from the health check.
    /// </summary>
    public bool WaitUntilStarted { get; set; }

    /// <summary>
    /// How long <see cref="WaitUntilStarted"/> waits.
    /// </summary>
    public TimeSpan StartTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How long shutdown waits for in-flight handlers.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(25);

    /// <summary>
    /// What happens when a component hits a permanent (configuration) error.
    /// </summary>
    public FaultAction OnComponentFaulted { get; set; } = FaultAction.StopApplication;

    /// <summary>
    /// How often a faulted component retries.
    /// </summary>
    public TimeSpan FaultedRetryInterval { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// The whole time budget of one publish, retries included.
    /// </summary>
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Extra publish attempts inside <see cref="PublishTimeout"/>, all with the same message id.
    /// </summary>
    public int PublishRetryAttempts { get; set; } = 2;

    /// <summary>
    /// Deliveries above a consumer's MaxDeliveryAttempts before a message is dead-lettered without running the handler.
    /// It also bounds the hand-backs of a message no binding of the process matches (a rolling deploy that adds a
    /// binding): such a message is handed back while its delivery count is at most this value, so a process that has
    /// the binding always runs its handler.
    /// </summary>
    public int MaxDeliverySlack { get; set; } = 5;

    /// <summary>
    /// The longest a handler may run on one message, for consumers whose attribute does not set
    /// <see cref="NatsConsumerAttribute.MaxHandlerSeconds"/>. <see cref="TimeSpan.Zero"/> or
    /// <see cref="Timeout.InfiniteTimeSpan"/> means no limit.
    /// </summary>
    public TimeSpan MaxHandlerTime { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Attribute instances to use instead of a handler class's own [NatsConsumer] attributes. Attribute arguments
    /// are constants, so tests use this to bind one handler class to unique stream and subject names.
    /// </summary>
    internal Dictionary<Type, NatsConsumerAttribute[]> ConsumerAttributeOverrides { get; set; } = new();

    internal ConsumerTestHooks ConsumerTestHooks { get; set; } = new();

    internal PublisherTestHooks PublisherTestHooks { get; set; } = new();
}

/// <summary>
/// Publisher knobs that only tests change.
/// </summary>
internal sealed class PublisherTestHooks
{
    /// <summary>
    /// How long the check of a schedule stream (its rules and MaxAge) is reused before the stream is read again.
    /// </summary>
    public TimeSpan ScheduleTargetRefreshInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Called with (subject, attempt) when a positive PubAck arrives. True drops it, as if the reply was lost after
    /// the stream stored the message.
    /// </summary>
    public Func<string, int, bool>? DropPubAck { get; set; }

    /// <summary>
    /// Runs after a CreateIfAbsent got 10071 and before it reads the pending holder.
    /// </summary>
    public Func<Task>? BeforePendingLookup { get; set; }

    /// <summary>
    /// Runs after a Replace stored its holder and before it purges the holders it replaces. An exception from it
    /// counts as a failed purge.
    /// </summary>
    public Func<Task>? BeforeReplacePurge { get; set; }
}

/// <summary>
/// Knobs that only tests change.
/// </summary>
internal sealed class ConsumerTestHooks
{
    /// <summary>
    /// Turns off the in-progress heartbeat, so a slow handler's message is redelivered after AckWait.
    /// </summary>
    public bool DisableHeartbeat { get; set; }

    /// <summary>
    /// How often a running worker reads its ConsumerInfo (ack floor, MaxDeliver, and whether the durable still exists).
    /// </summary>
    public TimeSpan InfoRefreshInterval { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How long fetches may keep ending with no responders or a protocol error, with nothing delivered, before the
    /// consumer reports that it is stalled.
    /// </summary>
    public TimeSpan StallAfter { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The waits between the inline attempts of one dead-letter write (design 2.6: 1 s, 2 s, 4 s).
    /// </summary>
    public TimeSpan[] DeadLetterRetryDelays { get; set; } =
        [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4)];

    /// <summary>
    /// The NAK delay after a dead-letter write failed every inline attempt.
    /// </summary>
    public TimeSpan DeadLetterNakDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Called when a fetch ends without a trouble notification. A non-null result is recorded as that fetch's
    /// trouble (as if it got no responders or missed heartbeats).
    /// </summary>
    public Func<string?>? FetchTrouble { get; set; }
}

public enum FaultAction
{
    /// <summary>
    /// Log Critical and stop the application, so the orchestrator restarts it.
    /// </summary>
    StopApplication,

    /// <summary>
    /// Keep running. The component stays Faulted and retries every <see cref="NatsMessagingOptions.FaultedRetryInterval"/>.
    /// </summary>
    Report
}
