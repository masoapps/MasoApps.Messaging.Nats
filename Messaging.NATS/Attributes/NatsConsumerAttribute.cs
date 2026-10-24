namespace Messaging.NATS;

/// <summary>
/// Binds a handler class to a stream subject. A class may carry several attributes: one per message type, subject or
/// stream. All bindings of one class on one stream share one durable consumer named
/// <see cref="Durable"/>, or "{QueueGroup}_{ClassName}".
/// <para>
/// Binding-level properties (<see cref="Subject"/>, <see cref="MessageType"/>, <see cref="MaxDeliveryAttempts"/>,
/// <see cref="RetryDelaysSeconds"/>, <see cref="DeadLetterSubject"/>) may differ between the attributes of a class.
/// Consumer-level properties (the rest) must agree: each one is set on one attribute only, or set to the same
/// value on several. Integers use 0 for "not set".
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = false)]
public sealed class NatsConsumerAttribute : Attribute
{
    /// <summary>
    /// The filter subject. Wildcards are allowed. Empty means the whole stream. That is only allowed for a durable's
    /// only binding, and never on a stream with message schedules.
    /// </summary>
    public string Subject { get; set; } = string.Empty;

    /// <summary>
    /// The message type this binding handles. Required when the class implements more than one handler interface.
    /// </summary>
    public Type? MessageType { get; set; }

    /// <summary>
    /// Handler failures before the message is dead-lettered. Deliveries where the handler did not run or did not
    /// finish (a shutdown, a crash) also count, so the handler may run fewer times. The delivery a 4.x
    /// Msg-DeferredUntil message spent waiting does not count. The handler never runs more than
    /// MaxDeliveryAttempts + <see cref="NatsMessagingOptions.MaxDeliverySlack"/> times.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 3;

    /// <summary>
    /// Delay in seconds before each retry. Index N is used after failed delivery N + 1, and the last entry repeats.
    /// Null uses [1, 5, 30, 120, 600]. An empty array retries at once.
    /// </summary>
    public int[]? RetryDelaysSeconds { get; set; }

    /// <summary>
    /// Where failed messages go. Null uses "{DeadLetterSubjectPrefix}.{subject the message was published to}".
    /// </summary>
    public string? DeadLetterSubject { get; set; }

    /// <summary>
    /// The stream to consume. Consumer level.
    /// </summary>
    public string Stream { get; set; } = string.Empty;

    /// <summary>
    /// Prefix of the durable name. Empty falls back to <see cref="NatsMessagingOptions.DefaultQueueGroup"/>, then
    /// <see cref="NatsMessagingOptions.ServiceName"/>. Consumer level.
    /// </summary>
    public string QueueGroup { get; set; } = string.Empty;

    /// <summary>
    /// The full durable name, used instead of "{QueueGroup}_{ClassName}". Consumer level.
    /// </summary>
    public string? Durable { get; set; }

    /// <summary>
    /// How long the server waits for a settle before it redelivers. 0 = 30 seconds. A running handler keeps its
    /// message with a heartbeat every third of the server's effective AckWait, so this does not limit how long a
    /// handler may run (<see cref="MaxHandlerSeconds"/> does). Consumer level.
    /// </summary>
    public int AckWaitSeconds { get; set; }

    /// <summary>
    /// The longest a handler may run on one message. 0 = <see cref="NatsMessagingOptions.MaxHandlerTime"/> (default
    /// 30 minutes). After it the handler's token is cancelled, its heartbeat stops (so the server redelivers the
    /// message after AckWait), an Error is logged and the consumer shows a Warning until the handler returns. A handler
    /// that throws or returns after that counts as a failed attempt. A handler that ignores its token keeps its slot
    /// until it returns. Consumer level.
    /// </summary>
    public int MaxHandlerSeconds { get; set; }

    /// <summary>
    /// Handlers running at once, per process. All bindings of the durable share them. 0 = 1. Consumer level.
    /// </summary>
    public int MaxConcurrency { get; set; }

    /// <summary>
    /// The most messages one pull request asks for. 0 = <see cref="MaxConcurrency"/>. A pull only asks for free
    /// handler slots, so a value above MaxConcurrency has no effect: there is no prefetch. Consumer level.
    /// </summary>
    public int MaxMsgs { get; set; }

    /// <summary>
    /// Unacknowledged messages allowed across all processes. 0 = 1000. Consumer level.
    /// </summary>
    public int MaxAckPending { get; set; }

    /// <summary>
    /// Where a new durable starts. Only used when the durable is first created. Consumer level.
    /// </summary>
    public DeliverFrom DeliverFrom { get; set; } = DeliverFrom.Default;

    /// <summary>
    /// The server deletes the durable after this long without a pull. 0 = never. A durable deleted while no process
    /// runs is recreated as a new one (see <see cref="DeliverFrom"/>), so use it only on WorkQueue streams, or on
    /// Limits streams with <see cref="NATS.DeliverFrom.All"/>. It is rejected on Interest streams: there the server
    /// removes the messages only the deleted durable had not acked, and stores none for it while it is gone.
    /// Consumer level.
    /// </summary>
    public int InactiveThresholdSeconds { get; set; }
}

/// <summary>
/// Where a new durable consumer starts.
/// </summary>
public enum DeliverFrom
{
    /// <summary>
    /// All on WorkQueue streams, otherwise New.
    /// </summary>
    Default,

    /// <summary>
    /// Every message still in the stream.
    /// </summary>
    All,

    /// <summary>
    /// Only messages stored after the durable is created. Not allowed on WorkQueue streams.
    /// </summary>
    New
}
