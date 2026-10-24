using NATS.Client.Core;

namespace Messaging.NATS;

/// <summary>
/// Options for one publish. Every property is optional.
/// </summary>
public sealed record PublishOptions
{
    /// <summary>
    /// The Nats-Msg-Id the stream deduplicates on. Pass a stable business id for publishes that must be delivered,
    /// and keep the stream's DuplicateWindow longer than your own retry horizon. When null, an id is generated
    /// (Guid "N") only when <see cref="RetryAttempts"/> is above zero, so library retries are deduplicated.
    /// </summary>
    public string? MessageId { get; init; }

    /// <summary>
    /// Sent as the X-Correlation-Id header. When null, an X-Correlation-Id in <see cref="Headers"/> is kept.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Extra headers. They are copied, never changed. Nats-* headers and the library's own X-* headers are dropped,
    /// so forwarding a received message's headers cannot purge a schedule, roll up a subject or cause a duplicate drop.
    /// traceparent and tracestate are dropped too: the message carries the trace context of its own publish span.
    /// </summary>
    public NatsHeaders? Headers { get; init; }

    /// <summary>
    /// A core NATS publish instead of a JetStream publish: no ack and no retry. No message id is generated, but a
    /// <see cref="MessageId"/> that is set is still sent, so the stream still deduplicates on it (a later acked
    /// publish with the same id returns <see cref="PublishResult.Duplicate"/> = true).
    /// </summary>
    public bool FireAndForget { get; init; }

    /// <summary>
    /// The whole budget of the publish, retries and a reconnect wait included. Null uses
    /// <see cref="NatsMessagingOptions.PublishTimeout"/>.
    /// </summary>
    public TimeSpan? Timeout { get; init; }

    /// <summary>
    /// Extra attempts inside <see cref="Timeout"/>, all with the same message id. Null uses
    /// <see cref="NatsMessagingOptions.PublishRetryAttempts"/>.
    /// </summary>
    public int? RetryAttempts { get; init; }
}

/// <summary>
/// What a keyed schedule does when a schedule with the same key is already pending.
/// </summary>
public enum ScheduleMode
{
    /// <summary>
    /// The new schedule replaces the pending one.
    /// </summary>
    Replace,

    /// <summary>
    /// The pending schedule is kept and the new one is not stored. Use it only when the payload of the first
    /// schedule stays valid until it fires.
    /// </summary>
    CreateIfAbsent
}

/// <summary>
/// Options for one schedule. Every property is optional.
/// </summary>
public sealed record ScheduleOptions
{
    /// <summary>
    /// The schedule key: one subject token of [A-Za-z0-9_-], up to 128 characters. At most one schedule per
    /// (subject, key) is pending. When null, a unique key is used, so any number of schedules can be pending.
    /// </summary>
    public string? Key { get; init; }

    /// <summary>
    /// What happens when a schedule with the same key is pending. Only used when <see cref="Key"/> is set.
    /// </summary>
    public ScheduleMode Mode { get; init; } = ScheduleMode.Replace;

    /// <summary>
    /// The schedule id (one token of [A-Za-z0-9_-], up to 128 characters), sent as the holder's Nats-Msg-Id and as
    /// X-Schedule-Id. When null, a Guid "N" is generated. Set it to retry a call that ended with
    /// <see cref="NatsPublishException.OutcomeUnknown"/>: the retry with the same id is deduplicated within the
    /// stream's DuplicateWindow, so it never leaves a second pending schedule. Without it, retrying an unkeyed
    /// schedule after an unknown outcome can leave two schedules that both fire. A keyed Replace is safe to retry
    /// without it.
    /// </summary>
    public string? ScheduleId { get; init; }

    /// <summary>
    /// Sent as the X-Correlation-Id header, and copied onto the fired message.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Extra headers, sanitised like <see cref="PublishOptions.Headers"/>. They are copied onto the fired message.
    /// </summary>
    public NatsHeaders? Headers { get; init; }

    /// <summary>
    /// The whole budget of the call. Null uses <see cref="NatsMessagingOptions.PublishTimeout"/>.
    /// </summary>
    public TimeSpan? Timeout { get; init; }

    /// <summary>
    /// Extra attempts inside <see cref="Timeout"/>. Null uses <see cref="NatsMessagingOptions.PublishRetryAttempts"/>.
    /// Retries are always deduplicated, because the schedule id is the holder's message id.
    /// </summary>
    public int? RetryAttempts { get; init; }
}

/// <summary>
/// The stream's acknowledgement of a publish. A fire-and-forget publish returns ("", 0, false, id).
/// </summary>
/// <param name="Stream">The stream that stored the message.</param>
/// <param name="Sequence">The stream sequence of the message.</param>
/// <param name="Duplicate">True when the stream already had a message with this id. This is a success.</param>
/// <param name="MessageId">The Nats-Msg-Id that was sent, or null when none was sent.</param>
public sealed record PublishResult(string Stream, ulong Sequence, bool Duplicate, string? MessageId);

/// <summary>
/// The result of <see cref="IPublisher.ScheduleAsync{T}(string, T, DateTimeOffset)"/>.
/// </summary>
/// <param name="ScheduleId">Unique per schedule. The fired message carries it as X-Schedule-Id
/// (MessageContext.ScheduleId). For a CreateIfAbsent that lost, this is the pending schedule's id. When that schedule
/// fired before it could be read, the key is free and the call creates its own schedule.</param>
/// <param name="Subject">The subject the message is delivered to.</param>
/// <param name="Key">The schedule key; a generated one for an unkeyed schedule.</param>
/// <param name="Created">False when CreateIfAbsent found a pending schedule and kept it.</param>
/// <param name="Sequence">The stream sequence of the pending schedule.</param>
/// <param name="DeliverAt">The requested delivery time.</param>
public sealed record ScheduleResult(string ScheduleId, string Subject, string Key, bool Created, ulong Sequence,
    DateTimeOffset DeliverAt);

public enum PublishFailure
{
    /// <summary>
    /// No stream captures the subject. Nothing was stored.
    /// </summary>
    NoStream,

    /// <summary>
    /// The stream rejected the message; see <see cref="NatsPublishException.ErrorCode"/>.
    /// </summary>
    Rejected,

    /// <summary>
    /// The budget ran out. The message may have been stored: retry with the same message id (for a schedule, the
    /// same <see cref="ScheduleOptions.ScheduleId"/>, or the same key with <see cref="ScheduleMode.Replace"/>).
    /// </summary>
    Timeout
}

/// <summary>
/// A publish or schedule that did not get a positive acknowledgement within its budget.
/// </summary>
public sealed class NatsPublishException : Exception
{
    public NatsPublishException(PublishFailure failure, string subject, string? messageId, int? errorCode,
        string message, Exception? innerException = null)
        : base(message, innerException)
    {
        Failure = failure;
        Subject = subject;
        MessageId = messageId;
        ErrorCode = errorCode;
    }

    public PublishFailure Failure { get; }

    /// <summary>
    /// True when the message may have been stored. Retry with the same <see cref="MessageId"/>, and the stream
    /// deduplicates within its DuplicateWindow. For a schedule, <see cref="MessageId"/> is the schedule id: retry
    /// with it as <see cref="ScheduleOptions.ScheduleId"/>, or with the same key and <see cref="ScheduleMode.Replace"/>.
    /// An unkeyed schedule retried without its id (the 4.x PublishAsync with a delay cannot pass one) can leave two
    /// pending schedules.
    /// </summary>
    public bool OutcomeUnknown => Failure == PublishFailure.Timeout;

    public string Subject { get; }

    public string? MessageId { get; }

    /// <summary>
    /// The JetStream err_code when <see cref="Failure"/> is <see cref="PublishFailure.Rejected"/>.
    /// </summary>
    public int? ErrorCode { get; }
}
