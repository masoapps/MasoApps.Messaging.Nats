using System.Globalization;
using NATS.Client.Core;

namespace Messaging.NATS;

/// <summary>
/// One delivery of a JetStream message to an <see cref="INatsHandler{T}"/>.
/// Tests build one with <see cref="Testing.TestMessageContext.Create{T}"/>.
/// </summary>
public sealed class MessageContext<T>
{
    internal MessageContext(T message, string subject, NatsHeaders? headers, int deliveryCount, string stream,
        string consumer, ulong streamSequence, DateTimeOffset enqueuedTime)
    {
        Message = message;
        Subject = subject;
        Headers = headers ?? new NatsHeaders();
        DeliveryCount = deliveryCount;
        Stream = stream;
        Consumer = consumer;
        StreamSequence = streamSequence;
        EnqueuedTime = enqueuedTime;

        CorrelationId = Header(Headers, HeaderSanitiser.CorrelationId);
        SentTime = ParseTime(Header(Headers, HeaderSanitiser.SentTime)) ?? enqueuedTime;
        ScheduleId = Header(Headers, HeaderSanitiser.ScheduleId);
        ScheduleKey = Header(Headers, HeaderSanitiser.ScheduleKey);
        ScheduledFor = ParseTime(Header(Headers, HeaderSanitiser.ScheduledFor));
        MessageId = ResolveMessageId(Headers, stream, streamSequence);
    }

    /// <summary>
    /// The deserialised payload. Never null: null or unreadable payloads are dead-lettered before the handler runs.
    /// </summary>
    public T Message { get; }

    /// <summary>
    /// The subject the message was published to (not the consumer's filter).
    /// </summary>
    public string Subject { get; }

    /// <summary>
    /// The message headers; empty when there are none. Treat them as read-only.
    /// </summary>
    public NatsHeaders Headers { get; }

    /// <summary>
    /// A stable id for idempotency. The first of: X-Original-Msg-Id (a replayed dead letter), Nats-Msg-Id
    /// (an immediate publish), X-Schedule-Id (a fired schedule), or "{stream}:{sequence}".
    /// </summary>
    public string MessageId { get; }

    /// <summary>
    /// The X-Correlation-Id header.
    /// </summary>
    public string? CorrelationId { get; }

    /// <summary>
    /// When the publisher sent the message (X-Sent-Time, also set when a schedule was created); otherwise
    /// <see cref="EnqueuedTime"/>.
    /// </summary>
    public DateTimeOffset SentTime { get; }

    /// <summary>
    /// When the stream stored the message. For a fired schedule this is the fire time. Millisecond precision
    /// (NATS.Net truncates the timestamp).
    /// </summary>
    public DateTimeOffset EnqueuedTime { get; }

    /// <summary>
    /// How many times this message has been delivered to the consumer, starting at 1.
    /// </summary>
    public int DeliveryCount { get; }

    public string Stream { get; }

    /// <summary>
    /// The durable consumer name.
    /// </summary>
    public string Consumer { get; }

    public ulong StreamSequence { get; }

    public bool IsRedelivery => DeliveryCount > 1;

    /// <summary>
    /// For a fired schedule: the id returned by ScheduleAsync, unique per call. Null otherwise.
    /// </summary>
    public string? ScheduleId { get; }

    /// <summary>
    /// For a fired keyed schedule: its key. Null otherwise.
    /// </summary>
    public string? ScheduleKey { get; }

    /// <summary>
    /// For a fired schedule: the time it was scheduled for. Null otherwise.
    /// </summary>
    public DateTimeOffset? ScheduledFor { get; }

    public bool IsScheduled => ScheduleId != null;

    internal static string ResolveMessageId(NatsHeaders? headers, string stream, ulong streamSequence) =>
        Header(headers, HeaderSanitiser.OriginalMessageId)
        ?? Header(headers, HeaderSanitiser.MessageId)
        ?? Header(headers, HeaderSanitiser.ScheduleId)
        ?? $"{stream}:{streamSequence}";

    internal static string? Header(NatsHeaders? headers, string name) =>
        headers != null && headers.TryGetValue(name, out var value) && !string.IsNullOrEmpty(value.ToString())
            ? value.ToString()
            : null;

    internal static DateTimeOffset? ParseTime(string? value) =>
        DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var time)
            ? time
            : null;
}
