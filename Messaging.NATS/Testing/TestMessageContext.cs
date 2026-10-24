using NATS.Client.Core;

namespace Messaging.NATS.Testing;

/// <summary>
/// Builds <see cref="MessageContext{T}"/> instances for handler unit tests.
/// </summary>
public static class TestMessageContext
{
    private static long _sequence;

    /// <summary>
    /// A context as the consumer would build it. The ids, times and schedule fields are written as the matching
    /// headers, so <see cref="MessageContext{T}.Headers"/> and the properties agree. Each context gets the next
    /// <paramref name="streamSequence"/> unless one is passed, so without a <paramref name="messageId"/> every
    /// context has its own MessageId ("{stream}:{sequence}"), as two stored messages would.
    /// </summary>
    public static MessageContext<T> Create<T>(T message, string subject = "test", NatsHeaders? headers = null,
        int deliveryCount = 1, string? messageId = null, string? correlationId = null, DateTimeOffset? sentTime = null,
        string? scheduleId = null, string? scheduleKey = null, string stream = "test", string consumer = "test",
        ulong? streamSequence = null, DateTimeOffset? enqueuedTime = null, DateTimeOffset? scheduledFor = null)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentOutOfRangeException.ThrowIfLessThan(deliveryCount, 1);

        var copy = new NatsHeaders();
        if (headers != null)
        {
            foreach (var (name, value) in headers)
            {
                copy[name] = value;
            }
        }

        if (messageId != null)
        {
            copy[HeaderSanitiser.MessageId] = messageId;
        }

        if (correlationId != null)
        {
            copy[HeaderSanitiser.CorrelationId] = correlationId;
        }

        if (sentTime is { } sent)
        {
            copy[HeaderSanitiser.SentTime] = HeaderSanitiser.FormatTime(sent);
        }

        if (scheduledFor is { } scheduled)
        {
            copy[HeaderSanitiser.ScheduledFor] = HeaderSanitiser.FormatTime(scheduled);
        }

        if (scheduleId != null)
        {
            copy[HeaderSanitiser.ScheduleId] = scheduleId;
            copy.TryAdd(HeaderSanitiser.ScheduledFor, HeaderSanitiser.FormatTime(DateTimeOffset.UtcNow));
        }

        if (scheduleKey != null)
        {
            copy[HeaderSanitiser.ScheduleKey] = scheduleKey;
        }

        var sequence = streamSequence ?? (ulong)Interlocked.Increment(ref _sequence);
        return new MessageContext<T>(message, subject, copy, deliveryCount, stream, consumer, sequence,
            enqueuedTime ?? DateTimeOffset.UtcNow);
    }
}
