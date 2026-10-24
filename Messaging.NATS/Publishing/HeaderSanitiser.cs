using System.Globalization;
using NATS.Client.Core;

namespace Messaging.NATS;

/// <summary>
/// Header names the library writes, and the copy-and-drop rule for headers a caller passes in.
/// </summary>
internal static class HeaderSanitiser
{
    public const string MessageId = "Nats-Msg-Id";
    public const string ExpectedLastSubjectSequence = "Nats-Expected-Last-Subject-Sequence";
    public const string ExpectedLastSubjectSequenceSubject = "Nats-Expected-Last-Subject-Sequence-Subject";
    public const string Schedule = "Nats-Schedule";
    public const string ScheduleTarget = "Nats-Schedule-Target";

    public const string CorrelationId = "X-Correlation-Id";
    public const string SentTime = "X-Sent-Time";
    public const string ScheduleId = "X-Schedule-Id";
    public const string ScheduleKey = "X-Schedule-Key";
    public const string ScheduledFor = "X-Scheduled-For";

    public const string OriginalSubject = "X-Original-Subject";
    public const string OriginalMessageId = "X-Original-Msg-Id";
    public const string Stream = "X-Stream";
    public const string Consumer = "X-Consumer";
    public const string StreamSequence = "X-Stream-Sequence";
    public const string DeliveryCount = "X-Delivery-Count";
    public const string FailureType = "X-Failure-Type";
    public const string FailureReason = "X-Failure-Reason";
    public const string FailedAt = "X-Failed-At";
    public const string ReplayConsumer = "X-Replay-Consumer";

    /// <summary>
    /// The 4.x deferral header ("MM/dd/yyyy HH:mm:ss", invariant culture, UTC). No longer written; honoured on
    /// consume until v7.
    /// </summary>
    public const string LegacyDeferredUntil = "Msg-DeferredUntil";

    // Library-owned X-* headers: exact names, then prefixes. X-Stream and X-Stream-Sequence are exact names, so a
    // user header such as X-Streaming-Mode is kept.
    private static readonly string[] LibraryNames =
        ["X-Sent-Time", "X-Scheduled-For", "X-Failed-At", "X-Consumer", "X-Delivery-Count", "X-Stream", "X-Stream-Sequence"];
    private static readonly string[] LibraryPrefixes = ["X-Schedule-", "X-Original-", "X-Replay", "X-Failure-"];

    // The W3C trace context. A forwarded one points at the upstream producer; the publisher writes its own span's.
    private static readonly string[] TraceContext = [NatsMessagingTelemetry.TraceParent, NatsMessagingTelemetry.TraceState];

    /// <summary>
    /// A new header set with every header of <paramref name="headers"/> except Nats-* (server-interpreted), the
    /// library's own X-* headers, and traceparent and tracestate. The input is never changed.
    /// </summary>
    public static NatsHeaders Copy(NatsHeaders? headers)
    {
        var copy = new NatsHeaders();
        if (headers == null)
        {
            return copy;
        }

        foreach (var (name, value) in headers)
        {
            if (!IsDropped(name))
            {
                copy[name] = value;
            }
        }

        return copy;
    }

    public static bool IsDropped(string name) =>
        name.StartsWith("Nats-", StringComparison.OrdinalIgnoreCase) ||
        LibraryNames.Any(n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase)) ||
        TraceContext.Any(n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase)) ||
        LibraryPrefixes.Any(p => name.StartsWith(p, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// RFC 3339 in UTC with 7 fractional digits, independent of the current culture. Used for X-Sent-Time,
    /// X-Scheduled-For and the "@at" schedule time, so a schedule never fires early through rounding.
    /// </summary>
    public static string FormatTime(DateTimeOffset time) =>
        time.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture);

    /// <summary>
    /// Adds X-Correlation-Id (the option wins over a forwarded one) and X-Sent-Time.
    /// </summary>
    public static void AddCommon(NatsHeaders headers, string? correlationId, DateTimeOffset sentTime)
    {
        if (correlationId != null)
        {
            headers[CorrelationId] = correlationId;
        }

        headers[SentTime] = FormatTime(sentTime);
    }
}
