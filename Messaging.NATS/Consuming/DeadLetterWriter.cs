using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// Writes a failed message to its dead-letter subject through the acked publish path (design 2.6): the raw payload
/// bytes, the original headers without the server-interpreted Nats-* ones, and the X-* failure headers. The message
/// id "{stream}:{sequence}:{stored}:{durable}" makes the write idempotent per (stored message, durable), so a write
/// whose source ack was lost is stored once, and two durables that fail one fan-out message get one entry each. The
/// stored time (UTC ticks) tells apart two messages with the same sequence in a stream that was deleted and created
/// again. A PubAck from an Interest stream does not count as stored: see <see cref="RetentionProblem"/>.
/// </summary>
internal sealed class DeadLetterWriter(INatsConnection connection, NatsMessagingOptions options, ILogger logger)
{
    public const int MaxReasonLength = 1024;

    private static readonly TimeSpan RetentionRecheck = TimeSpan.FromSeconds(30);

    private readonly JetStreamPublish _publish = new(connection, logger);
    private readonly NatsJSContext _js = new(connection);
    private readonly ConcurrentDictionary<string, RetentionCheck> _retention = new();

    /// <summary>
    /// Why a dead-letter stream with this retention loses its entries, or null when it keeps them. Nothing consumes
    /// the dead-letter subjects, so an Interest stream acknowledges each write and stores nothing.
    /// </summary>
    public static string? RetentionProblem(string stream, StreamConfigRetention retention) =>
        retention == StreamConfigRetention.Interest
            ? $"The dead-letter stream '{stream}' has Interest retention. Nothing consumes the dead-letter subjects, " +
              "so it acknowledges each dead letter and keeps none. Give the dead-letter subjects a stream with Limits " +
              "retention (StreamDefinition.DeadLetter)."
            : null;

    public static string MessageIdFor(string stream, ulong sequence, DateTimeOffset stored, string durable) =>
        $"{stream}:{sequence}:{stored.UtcTicks.ToString(CultureInfo.InvariantCulture)}:{durable}";

    /// <summary>
    /// One write: an acked publish within PublishTimeout and PublishRetryAttempts. Returns the positive PubAck (a
    /// duplicate counts as stored).
    /// </summary>
    /// <exception cref="NatsPublishException">No stream captures the subject, the stream rejected it, or no ack came.</exception>
    public async Task<PubAckResponse> WriteAsync(string deadLetterSubject, INatsJSMsg<byte[]> msg, string durable,
        string failureType, string reason, CancellationToken cancellationToken)
    {
        var metadata = msg.Metadata;
        var messageId = MessageIdFor(metadata?.Stream ?? string.Empty, metadata?.Sequence.Stream ?? 0,
            metadata?.Timestamp ?? default, durable);
        var headers = BuildHeaders(msg, durable, failureType, reason, DateTimeOffset.UtcNow);
        headers[HeaderSanitiser.MessageId] = messageId;
        var payload = msg.Data ?? [];

        using var activity = NatsMessagingTelemetry.StartProducer("publish", deadLetterSubject, messageId);
        activity?.SetTag(NatsMessagingTelemetry.BodySize, payload.Length);
        var started = Stopwatch.GetTimestamp();
        try
        {
            using var budget = new PublishBudget(options.PublishTimeout, cancellationToken);
            var ack = await _publish.PublishAsync(deadLetterSubject, payload, headers, messageId,
                options.PublishRetryAttempts, budget, activity).ConfigureAwait(false);
            await EnsureKeptAsync(ack.Stream, budget.Token).ConfigureAwait(false);
            NatsMessagingTelemetry.RecordOperation(activity, "publish", deadLetterSubject, started,
                ack.Duplicate ? "duplicate" : "ok", null);
            return ack;
        }
        catch (Exception e)
        {
            NatsMessagingTelemetry.RecordOperation(activity, "publish", deadLetterSubject, started,
                NatsMessagingTelemetry.PublishOutcome(e), e);
            throw;
        }
    }

    /// <summary>
    /// Throws when the stream that acknowledged the write does not keep it, so the source message is not acked and
    /// stays in its stream. The retention is read once per stream and again after <see cref="RetentionRecheck"/>; a
    /// failed read keeps the last value, or fails the write when there is none. A stream that is gone again by then
    /// cannot be checked, and the write counts as stored.
    /// </summary>
    private async Task EnsureKeptAsync(string? stream, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(stream))
        {
            return;
        }

        var found = _retention.TryGetValue(stream, out var known);
        if (!found || Stopwatch.GetElapsedTime(known.CheckedAt) >= RetentionRecheck)
        {
            try
            {
                var info = await _js.GetStreamAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
                known = new RetentionCheck(info.Info.Config.Retention, Stopwatch.GetTimestamp());
                _retention[stream] = known;
            }
            catch (NatsJSApiException e) when (e.Error.ErrCode == ConsumerWorker.StreamNotFound)
            {
                return;
            }
            catch (Exception) when (found && !cancellationToken.IsCancellationRequested)
            {
            }
        }

        if (RetentionProblem(stream, known.Retention) is { } problem)
        {
            throw new NatsMessagingConfigurationException(problem);
        }
    }

    /// <summary>
    /// The original headers without Nats-*, plus the X-* failure headers. X-Original-Msg-Id is the message id the
    /// handler saw (<see cref="MessageContext{T}.MessageId"/>), so a replay keeps it.
    /// </summary>
    internal static NatsHeaders BuildHeaders(INatsJSMsg<byte[]> msg, string durable, string failureType,
        string reason, DateTimeOffset failedAt)
    {
        var metadata = msg.Metadata;
        var stream = metadata?.Stream ?? string.Empty;
        var sequence = metadata?.Sequence.Stream ?? 0;

        var headers = new NatsHeaders();
        if (msg.Headers != null)
        {
            foreach (var (name, value) in msg.Headers)
            {
                if (!name.StartsWith("Nats-", StringComparison.OrdinalIgnoreCase))
                {
                    headers[name] = value;
                }
            }
        }

        headers[HeaderSanitiser.OriginalSubject] = msg.Subject;
        headers[HeaderSanitiser.OriginalMessageId] = MessageContext<object>.ResolveMessageId(msg.Headers, stream, sequence);
        headers[HeaderSanitiser.Stream] = stream;
        headers[HeaderSanitiser.Consumer] = durable;
        headers[HeaderSanitiser.StreamSequence] = sequence.ToString(CultureInfo.InvariantCulture);
        headers[HeaderSanitiser.DeliveryCount] = (metadata?.NumDelivered ?? 0).ToString(CultureInfo.InvariantCulture);
        headers[HeaderSanitiser.FailureType] = failureType;
        headers[HeaderSanitiser.FailureReason] = Truncate(OneLine(reason), MaxReasonLength);
        headers[HeaderSanitiser.FailedAt] = HeaderSanitiser.FormatTime(failedAt);
        return headers;
    }

    // Header values cannot carry line breaks.
    private static string OneLine(string value) => value.ReplaceLineEndings(" ");

    private static string Truncate(string value, int maxLength) => value.Length <= maxLength ? value : value[..maxLength];

    private readonly record struct RetentionCheck(StreamConfigRetention Retention, long CheckedAt);
}
