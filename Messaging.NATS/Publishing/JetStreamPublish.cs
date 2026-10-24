using System.Diagnostics;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// The acked publish path: attempts inside a budget, all with the same bytes, headers and message id.
/// Each attempt is a plain request for a PubAck, because <c>NatsJSContext.PublishAsync</c> reports "no stream"
/// and "no reply in time" as the same exception, and only the second may have stored the message.
/// </summary>
internal sealed class JetStreamPublish(INatsConnection connection, ILogger logger,
    Func<string, int, bool>? dropPubAck = null)
{
    public const int WrongLastSequence = 10071;

    // The clustered form of 10071: another write to the same subject was in flight.
    public const int WrongLastSequenceConstant = 10164;
    public const int SchedulesDisabled = 10188;
    public const int InvalidScheduleTarget = 10190;

    // HTTP 503, but retrying a full stream does not help.
    private const int StreamStoreFailed = 10077;

    private static readonly TimeSpan FirstRetryDelay = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Publishes and returns the positive PubAck (a duplicate is positive). The number of attempts is written to
    /// <paramref name="activity"/> as messaging.nats.publish.attempts.
    /// </summary>
    /// <exception cref="NatsPublishException">No positive PubAck within the budget, or a rejection.</exception>
    /// <exception cref="NatsMessagingConfigurationException">The stream cannot hold the schedule (10188, 10190).</exception>
    /// <exception cref="OperationCanceledException">The caller cancelled; the outcome is unknown.</exception>
    public async Task<PubAckResponse> PublishAsync(string subject, byte[] payload, NatsHeaders headers,
        string? messageId, int retryAttempts, PublishBudget budget, Activity? activity = null)
    {
        var attempts = 0;
        try
        {
            return await AttemptAsync(subject, payload, headers, messageId, retryAttempts, budget,
                n => attempts = n).ConfigureAwait(false);
        }
        finally
        {
            activity?.SetTag(NatsMessagingTelemetry.PublishAttempts, attempts);
        }
    }

    private async Task<PubAckResponse> AttemptAsync(string subject, byte[] payload, NatsHeaders headers,
        string? messageId, int retryAttempts, PublishBudget budget, Action<int> onAttempt)
    {
        var attempts = 0;
        var outcomeUnknown = false;
        ApiError? unavailable = null;
        Exception? lastError = null;
        var delay = FirstRetryDelay;

        while (attempts <= retryAttempts && budget.Remaining > TimeSpan.Zero)
        {
            attempts++;
            onAttempt(attempts);
            var retryNow = false;
            try
            {
                var replyTimeout = Min(budget.Remaining, connection.Opts.RequestTimeout);
                var reply = await connection.RequestAsync<byte[], PubAckResponse>(
                    subject,
                    payload,
                    headers,
                    NatsRawSerializer<byte[]>.Default,
                    NatsJSJsonSerializer<PubAckResponse>.Default,
                    replyOpts: new NatsSubOpts { Timeout = replyTimeout },
                    cancellationToken: budget.Token).ConfigureAwait(false);

                if (reply.HasNoResponders)
                {
                    lastError = new NatsNoRespondersException();
                }
                else if (reply.Error != null || reply.Data == null)
                {
                    // An unreadable reply: something answered, so the message may be stored.
                    outcomeUnknown = true;
                    lastError = reply.Error ?? new NatsException("Empty PubAck");
                }
                else if (reply.Data.Error is not { } error)
                {
                    if (dropPubAck?.Invoke(subject, attempts) != true)
                    {
                        return reply.Data;
                    }

                    outcomeUnknown = true;
                    lastError = new NatsNoReplyException();
                    retryNow = true;
                }
                else if (error.ErrCode is SchedulesDisabled or InvalidScheduleTarget)
                {
                    throw new NatsMessagingConfigurationException(
                        $"Publish to '{subject}' was rejected: {error.Description} (err_code {error.ErrCode})");
                }
                else if (error.Code == 503 && error.ErrCode != StreamStoreFailed)
                {
                    unavailable = error;
                }
                else
                {
                    throw new NatsPublishException(PublishFailure.Rejected, subject, messageId, error.ErrCode,
                        $"Publish to '{subject}' was rejected: {error.Description} (err_code {error.ErrCode})");
                }
            }
            catch (NatsNoRespondersException e)
            {
                lastError = e;
            }
            catch (NatsNoReplyException e)
            {
                // The stream may have stored it; the retry carries the same id, so the stream deduplicates.
                outcomeUnknown = true;
                lastError = e;
                retryNow = true;
            }
            catch (OperationCanceledException e) when (budget.Expired)
            {
                outcomeUnknown = true;
                lastError = e;
                break;
            }
            catch (NatsException e) when (e is not NatsJSException)
            {
                outcomeUnknown = true;
                lastError = e;
            }

            if (attempts > retryAttempts)
            {
                break;
            }

            logger.LogDebug(lastError, "Publish attempt {Attempt} to {Subject} failed; retrying", attempts, subject);

            if (!retryNow && !await DelayAsync(Min(delay, budget.Remaining), budget))
            {
                break;
            }

            delay *= 2;
        }

        if (outcomeUnknown)
        {
            throw new NatsPublishException(PublishFailure.Timeout, subject, messageId, null,
                $"Publish to '{subject}' got no acknowledgement within {budget.Timeout} ({attempts} attempts). " +
                "The message may have been stored; retry with the same message id.", lastError);
        }

        if (unavailable != null)
        {
            throw new NatsPublishException(PublishFailure.Rejected, subject, messageId, unavailable.ErrCode,
                $"Publish to '{subject}' failed: {unavailable.Description} (err_code {unavailable.ErrCode}, " +
                $"{attempts} attempts)", lastError);
        }

        throw new NatsPublishException(PublishFailure.NoStream, subject, messageId, null,
            $"No stream captures '{subject}' ({attempts} attempts within {budget.Timeout})", lastError);
    }

    private static async Task<bool> DelayAsync(TimeSpan delay, PublishBudget budget)
    {
        try
        {
            await Task.Delay(delay, budget.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (budget.Expired)
        {
            return false;
        }
    }

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
}
