namespace Messaging.NATS;

/// <summary>
/// Publishes, schedules and requests. Every new shape is its own overload with no optional parameters, so Moq
/// Setup and Verify expressions can bind to any of them.
/// </summary>
public interface IPublisher
{
    /// <summary>
    /// The 4.x shape. Serialises with the message's runtime type and uses
    /// <see cref="NatsMessagingOptions.PublishTimeout"/> as its budget.
    /// </summary>
    /// <param name="subject">The subject to publish to</param>
    /// <param name="message">The message to publish</param>
    /// <param name="deferredBy">Null, zero or negative: an immediate acked publish. Positive: an unkeyed schedule,
    /// so many can be pending per subject. It takes no schedule id, so retrying it after
    /// <see cref="NatsPublishException.OutcomeUnknown"/> can leave two pending schedules; use
    /// <see cref="ScheduleAsync{T}(string, T, DateTimeOffset, ScheduleOptions?)"/> with
    /// <see cref="ScheduleOptions.ScheduleId"/> when that matters.</param>
    /// <exception cref="NatsPublishException">The stream did not acknowledge the message.</exception>
    /// <exception cref="NatsMessagingConfigurationException">A positive <paramref name="deferredBy"/>, and no stream,
    /// several streams, or a stream that cannot hold schedules (for example AllowMsgSchedules is off) captures the
    /// subject.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A positive <paramref name="deferredBy"/> at or beyond the
    /// stream's MaxAge, so the schedule would expire before it fires.</exception>
    /// <exception cref="ArgumentException">A positive <paramref name="deferredBy"/> and a subject with wildcards, or
    /// one inside the schedule holder namespace "_sched.".</exception>
    Task PublishAsync(string subject, object message, TimeSpan? deferredBy);

    /// <summary>
    /// Publishes to the stream that captures <paramref name="subject"/> and waits for its acknowledgement.
    /// </summary>
    /// <exception cref="NatsPublishException">The stream did not acknowledge the message within the budget.</exception>
    Task<PublishResult> PublishAsync<T>(string subject, T message);

    /// <inheritdoc cref="PublishAsync{T}(string, T)"/>
    Task<PublishResult> PublishAsync<T>(string subject, T message, CancellationToken cancellationToken);

    /// <inheritdoc cref="PublishAsync{T}(string, T)"/>
    Task<PublishResult> PublishAsync<T>(string subject, T message, PublishOptions? options);

    /// <inheritdoc cref="PublishAsync{T}(string, T)"/>
    Task<PublishResult> PublishAsync<T>(string subject, T message, PublishOptions? options,
        CancellationToken cancellationToken);

    /// <summary>
    /// Schedules <paramref name="message"/> for delivery to <paramref name="subject"/> at
    /// <paramref name="deliverAt"/> (never earlier). The stream that captures the subject must allow message
    /// schedules (nats-server 2.12+). A time in the past fires at once.
    /// </summary>
    /// <exception cref="NatsMessagingConfigurationException">The stream cannot hold schedules.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The stream's MaxAge would expire the schedule before it fires.</exception>
    /// <exception cref="ArgumentException">The subject has wildcards or is inside "_sched.", or
    /// <see cref="ScheduleOptions.Key"/> or <see cref="ScheduleOptions.ScheduleId"/> is not one token of
    /// [A-Za-z0-9_-] up to 128 characters.</exception>
    /// <exception cref="NatsPublishException">The stream did not acknowledge the schedule within the budget, or a
    /// JetStream API call of the schedule (the stream lookup, the CreateIfAbsent lookup, a Replace's purge) failed.
    /// No responders and HTTP 503 answers are retried within the budget first.</exception>
    Task<ScheduleResult> ScheduleAsync<T>(string subject, T message, DateTimeOffset deliverAt);

    /// <inheritdoc cref="ScheduleAsync{T}(string, T, DateTimeOffset)"/>
    Task<ScheduleResult> ScheduleAsync<T>(string subject, T message, DateTimeOffset deliverAt, ScheduleOptions? options);

    /// <inheritdoc cref="ScheduleAsync{T}(string, T, DateTimeOffset)"/>
    Task<ScheduleResult> ScheduleAsync<T>(string subject, T message, DateTimeOffset deliverAt, ScheduleOptions? options,
        CancellationToken cancellationToken);

    /// <summary>
    /// Removes the pending schedule for (<paramref name="subject"/>, <paramref name="key"/>). True when one was
    /// removed. A fire already in flight can still be delivered.
    /// </summary>
    /// <exception cref="NatsMessagingConfigurationException">No stream, several streams, or a stream that cannot hold
    /// schedules captures the subject.</exception>
    /// <exception cref="ArgumentException">The subject has wildcards or is inside "_sched.", or the key is not one
    /// token of [A-Za-z0-9_-].</exception>
    /// <exception cref="NatsPublishException">The stream lookup or the purge did not complete within the budget, or
    /// failed. No responders and HTTP 503 answers are retried within the budget first.</exception>
    Task<bool> CancelScheduleAsync(string subject, string key);

    /// <inheritdoc cref="CancelScheduleAsync(string, string)"/>
    Task<bool> CancelScheduleAsync(string subject, string key, CancellationToken cancellationToken);

    /// <summary>
    /// Sends a request and waits for a response
    /// </summary>
    /// <typeparam name="TRequest">The type of the request message</typeparam>
    /// <typeparam name="TResponse">The type of the response message</typeparam>
    /// <param name="subject">The subject to send the request to</param>
    /// <param name="request">The request message</param>
    /// <param name="timeout">Optional timeout for the request (defaults to 5 seconds)</param>
    /// <param name="cancellationToken">Cancellation token to cancel the request</param>
    /// <returns>The response message, or null if no response was received</returns>
    /// <exception cref="TimeoutException">Thrown when the request times out</exception>
    /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled</exception>
    Task<TResponse?> RequestAsync<TRequest, TResponse>(
        string subject,
        TRequest request,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default);
}
