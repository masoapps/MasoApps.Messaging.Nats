using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;

namespace Messaging.NATS;

/// <summary>
/// Serves one <see cref="INatsRequestHandler{TRequest,TResponse}"/> on a core subscription. Run inside a
/// <see cref="RetryLoop"/>: a failed or ended subscription throws, so the loop reports it and subscribes again.
/// </summary>
internal sealed class ReplyHandlerWorker
{
    private static readonly TimeSpan ReplyTimeout = TimeSpan.FromSeconds(5);

    private static readonly MethodInfo RunMethod =
        typeof(ReplyHandlerWorker).GetMethod(nameof(RunTypedAsync), BindingFlags.Instance | BindingFlags.NonPublic)!;

    private readonly ReplyHandlerRegistration _registration;
    private readonly INatsConnection _connection;
    private readonly IServiceScopeFactory _scopes;
    private readonly ILogger _logger;
    private readonly Func<RetryAttempt, CancellationToken, Task> _run;

    public ReplyHandlerWorker(ReplyHandlerRegistration registration, INatsConnection connection,
        IServiceScopeFactory scopes, ILogger<ReplyHandlerWorker> logger)
    {
        _registration = registration;
        _connection = connection;
        _scopes = scopes;
        _logger = logger;
        _run = RunMethod.MakeGenericMethod(registration.RequestType, registration.ResponseType)
            .CreateDelegate<Func<RetryAttempt, CancellationToken, Task>>(this);
    }

    public Task RunAsync(RetryAttempt attempt, CancellationToken cancellationToken) => _run(attempt, cancellationToken);

    private async Task RunTypedAsync<TRequest, TResponse>(RetryAttempt attempt, CancellationToken cancellationToken)
    {
        var subject = _registration.Subject;
        await using var sub = await _connection.SubscribeCoreAsync<TRequest>(subject, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        _logger.LogInformation("Reply handler {Handler} subscribed to {Subject}", _registration.HandlerType.Name, subject);
        attempt.Running();

        await foreach (var msg in sub.Msgs.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            // ReadAllAsync keeps yielding buffered requests after the token is cancelled. Stopping leaves them
            // unhandled: their callers time out and retry elsewhere, and no handler runs during host disposal.
            if (cancellationToken.IsCancellationRequested)
            {
                var queued = sub.Msgs.CanCount ? sub.Msgs.Count + 1 : 1;
                _logger.LogInformation("Reply handler {Handler} on {Subject} is stopping; {Count} queued request(s) not handled",
                    _registration.HandlerType.Name, subject, queued);
                break;
            }

            await HandleAsync<TRequest, TResponse>(msg).ConfigureAwait(false);
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new NatsMessagingTransientException($"The subscription to {subject} ended");
    }

    private async Task HandleAsync<TRequest, TResponse>(NatsMsg<TRequest> msg)
    {
        var subject = _registration.Subject;
        if (msg.Data == null)
        {
            _logger.LogError(msg.Error, "Request on {Subject} has no readable data", subject);
            return;
        }

        try
        {
            await using var scope = _scopes.CreateAsyncScope();
            var handler = (INatsRequestHandler<TRequest, TResponse>)scope.ServiceProvider
                .GetRequiredService(_registration.HandlerType);
            var response = await handler.HandleRequestAsync(subject, msg.Data).ConfigureAwait(false);

            if (msg.ReplyTo != null)
            {
                // Not the stop token: a request handled during shutdown still gets its reply while the connection
                // is open, so the caller does not time out and retry a side effect that already happened.
                using var replyTimeout = new CancellationTokenSource(ReplyTimeout);
                await _connection.PublishAsync(msg.ReplyTo, response, cancellationToken: replyTimeout.Token)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Request handler {Handler} failed on {Subject}", _registration.HandlerType.Name, subject);
        }
    }
}
