namespace Messaging.NATS;

public interface INatsRequestHandler<TRequest, TResponse>
{
    Task<TResponse> HandleRequestAsync(string subject, TRequest request);
}