namespace Messaging.NATS.IntegrationTests.Handlers;

/// <summary>
/// Counts the requests one host answered. Register one per host.
/// </summary>
public sealed class ReplyCounter
{
    private int _calls;

    public int Calls => Volatile.Read(ref _calls);

    public void Add() => Interlocked.Increment(ref _calls);
}

[NatsRequestHandler(Subject = Subject, QueueGroup = "requestreply-workers")]
public sealed class QueueGroupReplyHandler(ReplyCounter counter) : INatsRequestHandler<PingRequest, PingResponse>
{
    public const string Subject = "requestreply.queuegroup";

    public Task<PingResponse> HandleRequestAsync(string subject, PingRequest request)
    {
        counter.Add();
        return Task.FromResult(new PingResponse(request.Payload, nameof(QueueGroupReplyHandler)));
    }
}

[NatsRequestHandler(Subject = Subject)]
public sealed class NoQueueGroupReplyHandler(ReplyCounter counter) : INatsRequestHandler<PingRequest, PingResponse>
{
    public const string Subject = "requestreply.noqueuegroup";

    public Task<PingResponse> HandleRequestAsync(string subject, PingRequest request)
    {
        counter.Add();
        return Task.FromResult(new PingResponse(request.Payload, nameof(NoQueueGroupReplyHandler)));
    }
}

[NatsRequestHandler(Subject = "requestreply.badqueuegroup", QueueGroup = "has space")]
public sealed class BadQueueGroupReplyHandler : INatsRequestHandler<PingRequest, PingResponse>
{
    public Task<PingResponse> HandleRequestAsync(string subject, PingRequest request) =>
        Task.FromResult(new PingResponse(request.Payload, nameof(BadQueueGroupReplyHandler)));
}
