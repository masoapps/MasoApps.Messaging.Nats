namespace Messaging.NATS.IntegrationTests.Handlers;

public class ReplyBacklog
{
    private int _calls;

    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int Calls => Volatile.Read(ref _calls);

    public int Enter() => Interlocked.Increment(ref _calls);
}

/// <summary>
/// The first request waits for the test to release the gate; the others answer at once. Requests sent meanwhile
/// queue up in the subscription.
/// </summary>
[NatsRequestHandler(Subject = Subject)]
public class BacklogReplyHandler(ReplyBacklog backlog) : INatsRequestHandler<PingRequest, PingResponse>
{
    public const string Subject = "requestreply.backlog";

    public async Task<PingResponse> HandleRequestAsync(string subject, PingRequest request)
    {
        if (backlog.Enter() == 1)
        {
            backlog.Entered.TrySetResult();
            await backlog.Release.Task;
        }

        return new PingResponse(request.Payload, nameof(BacklogReplyHandler));
    }
}
