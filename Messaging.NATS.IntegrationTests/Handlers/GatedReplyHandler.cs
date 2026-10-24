namespace Messaging.NATS.IntegrationTests.Handlers;

public class ReplyGate
{
    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>
/// Answers once the test releases the gate, so a test can stop the host while a request is being handled.
/// </summary>
[NatsRequestHandler(Subject = Subject)]
public class GatedReplyHandler(ReplyGate gate) : INatsRequestHandler<PingRequest, PingResponse>
{
    public const string Subject = "requestreply.gated";

    public async Task<PingResponse> HandleRequestAsync(string subject, PingRequest request)
    {
        gate.Entered.TrySetResult();
        await gate.Release.Task;
        return new PingResponse(request.Payload, nameof(GatedReplyHandler));
    }
}
