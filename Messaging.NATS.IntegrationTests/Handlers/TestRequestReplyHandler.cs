namespace Messaging.NATS.IntegrationTests.Handlers;

[NatsRequestHandler(Subject = Subject)]
public class TestRequestReplyHandler : INatsRequestHandler<PingRequest, PingResponse>
{
    public const string Subject = "requestreply.ping";
    public const string Fail = "fail";

    public Task<PingResponse> HandleRequestAsync(string subject, PingRequest request)
    {
        if (request.Payload == Fail)
        {
            throw new InvalidOperationException("asked to fail");
        }

        return Task.FromResult(new PingResponse(request.Payload, "TestHandler"));
    }
}
