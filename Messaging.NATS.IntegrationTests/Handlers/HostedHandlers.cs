using System.Collections.Concurrent;
using NATS.Client.JetStream;

namespace Messaging.NATS.IntegrationTests.Handlers;

// Attribute values are constants, so each class here is used by one test only.

public class HostedSignals
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource<OrderEvent>> _orders = new();

    public Task<OrderEvent> WaitForOrder(string orderId) => Order(orderId).Task;

    public void Received(OrderEvent order) => Order(order.OrderId).TrySetResult(order);

    private TaskCompletionSource<OrderEvent> Order(string orderId) =>
        _orders.GetOrAdd(orderId, _ => new TaskCompletionSource<OrderEvent>(TaskCreationOptions.RunContinuationsAsynchronously));
}

[NatsConsumer(Stream = Stream, Subject = "hostedorders.created", QueueGroup = "hosted")]
public class HostedOrderConsumer(HostedSignals signals) : INatsConsumer<OrderEvent>
{
    public const string Stream = "HOSTED_ORDERS";
    public const string Durable = "hosted_HostedOrderConsumer";

    public async Task HandleMessageAsync(INatsJSMsg<OrderEvent> message, CancellationToken cancellationToken)
    {
        signals.Received(message.Data!);
        await message.AckAsync(cancellationToken: cancellationToken);
    }
}

[NatsConsumer(Stream = Stream, Subject = "hostedwaiting.created", QueueGroup = "hosted")]
public class UndeclaredStreamConsumer(HostedSignals signals) : INatsConsumer<OrderEvent>
{
    public const string Stream = "HOSTED_UNDECLARED";
    public const string Durable = "hosted_UndeclaredStreamConsumer";

    public async Task HandleMessageAsync(INatsJSMsg<OrderEvent> message, CancellationToken cancellationToken)
    {
        signals.Received(message.Data!);
        await message.AckAsync(cancellationToken: cancellationToken);
    }
}

[NatsConsumer(Stream = Stream, Subject = "hostedowned.created", QueueGroup = "hosted")]
public class OwnedDeadLetterConsumer : INatsConsumer<OrderEvent>
{
    public const string Stream = "HOSTED_OWNED_DLQ";

    public Task HandleMessageAsync(INatsJSMsg<OrderEvent> message, CancellationToken cancellationToken) => Task.CompletedTask;
}

[NatsRequestHandler(Subject = Subject)]
public class HostedPingHandler : INatsRequestHandler<PingRequest, PingResponse>
{
    public const string Subject = "hosted.ping";

    public Task<PingResponse> HandleRequestAsync(string subject, PingRequest request) =>
        Task.FromResult(new PingResponse(request.Payload, nameof(HostedPingHandler)));
}

[NatsConsumer(Stream = "UNCOVERED_DLQ", Subject = "uncovered.created", QueueGroup = "uncovered",
    DeadLetterSubject = "elsewhere.uncovered")]
public class UncoveredDeadLetterConsumer : INatsConsumer<OrderEvent>
{
    public Task HandleMessageAsync(INatsJSMsg<OrderEvent> message, CancellationToken cancellationToken) => Task.CompletedTask;
}

[NatsConsumer(Stream = "NO_GROUP", Subject = "nogroup.created")]
public class NoQueueGroupConsumer : INatsConsumer<OrderEvent>
{
    public Task HandleMessageAsync(INatsJSMsg<OrderEvent> message, CancellationToken cancellationToken) => Task.CompletedTask;
}
