namespace Messaging.NATS.IntegrationTests.Handlers;

// Two handler classes for one subject: each gets its own durable, like several report generators handling one command.
// Tests bind them with ConsumerBindings.Bind.

public sealed class FanOutProbes
{
    public Probe<MessageContext<OrderEvent>> First { get; } = new();

    public Probe<MessageContext<OrderEvent>> Second { get; } = new();
}

public sealed class FirstFanOutHandler(FanOutProbes probes) : INatsHandler<OrderEvent>
{
    public Task HandleAsync(MessageContext<OrderEvent> context, CancellationToken cancellationToken) =>
        probes.First.InvokeAsync(context, cancellationToken);
}

public sealed class SecondFanOutHandler(FanOutProbes probes) : INatsHandler<OrderEvent>
{
    public Task HandleAsync(MessageContext<OrderEvent> context, CancellationToken cancellationToken) =>
        probes.Second.InvokeAsync(context, cancellationToken);
}
