// Two versions of one handler class, as a service would ship them in two releases: the durable name depends on the
// class name only, so both use "{queueGroup}_ActivityHandler".

namespace Messaging.NATS.IntegrationTests.Handlers.V1
{
    public sealed class ActivityHandler(Probe<MessageContext<OrderEvent>> orders) : INatsHandler<OrderEvent>
    {
        public Task HandleAsync(MessageContext<OrderEvent> context, CancellationToken cancellationToken) =>
            orders.InvokeAsync(context, cancellationToken);
    }
}

namespace Messaging.NATS.IntegrationTests.Handlers.V2
{
    public sealed class ActivityHandler(Probe<MessageContext<OrderEvent>> orders, Probe<MessageContext<PaymentEvent>> payments)
        : INatsHandler<OrderEvent>, INatsHandler<PaymentEvent>
    {
        public Task HandleAsync(MessageContext<OrderEvent> context, CancellationToken cancellationToken) =>
            orders.InvokeAsync(context, cancellationToken);

        public Task HandleAsync(MessageContext<PaymentEvent> context, CancellationToken cancellationToken) =>
            payments.InvokeAsync(context, cancellationToken);
    }
}
