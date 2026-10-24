using NATS.Client.JetStream;

namespace Messaging.NATS.IntegrationTests.Handlers;

// These classes carry no attribute: tests bind them with ConsumerBindings.Bind, so each test has its own
// stream, subjects and probe.

public sealed class OrderHandler(Probe<MessageContext<OrderEvent>> probe) : INatsHandler<OrderEvent>
{
    public Task HandleAsync(MessageContext<OrderEvent> context, CancellationToken cancellationToken) =>
        probe.InvokeAsync(context, cancellationToken);
}

public sealed class LegacyOrderConsumer(Probe<INatsJSMsg<OrderEvent>> probe) : INatsConsumer<OrderEvent>
{
    public Task HandleMessageAsync(INatsJSMsg<OrderEvent> message, CancellationToken cancellationToken) =>
        probe.InvokeAsync(message, cancellationToken);
}

public sealed class MultiTypeHandler(Probe<MessageContext<OrderEvent>> orders, Probe<MessageContext<PaymentEvent>> payments)
    : INatsHandler<OrderEvent>, INatsHandler<PaymentEvent>
{
    public Task HandleAsync(MessageContext<OrderEvent> context, CancellationToken cancellationToken) =>
        orders.InvokeAsync(context, cancellationToken);

    public Task HandleAsync(MessageContext<PaymentEvent> context, CancellationToken cancellationToken) =>
        payments.InvokeAsync(context, cancellationToken);
}

public abstract class BaseReportConsumer<T>(Probe<INatsJSMsg<T>> probe) : INatsConsumer<T>
{
    public async Task HandleMessageAsync(INatsJSMsg<T> message, CancellationToken cancellationToken)
    {
        await probe.InvokeAsync(message, cancellationToken);
        await message.AckAsync(cancellationToken: cancellationToken);
    }
}

[NatsConsumer(Stream = "REPORTS", Subject = "reports.daily", QueueGroup = "reports")]
public sealed class DailyReportConsumer(Probe<INatsJSMsg<OrderEvent>> probe) : BaseReportConsumer<OrderEvent>(probe);

[NatsConsumer(Stream = "ACTIVITY", Subject = "activity.orders", MessageType = typeof(OrderEvent), QueueGroup = "activity")]
[NatsConsumer(Stream = "ACTIVITY", Subject = "activity.payments", MessageType = typeof(PaymentEvent), MaxDeliveryAttempts = 7)]
public sealed class AttributedMultiTypeHandler : INatsHandler<OrderEvent>, INatsHandler<PaymentEvent>
{
    public Task HandleAsync(MessageContext<OrderEvent> context, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task HandleAsync(MessageContext<PaymentEvent> context, CancellationToken cancellationToken) => Task.CompletedTask;
}

public sealed class BothInterfacesHandler : INatsHandler<OrderEvent>, INatsConsumer<OrderEvent>
{
    public Task HandleAsync(MessageContext<OrderEvent> context, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task HandleMessageAsync(INatsJSMsg<OrderEvent> message, CancellationToken cancellationToken) => Task.CompletedTask;
}

public sealed class NotAHandler;

// The 4.x attribute shape (survey must-keep item 3): the same four named properties.
[NatsConsumer(Stream = "orders", Subject = "orders.created", QueueGroup = "order-worker", MaxDeliveryAttempts = 10)]
public sealed class FourXShapedConsumer : INatsConsumer<OrderEvent>
{
    public async Task HandleMessageAsync(INatsJSMsg<OrderEvent> message, CancellationToken cancellationToken) =>
        await message.AckAsync(cancellationToken: cancellationToken);
}
