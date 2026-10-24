namespace Messaging.NATS;

/// <summary>
/// A JetStream message handler. Bind it with <see cref="NatsConsumerAttribute"/>.
/// <list type="bullet">
/// <item>Return normally: the message is acked.</item>
/// <item>Throw <see cref="PoisonMessageException"/>: the message is dead-lettered at once.</item>
/// <item>Throw anything else: the message is retried after a delay
/// (<see cref="NatsConsumerAttribute.RetryDelaysSeconds"/>), and dead-lettered when
/// <see cref="NatsConsumerAttribute.MaxDeliveryAttempts"/> run out.</item>
/// </list>
/// The handler never settles the message itself. A payload that is null or cannot be deserialised is dead-lettered
/// without calling the handler.
/// </summary>
public interface INatsHandler<T>
{
    Task HandleAsync(MessageContext<T> context, CancellationToken cancellationToken);
}
