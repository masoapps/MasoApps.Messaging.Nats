namespace Messaging.NATS;

/// <summary>
/// Throw from <see cref="INatsHandler{T}.HandleAsync"/> or <see cref="INatsConsumer{TMessage}.HandleMessageAsync"/>
/// to dead-letter the message at once, without using the remaining delivery attempts. Use it when the message can
/// never succeed (for example a malformed payload or a schema violation).
/// </summary>
public class PoisonMessageException : Exception
{
    public PoisonMessageException(string message) : base(message) { }

    public PoisonMessageException(string message, Exception innerException) : base(message, innerException) { }
}
