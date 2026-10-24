using NATS.Client.JetStream;

namespace Messaging.NATS;

public interface INatsConsumer<TMessage>
{
    Task HandleMessageAsync(INatsJSMsg<TMessage> message, CancellationToken cancellationToken);
}