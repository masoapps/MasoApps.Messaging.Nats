using NATS.Client.JetStream;

namespace Messaging.NATS.IntegrationTests.Handlers;

public class TestPubSubHandler(TaskCompletionSource<OrderEvent> tcs) : INatsConsumer<OrderEvent>
{
    public async Task HandleMessageAsync(INatsJSMsg<OrderEvent> message, CancellationToken cancellationToken)
    {
        tcs.TrySetResult(message.Data!);
        await message.AckAsync(cancellationToken: cancellationToken);
    }
}
