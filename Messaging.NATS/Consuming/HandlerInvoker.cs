using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Messaging.NATS;

/// <summary>
/// Calls one binding's handler for one delivery, in its own DI scope.
/// </summary>
internal interface IHandlerInvoker
{
    /// <summary>
    /// Returns null once the handler has returned, and throws what the handler throws. Returns an exception without
    /// calling the handler when the payload cannot be given to it (an <see cref="INatsHandler{T}"/> never sees a null
    /// or unreadable payload).
    /// </summary>
    Task<Exception?> InvokeAsync(IServiceScopeFactory scopes, INatsJSMsg<byte[]> msg, MessageSettlement settlement,
        ConsumerDurable durable, CancellationToken cancellationToken);
}

internal static class HandlerInvoker
{
    public static IHandlerInvoker Create(ConsumerBinding binding, INatsSerializerRegistry registry)
    {
        var type = (binding.IsLegacy ? typeof(LegacyInvoker<>) : typeof(Invoker<>)).MakeGenericType(binding.MessageType);
        return (IHandlerInvoker)Activator.CreateInstance(type, binding.HandlerType, registry)!;
    }

    /// <summary>
    /// Deserialises like NATS.Net does for a typed subscription: the registry's deserialiser, and a
    /// <see cref="NatsDeserializeException"/> with Data left default when it throws.
    /// </summary>
    internal static (T? Data, NatsDeserializeException? Error) Deserialize<T>(INatsDeserialize<T> deserializer,
        byte[]? payload)
    {
        var buffer = new ReadOnlySequence<byte>(payload ?? []);
        try
        {
            return (deserializer.Deserialize(buffer), null);
        }
        catch (Exception e)
        {
            return (default, new NatsDeserializeException(payload ?? [], e));
        }
    }

    private sealed class Invoker<T>(Type handlerType, INatsSerializerRegistry registry) : IHandlerInvoker
    {
        private readonly INatsDeserialize<T> _deserializer = registry.GetDeserializer<T>();

        public async Task<Exception?> InvokeAsync(IServiceScopeFactory scopes, INatsJSMsg<byte[]> msg,
            MessageSettlement settlement, ConsumerDurable durable, CancellationToken cancellationToken)
        {
            var (data, error) = Deserialize(_deserializer, msg.Data);
            if (error != null)
            {
                return error;
            }

            if (data == null)
            {
                return new NatsMessagingDeadLetterException("NullPayload", "The payload is empty or null");
            }

            var metadata = msg.Metadata!.Value;
            var context = new MessageContext<T>(data, msg.Subject, msg.Headers, (int)metadata.NumDelivered,
                metadata.Stream, metadata.Consumer, metadata.Sequence.Stream, metadata.Timestamp);

            await using var scope = scopes.CreateAsyncScope();
            var handler = (INatsHandler<T>)scope.ServiceProvider.GetRequiredService(handlerType);
            await handler.HandleAsync(context, cancellationToken).ConfigureAwait(false);
            return null;
        }
    }

    private sealed class LegacyInvoker<T>(Type handlerType, INatsSerializerRegistry registry) : IHandlerInvoker
    {
        private readonly INatsDeserialize<T> _deserializer = registry.GetDeserializer<T>();

        public async Task<Exception?> InvokeAsync(IServiceScopeFactory scopes, INatsJSMsg<byte[]> msg,
            MessageSettlement settlement, ConsumerDurable durable, CancellationToken cancellationToken)
        {
            var (data, error) = Deserialize(_deserializer, msg.Data);
            var adapter = new LegacyMsgAdapter<T>(msg, data, error, settlement);

            await using var scope = scopes.CreateAsyncScope();
            var handler = (INatsConsumer<T>)scope.ServiceProvider.GetRequiredService(handlerType);
            await handler.HandleMessageAsync(adapter, cancellationToken).ConfigureAwait(false);
            return null;
        }
    }
}

/// <summary>
/// Why the library dead-lettered a message without a handler exception: written to X-Failure-Type and
/// X-Failure-Reason.
/// </summary>
internal sealed class NatsMessagingDeadLetterException(string failureType, string message) : Exception(message)
{
    public string FailureType { get; } = failureType;
}
