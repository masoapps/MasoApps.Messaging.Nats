using System.Text.Json;
using NATS.Client.Core;
using NATS.Client.Serializers.Json;

namespace Messaging.NATS;

/// <summary>
/// A serializer registry that uses reflection-based JSON serialization with custom options.
/// </summary>
public sealed class NatsReflectionJsonSerializerRegistry(JsonSerializerOptions options) : INatsSerializerRegistry
{
    public INatsSerialize<T> GetSerializer<T>() => new NatsJsonSerializer<T>(options);

    public INatsDeserialize<T> GetDeserializer<T>() => new NatsJsonSerializer<T>(options);
}
