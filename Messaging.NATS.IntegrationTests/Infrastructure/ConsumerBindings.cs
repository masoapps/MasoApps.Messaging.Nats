using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

public static class ConsumerBindings
{
    /// <summary>
    /// Binds <typeparamref name="THandler"/> with these attribute values instead of its own [NatsConsumer]
    /// attributes. Attribute arguments are constants, so this is how a test gets unique stream and subject names.
    /// </summary>
    public static NatsMessagingOptions Bind<THandler>(this NatsMessagingOptions options,
        params NatsConsumerAttribute[] attributes)
    {
        options.ConsumerAttributeOverrides[typeof(THandler)] = attributes;
        return options;
    }

    /// <summary>
    /// A unique Limits stream "{prefix}_xxx" capturing "{root}.>", with root returned for building subjects.
    /// </summary>
    public static (StreamDefinition Definition, string Root) UniqueStream(string prefix,
        StreamConfigRetention retention = StreamConfigRetention.Limits)
    {
        var root = TestNames.Unique(prefix.ToLowerInvariant());
        return (new StreamDefinition { Name = TestNames.Unique(prefix), Subjects = [$"{root}.>"], Retention = retention }, root);
    }
}
