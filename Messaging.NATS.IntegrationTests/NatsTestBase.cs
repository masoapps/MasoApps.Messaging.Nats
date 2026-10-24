using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.Serializers.Json;

namespace Messaging.NATS.IntegrationTests;

public abstract class NatsTestBase
{
    protected static NatsConnection CreateConnection()
    {
        return new NatsConnection(new NatsOpts
        {
            Url = NatsContainerFixture.NatsUrl,
            SerializerRegistry = NatsJsonSerializerRegistry.Default
        });
    }

    protected static NatsJSContext CreateJetStreamContext(NatsConnection connection)
    {
        return new NatsJSContext(connection);
    }

    protected static async Task CreateStream(NatsJSContext js, string name, string[] subjects, bool allowSchedules = false)
    {
        await js.CreateStreamAsync(new StreamConfig(name, subjects)
        {
            AllowMsgSchedules = allowSchedules
        });
    }

    protected static NatsPublisher CreatePublisher(NatsConnection connection, NatsJSContext js,
        Action<NatsMessagingOptions>? configure = null)
    {
        var options = new NatsMessagingOptions { NatsUrl = connection.Opts.Url };
        configure?.Invoke(options);
        return new NatsPublisher(connection, js, options, NullLogger<NatsPublisher>.Instance);
    }

    protected static ServiceProvider BuildServiceProvider(Action<IServiceCollection> configure)
    {
        var services = new ServiceCollection();
        configure(services);
        return services.BuildServiceProvider();
    }
}
