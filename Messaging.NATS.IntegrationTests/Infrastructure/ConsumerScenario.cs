using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.Serializers.Json;

namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// A started <see cref="TestHost"/> with one handler class bound to a unique declared stream.
/// </summary>
public sealed class ConsumerScenario : IAsyncDisposable
{
    private ConsumerScenario(TestHost host, StreamDefinition stream, string root, string durable)
    {
        Host = host;
        Stream = stream;
        Root = root;
        Durable = durable;
    }

    public TestHost Host { get; }

    public StreamDefinition Stream { get; }

    /// <summary>
    /// The stream captures "{Root}.>".
    /// </summary>
    public string Root { get; }

    public string Durable { get; }

    public string Component => $"consumer:{Stream.Name}/{Durable}";

    public IPublisher Publisher => Host.Publisher;

    /// <summary>
    /// Starts a host with <typeparamref name="THandler"/> bound by <paramref name="attributes"/>. Each attribute gets
    /// the stream unless it sets one, and the first gets a unique queue group when none sets one. A subject starting
    /// with "." is appended to the root.
    /// </summary>
    public static async Task<ConsumerScenario> StartAsync<THandler>(IEnumerable<object> singletons,
        Action<NatsMessagingOptions>? configure, params NatsConsumerAttribute[] attributes)
    {
        var (stream, root) = ConsumerBindings.UniqueStream("CONSUMER");
        return await StartAsync<THandler>(stream, root, singletons, configure, attributes);
    }

    public static async Task<ConsumerScenario> StartAsync<THandler>(StreamDefinition stream, string root,
        IEnumerable<object> singletons, Action<NatsMessagingOptions>? configure, params NatsConsumerAttribute[] attributes)
    {
        var queueGroup = attributes.Select(a => a.QueueGroup).FirstOrDefault(g => g.Length > 0);
        if (queueGroup == null)
        {
            queueGroup = TestNames.Unique("qg");
            attributes[0].QueueGroup = queueGroup;
        }

        foreach (var attribute in attributes)
        {
            attribute.Stream = attribute.Stream.Length == 0 ? stream.Name : attribute.Stream;
            attribute.Subject = attribute.Subject.StartsWith('.') ? root + attribute.Subject : attribute.Subject;
        }

        var host = await TestHost.StartAsync(o =>
            {
                o.StreamDefinitions = [stream];
                o.Bind<THandler>(attributes);
                configure?.Invoke(o);
            },
            services =>
            {
                foreach (var singleton in singletons)
                {
                    services.AddSingleton(singleton.GetType(), singleton);
                    if (singleton is ILoggerProvider provider)
                    {
                        services.AddSingleton(provider);
                    }
                }
            });

        try
        {
            await host.WaitUntilStartedAsync();
        }
        catch
        {
            await host.DisposeAsync();
            throw;
        }

        var durable = attributes.Select(a => a.Durable).FirstOrDefault(d => d != null)
                      ?? $"{queueGroup}_{typeof(THandler).Name}";
        return new ConsumerScenario(host, stream, root, durable);
    }

    public Task<ConsumerInfo> InfoAsync() => Host.ConsumerInfoAsync(Stream.Name, Durable);

    /// <summary>
    /// Stores raw bytes on <paramref name="subject"/> (bypassing the library's serialiser).
    /// </summary>
    public async Task<ulong> PublishRawAsync(string subject, byte[] payload, NatsHeaders? headers = null)
    {
        await using var connection = new NatsConnection(new NatsOpts
        {
            Url = NatsContainerFixture.NatsUrl,
            SerializerRegistry = NatsJsonSerializerRegistry.Default
        });
        var ack = await new NatsJSContext(connection).PublishAsync(subject, payload, NatsRawSerializer<byte[]>.Default,
            headers: headers);
        ack.EnsureSuccess();
        return ack.Seq;
    }

    public ValueTask DisposeAsync() => Host.DisposeAsync();
}
