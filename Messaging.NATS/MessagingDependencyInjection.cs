using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.Hosting;
using NATS.Client.JetStream;
using NATS.Client.Serializers.Json;

namespace Messaging.NATS;

public static class MessagingDependencyInjection
{
    /// <summary>
    /// Adds NATS messaging with attribute-based routing to the service collection.
    /// Options and handler attributes are validated here, so a misconfiguration throws before the host starts.
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="configureOptions">Action to configure NATS messaging options</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddNatsMessaging(this IServiceCollection services, Action<NatsMessagingOptions> configureOptions)
    {
        var options = new NatsMessagingOptions();
        configureOptions(options);

        // Copy the definitions so a shared list (or its items) is never changed by the library, and later
        // changes by the caller do not reach a running host.
        options.StreamDefinitions = (options.StreamDefinitions ?? []).Select(d => d.Clone()).ToList();
        options.DeadLetterStream = options.DeadLetterStream?.Clone();
        options.Assemblies ??= [];
        options.HandlerTypes ??= [];

        var registration = NatsMessagingRegistration.Create(options);

        INatsSerializerRegistry serializerRegistry = options switch
        {
            { SerializerContext: not null } => new NatsJsonContextSerializerRegistry(options.SerializerContext),
            { JsonSerializerOptions: not null } => new NatsReflectionJsonSerializerRegistry(options.JsonSerializerOptions),
            _ => NatsJsonSerializerRegistry.Default
        };

        services.AddSingleton<IPublisher, NatsPublisher>();
        services.AddSingleton<NatsCacheFactory>();

        foreach (var handlerType in registration.ServiceTypes)
        {
            services.AddTransient(handlerType);

            var requestHandlerInterface = NatsMessagingRegistration.FindInterface(handlerType, typeof(INatsRequestHandler<,>));
            if (requestHandlerInterface != null)
            {
                services.AddTransient(requestHandlerInterface, handlerType);
            }
        }

        // "opts with", not "NatsOpts.Default with", keeps the LoggerFactory the hosting package sets.
        services.AddNats(configureOpts: opts =>
        {
            opts = opts with
            {
                Url = options.NatsUrl,
                Name = options.ServiceName ?? opts.Name,
                SerializerRegistry = serializerRegistry,
                AuthOpts = opts.AuthOpts with { CredsFile = options.CredsFilePath },
                RetryOnInitialConnect = options.RetryOnInitialConnect
            };
            return options.ConfigureNatsOpts?.Invoke(opts) ?? opts;
        });

        services.AddSingleton<NatsJSContext>(sp => new NatsJSContext(sp.GetRequiredService<NatsConnection>()));
        services.AddSingleton<StreamProvisioner>();
        services.AddSingleton(sp => new NatsDeadLetters(
            sp.GetRequiredService<INatsConnection>(),
            sp.GetRequiredService<NatsJSContext>(),
            options,
            sp.GetRequiredService<ILogger<NatsDeadLetters>>()));

        services.AddSingleton(options);
        services.AddSingleton(registration);
        services.AddSingleton(_ =>
        {
            var status = new NatsMessagingStatus();
            foreach (var (name, kind) in registration.Components(options))
            {
                status.Register(name, kind);
            }

            return status;
        });

        services.AddHostedService<NatsMessagingHostedService>();

        return services;
    }
}
