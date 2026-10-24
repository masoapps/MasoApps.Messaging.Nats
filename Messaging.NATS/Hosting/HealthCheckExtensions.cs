using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using NATS.Client.Core;

namespace Messaging.NATS;

public static class HealthCheckExtensions
{
    /// <summary>
    /// Adds a health check over the NATS messaging components. Map it to the readiness probe.
    /// </summary>
    /// <param name="builder">The health checks builder</param>
    /// <param name="name">The check name</param>
    /// <param name="failureStatus">Status reported for a faulted or not yet started component (default Unhealthy)</param>
    /// <param name="tags">Tags for filtering checks, for example "ready"</param>
    /// <param name="componentFilter">The components to look at; null means all. See <see cref="NatsHealthFilters"/>.</param>
    public static IHealthChecksBuilder AddNatsMessagingCheck(this IHealthChecksBuilder builder,
        string name = "nats", HealthStatus? failureStatus = null, IEnumerable<string>? tags = null,
        Func<ComponentStatus, bool>? componentFilter = null)
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new NatsMessagingHealthCheck(
                sp.GetRequiredService<NatsMessagingStatus>(),
                sp.GetRequiredService<INatsConnection>(),
                componentFilter),
            failureStatus,
            tags));
    }
}

public static class NatsHealthFilters
{
    /// <summary>
    /// Only the connection. For services where a broken consumer must not fail readiness.
    /// </summary>
    public static readonly Func<ComponentStatus, bool> ConnectionOnly = c => c.Kind == ComponentKind.Connection;

    /// <summary>
    /// The connection, streams, the dead-letter check and reply handlers; consumers are left out.
    /// </summary>
    public static readonly Func<ComponentStatus, bool> ExcludeConsumers = c => c.Kind != ComponentKind.Consumer;
}
