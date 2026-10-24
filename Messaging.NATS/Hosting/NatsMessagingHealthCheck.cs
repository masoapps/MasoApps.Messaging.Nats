using Microsoft.Extensions.Diagnostics.HealthChecks;
using NATS.Client.Core;

namespace Messaging.NATS;

/// <summary>
/// Reports the components that pass the filter:
/// Healthy when the connection is open and every selected component is Running;
/// Degraded when they have all started but one is Retrying, has a Warning, or the connection is reconnecting;
/// Unhealthy (the registration's failure status) when one is Faulted, Stopped, or has not reached Running yet, or the
/// connection is Failed (NATS.Net never leaves that state).
/// </summary>
internal sealed class NatsMessagingHealthCheck(
    NatsMessagingStatus status,
    INatsConnection connection,
    Func<ComponentStatus, bool>? componentFilter) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var failure = context.Registration?.FailureStatus ?? HealthStatus.Unhealthy;
        var selected = status.Components.Where(componentFilter ?? (_ => true)).ToList();
        var data = selected.ToDictionary(c => c.Name, c => (object)c);

        return Task.FromResult(Evaluate(selected, failure, data));
    }

    private HealthCheckResult Evaluate(List<ComponentStatus> selected, HealthStatus failure,
        IReadOnlyDictionary<string, object> data)
    {
        var faulted = selected.Where(c => c.State is ComponentState.Faulted or ComponentState.Stopped).ToList();
        if (faulted.Count > 0)
        {
            return new HealthCheckResult(failure, Describe(faulted), data: data);
        }

        var notStarted = selected.Where(c => !c.HasStarted).ToList();
        if (notStarted.Count > 0)
        {
            return new HealthCheckResult(failure, "Not started: " + Describe(notStarted), data: data);
        }

        if (connection.ConnectionState == NatsConnectionState.Failed)
        {
            return new HealthCheckResult(failure,
                "NATS connection is Failed: it stopped reconnecting (the same authentication error twice in a row) " +
                "and never recovers; restart the process", data: data);
        }

        if (connection.ConnectionState != NatsConnectionState.Open)
        {
            var started = status.Get(NatsMessagingRegistration.ConnectionComponent)?.HasStarted == true;
            return started
                ? HealthCheckResult.Degraded($"NATS connection is {connection.ConnectionState}", data: data)
                : new HealthCheckResult(failure, $"NATS connection is {connection.ConnectionState}", data: data);
        }

        var degraded = selected.Where(c => c.State == ComponentState.Retrying || c.Warning != null).ToList();
        if (degraded.Count > 0)
        {
            return HealthCheckResult.Degraded(Describe(degraded), data: data);
        }

        return HealthCheckResult.Healthy(data: data);
    }

    private static string Describe(IEnumerable<ComponentStatus> components) =>
        string.Join("; ", components.Select(c =>
        {
            var text = $"{c.Name} {c.State}";
            if (c.Error != null)
            {
                text += $": {c.Error}";
            }

            return c.Warning != null ? $"{text} (warning: {c.Warning})" : text;
        }));
}
