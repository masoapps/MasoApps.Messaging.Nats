namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// Unique stream, subject and durable names, so tests can run in parallel on one server.
/// </summary>
public static class TestNames
{
    /// <summary>
    /// "{prefix}_{10 hex chars}". Valid as a stream name, a durable name and a subject token.
    /// </summary>
    public static string Unique(string prefix) => $"{prefix}_{Guid.NewGuid().ToString("N")[..10]}";
}
