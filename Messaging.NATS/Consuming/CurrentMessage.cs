namespace Messaging.NATS;

/// <summary>
/// The message whose handler is running on this async flow. The publisher reads it to give follow-on publishes and
/// schedules the same X-Correlation-Id, and the handled message's id as X-Causation-Id.
/// </summary>
internal sealed record CurrentMessage(string MessageId, string CorrelationId)
{
    private static readonly AsyncLocal<CurrentMessage?> Ambient = new();

    public static CurrentMessage? Value => Ambient.Value;

    /// <summary>
    /// Sets the current message until the returned scope is disposed. The correlation id is the handled message's
    /// X-Correlation-Id, or its message id when it has none, so the chain starts at the first handled message.
    /// </summary>
    public static Scope Enter(string messageId, string? correlationId)
    {
        var previous = Ambient.Value;
        Ambient.Value = new CurrentMessage(messageId, correlationId ?? messageId);
        return new Scope(previous);
    }

    public readonly struct Scope(CurrentMessage? previous) : IDisposable
    {
        public void Dispose() => Ambient.Value = previous;
    }
}
