namespace Messaging.NATS;

/// <summary>
/// A configuration problem that retrying will not fix: a stream or consumer that does not match what the
/// library needs, or a schedule target the server rejects.
/// </summary>
public class NatsMessagingConfigurationException : Exception
{
    public NatsMessagingConfigurationException(string message) : base(message) { }

    public NatsMessagingConfigurationException(string message, Exception innerException) : base(message, innerException) { }
}
