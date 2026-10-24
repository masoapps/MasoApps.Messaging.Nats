namespace Messaging.NATS;

/// <summary>
/// A condition the retry loop should wait out, such as a stream another service has not created yet.
/// </summary>
internal sealed class NatsMessagingTransientException(string message) : Exception(message);
