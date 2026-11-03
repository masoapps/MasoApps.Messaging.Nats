namespace Messaging.NATS;

/// <summary>
/// Serves an <see cref="INatsRequestHandler{TRequest,TResponse}"/> on a core NATS subject. The instances of a
/// service share a queue group, so one of them answers each request.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public class NatsRequestHandlerAttribute : Attribute
{
    public string Subject { get; set; } = string.Empty;

    /// <summary>
    /// The core NATS queue group. Instances that share it take turns, so one of them answers each request. When
    /// null, <see cref="NatsMessagingOptions.DefaultQueueGroup"/> is used, then
    /// <see cref="NatsMessagingOptions.ServiceName"/>. Whitespace becomes "_".
    /// </summary>
    public string? QueueGroup { get; set; }
}
