namespace Messaging.NATS;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public class NatsRequestHandlerAttribute : Attribute
{
    public string Subject { get; set; } = string.Empty;
} 