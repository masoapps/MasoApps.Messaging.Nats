namespace Messaging.NATS;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public class NatsRequestHandlerAttribute : Attribute
{
    public string Subject { get; set; } = string.Empty;

    /// <summary>
    /// A core NATS queue group. When set, the instances that share it take turns: one of them answers each request.
    /// When null (the default), every instance answers each request and the caller takes the first reply. Set it when
    /// the handler has side effects or costs much to run. No whitespace.
    /// </summary>
    public string? QueueGroup { get; set; }
}
