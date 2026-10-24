using System.Reflection;
using System.Text.RegularExpressions;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

internal sealed record ReplyHandlerRegistration(Type HandlerType, Type RequestType, Type ResponseType, string Subject)
{
    public string ComponentName => $"reply:{Subject}";
}

/// <summary>
/// Everything found and checked at registration: handler types, consumer durables and reply handlers.
/// Problems throw here, before the host starts.
/// </summary>
internal sealed partial class NatsMessagingRegistration
{
    public const string ConnectionComponent = "connection";
    public const string DeadLetterComponent = "dlq";

    private NatsMessagingRegistration(IReadOnlyList<Type> serviceTypes, IReadOnlyList<ConsumerDurable> consumers,
        IReadOnlyList<ReplyHandlerRegistration> replyHandlers)
    {
        ServiceTypes = serviceTypes;
        Consumers = consumers;
        ReplyHandlers = replyHandlers;
    }

    /// <summary>
    /// Handler classes to register in DI (transient), each with the handler interfaces it implements.
    /// </summary>
    public IReadOnlyList<Type> ServiceTypes { get; }

    public IReadOnlyList<ConsumerDurable> Consumers { get; }

    public IReadOnlyList<ReplyHandlerRegistration> ReplyHandlers { get; }

    public static string StreamComponent(string stream) => $"stream:{stream}";

    public static NatsMessagingRegistration Create(NatsMessagingOptions options)
    {
        ValidateOptions(options);

        var explicitTypes = options.HandlerTypes.Concat(options.ConsumerAttributeOverrides.Keys).ToList();
        var candidates = options.Assemblies.SelectMany(a => a.GetTypes())
            .Concat(explicitTypes)
            .Where(t => t is { IsAbstract: false, IsInterface: false })
            .Distinct()
            .ToList();

        foreach (var type in explicitTypes)
        {
            if (ConsumerDiscovery.AttributesOf(type, options).Count == 0 &&
                type.GetCustomAttribute<NatsRequestHandlerAttribute>() == null)
            {
                throw new InvalidOperationException(
                    $"Handler type {type.FullName} has neither a [NatsConsumer] nor a [NatsRequestHandler] attribute");
            }

            if (type.IsAbstract || type.IsInterface)
            {
                throw new InvalidOperationException($"Handler type {type.FullName} must be a concrete class");
            }
        }

        var serviceTypes = candidates
            .Where(t => ConsumerDiscovery.HandlerInterfaces(t).Count > 0 ||
                        FindInterface(t, typeof(INatsRequestHandler<,>)) != null)
            .ToList();

        var consumers = ConsumerDiscovery.Discover(candidates, options);

        var replyHandlers = candidates
            .Where(t => t.GetCustomAttribute<NatsRequestHandlerAttribute>() != null)
            .Select(CreateReplyHandler)
            .ToList();

        var duplicateReply = replyHandlers.GroupBy(r => r.Subject).FirstOrDefault(g => g.Count() > 1);
        if (duplicateReply != null)
        {
            throw new InvalidOperationException(
                $"Several request handlers use subject '{duplicateReply.Key}': " +
                string.Join(", ", duplicateReply.Select(r => r.HandlerType.FullName)));
        }

        ValidateDeadLetterStream(options, consumers);

        return new NatsMessagingRegistration(serviceTypes, consumers, replyHandlers);
    }

    /// <summary>
    /// Every component in start order, with its kind.
    /// </summary>
    public IEnumerable<(string Name, ComponentKind Kind)> Components(NatsMessagingOptions options)
    {
        yield return (ConnectionComponent, ComponentKind.Connection);

        foreach (var stream in options.StreamDefinitions)
        {
            yield return (StreamComponent(stream.Name), ComponentKind.Stream);
        }

        if (Consumers.Count > 0)
        {
            yield return (DeadLetterComponent, ComponentKind.DeadLetter);
        }

        foreach (var consumer in Consumers)
        {
            yield return (consumer.ComponentName, ComponentKind.Consumer);
        }

        foreach (var reply in ReplyHandlers)
        {
            yield return (reply.ComponentName, ComponentKind.ReplyHandler);
        }
    }

    private static void ValidateOptions(NatsMessagingOptions options)
    {
        if (string.IsNullOrEmpty(options.NatsUrl))
        {
            throw new ArgumentException("NatsUrl must be provided", nameof(options.NatsUrl));
        }

        Require(options.PublishTimeout > TimeSpan.Zero, nameof(options.PublishTimeout), "must be greater than zero");
        Require(options.PublishRetryAttempts >= 0, nameof(options.PublishRetryAttempts), "must be zero or more");
        Require(options.MaxDeliverySlack >= 1, nameof(options.MaxDeliverySlack), "must be at least 1");
        Require(options.StartTimeout > TimeSpan.Zero, nameof(options.StartTimeout), "must be greater than zero");
        Require(options.ShutdownTimeout >= TimeSpan.Zero, nameof(options.ShutdownTimeout), "must not be negative");
        Require(options.FaultedRetryInterval > TimeSpan.Zero, nameof(options.FaultedRetryInterval), "must be greater than zero");

        var prefix = options.DeadLetterSubjectPrefix;
        Require(SubjectMatcher.IsValid(prefix) && !prefix.Contains('*') && !prefix.Contains('>'),
            nameof(options.DeadLetterSubjectPrefix), "must be a subject without wildcards");

        foreach (var definition in options.StreamDefinitions)
        {
            ValidateStream(definition, nameof(options.StreamDefinitions));
        }

        var duplicateName = options.StreamDefinitions.GroupBy(d => d.Name).FirstOrDefault(g => g.Count() > 1);
        if (duplicateName != null)
        {
            throw new ArgumentException($"Stream '{duplicateName.Key}' is declared more than once",
                nameof(options.StreamDefinitions));
        }

        var declared = options.StreamDefinitions.ToList();
        if (options.DeadLetterStream is { } dlq)
        {
            ValidateStream(dlq, nameof(options.DeadLetterStream));
            if (dlq.Retention == StreamConfigRetention.Interest)
            {
                throw new ArgumentException(
                    $"DeadLetterStream '{dlq.Name}' has Interest retention. Nothing consumes the dead-letter " +
                    "subjects, so the stream would acknowledge each dead letter and keep none. Use Limits retention " +
                    "(StreamDefinition.DeadLetter).", nameof(options.DeadLetterStream));
            }

            if (declared.Any(d => d.Name == dlq.Name))
            {
                throw new ArgumentException(
                    $"Stream '{dlq.Name}' is both the DeadLetterStream and in StreamDefinitions; declare it in one place only",
                    nameof(options.StreamDefinitions));
            }

            declared.Add(dlq);
        }

        for (var i = 0; i < declared.Count; i++)
        {
            for (var j = i + 1; j < declared.Count; j++)
            {
                var overlap = declared[i].Subjects.SelectMany(a => declared[j].Subjects.Select(b => (a, b)))
                    .FirstOrDefault(p => SubjectMatcher.Overlaps(p.a, p.b));
                if (overlap != default)
                {
                    throw new ArgumentException(
                        $"Streams '{declared[i].Name}' and '{declared[j].Name}' overlap: '{overlap.a}' and '{overlap.b}'. " +
                        "A subject can belong to one stream only.", nameof(options.StreamDefinitions));
                }
            }
        }
    }

    private static void ValidateStream(StreamDefinition definition, string parameter)
    {
        var name = definition.Name;
        if (string.IsNullOrWhiteSpace(name) || InvalidNameCharacters().IsMatch(name))
        {
            throw new ArgumentException($"Stream name '{name}' is invalid", parameter);
        }

        if (definition.Subjects == null || definition.Subjects.Length == 0)
        {
            throw new ArgumentException($"Stream '{name}' has no subjects", parameter);
        }

        var badSubject = definition.Subjects.FirstOrDefault(s => !SubjectMatcher.IsValid(s));
        if (badSubject != null)
        {
            throw new ArgumentException($"Stream '{name}' has an invalid subject '{badSubject}'", parameter);
        }

        if (definition.Replicas is < 1 or > 5)
        {
            throw new ArgumentException($"Stream '{name}': Replicas must be between 1 and 5", parameter);
        }

        if (definition.DuplicateWindow is { } window)
        {
            if (window < TimeSpan.Zero)
            {
                throw new ArgumentException($"Stream '{name}': DuplicateWindow must not be negative", parameter);
            }

            if (definition.MaxAge > 0 && window > TimeSpan.FromSeconds(definition.MaxAge))
            {
                throw new ArgumentException($"Stream '{name}': DuplicateWindow must not be longer than MaxAge", parameter);
            }
        }

        var problems = ScheduleStreamRules.Check(definition);
        if (problems.Count > 0)
        {
            throw new ArgumentException(
                $"Stream '{name}' has AllowMsgSchedules but cannot hold schedules safely: {string.Join("; ", problems)}",
                parameter);
        }
    }

    private static void ValidateDeadLetterStream(NatsMessagingOptions options, IReadOnlyList<ConsumerDurable> consumers)
    {
        var explicitSubjects = consumers.SelectMany(c => c.Bindings).Select(b => b.DeadLetterSubject).OfType<string>()
            .Distinct().ToList();
        foreach (var explicitSubject in explicitSubjects)
        {
            if (!SubjectMatcher.IsValid(explicitSubject) || explicitSubject.Contains('*') || explicitSubject.Contains('>'))
            {
                throw new InvalidOperationException($"DeadLetterSubject '{explicitSubject}' must be a subject without wildcards");
            }
        }

        if (options.DeadLetterStream is not { } dlq)
        {
            return;
        }

        var required = explicitSubjects
            .Prepend($"{options.DeadLetterSubjectPrefix}.>")
            .Distinct();

        foreach (var subject in required)
        {
            if (!SubjectMatcher.IsCoveredBy(subject, dlq.Subjects))
            {
                throw new ArgumentException(
                    $"DeadLetterStream '{dlq.Name}' (subjects {string.Join(", ", dlq.Subjects)}) does not capture " +
                    $"dead-letter subject '{subject}'. Change DeadLetterStream.Subjects, DeadLetterSubjectPrefix or the " +
                    "consumer's DeadLetterSubject.", nameof(options.DeadLetterStream));
            }
        }
    }

    private static ReplyHandlerRegistration CreateReplyHandler(Type type)
    {
        var attribute = type.GetCustomAttribute<NatsRequestHandlerAttribute>()!;
        var handlerInterface = FindInterface(type, typeof(INatsRequestHandler<,>))
            ?? throw new InvalidOperationException(
                $"Type {type.FullName} has [NatsRequestHandler] but does not implement INatsRequestHandler<TRequest, TResponse>");

        if (!SubjectMatcher.IsValid(attribute.Subject))
        {
            throw new InvalidOperationException(
                $"Request handler {type.FullName} has a missing or invalid Subject '{attribute.Subject}'");
        }

        var args = handlerInterface.GetGenericArguments();
        return new ReplyHandlerRegistration(type, args[0], args[1], attribute.Subject);
    }

    internal static Type? FindInterface(Type type, Type openGeneric) =>
        type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == openGeneric);

    /// <summary>
    /// Replaces the characters JetStream does not allow in names. Names that are already valid do not change.
    /// </summary>
    internal static string CleanName(string name) => InvalidNameCharacters().Replace(name, "_");

    private static void Require(bool condition, string option, string message)
    {
        if (!condition)
        {
            throw new ArgumentException($"{option} {message}", option);
        }
    }

    [GeneratedRegex(@"[.*>\\/\s]")]
    private static partial Regex InvalidNameCharacters();
}
