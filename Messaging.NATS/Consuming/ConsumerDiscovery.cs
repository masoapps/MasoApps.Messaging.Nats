using System.Reflection;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// Turns handler classes and their <see cref="NatsConsumerAttribute"/>s into durables: one per (class, stream),
/// each with one binding per attribute. Every rule of design 2.2 that can be checked without a server throws here.
/// </summary>
internal static class ConsumerDiscovery
{
    internal static readonly TimeSpan DefaultAckWait = TimeSpan.FromSeconds(30);
    internal const int DefaultMaxAckPending = 1000;

    public static IReadOnlyList<ConsumerDurable> Discover(IEnumerable<Type> candidates, NatsMessagingOptions options)
    {
        var declared = options.StreamDefinitions
            .Concat(options.DeadLetterStream is { } dlq ? [dlq] : [])
            .ToDictionary(d => d.Name);

        var durables = new List<ConsumerDurable>();
        foreach (var type in candidates)
        {
            var attributes = AttributesOf(type, options);
            if (attributes.Count > 0)
            {
                durables.AddRange(DurablesOf(type, attributes, options, declared));
            }
        }

        var duplicate = durables.GroupBy(d => (d.Stream, d.Durable)).FirstOrDefault(g => g.Count() > 1);
        if (duplicate != null)
        {
            throw new InvalidOperationException(
                $"Durable '{duplicate.Key.Durable}' on stream '{duplicate.Key.Stream}' is used by " +
                $"{string.Join(", ", duplicate.Select(d => d.HandlerType.FullName))}. Durable names must be unique per " +
                "stream; set Durable or QueueGroup on one of them.");
        }

        return durables;
    }

    /// <summary>
    /// The attributes of <paramref name="type"/>. Tests can bind a class with attribute instances built at run time
    /// (<see cref="NatsMessagingOptions.ConsumerAttributeOverrides"/>), because attribute arguments are constants.
    /// </summary>
    public static IReadOnlyList<NatsConsumerAttribute> AttributesOf(Type type, NatsMessagingOptions options) =>
        options.ConsumerAttributeOverrides.TryGetValue(type, out var overrides)
            ? overrides
            : type.GetCustomAttributes<NatsConsumerAttribute>(inherit: false).ToArray();

    /// <summary>
    /// The handler interfaces of <paramref name="type"/>, inherited ones included: (message type, legacy).
    /// </summary>
    public static IReadOnlyList<(Type MessageType, bool IsLegacy)> HandlerInterfaces(Type type) =>
        type.GetInterfaces()
            .Where(i => i.IsGenericType &&
                        (i.GetGenericTypeDefinition() == typeof(INatsHandler<>) ||
                         i.GetGenericTypeDefinition() == typeof(INatsConsumer<>)))
            .Select(i => (i.GetGenericArguments()[0], i.GetGenericTypeDefinition() == typeof(INatsConsumer<>)))
            .ToList();

    private static IEnumerable<ConsumerDurable> DurablesOf(Type type, IReadOnlyList<NatsConsumerAttribute> attributes,
        NatsMessagingOptions options, IReadOnlyDictionary<string, StreamDefinition> declared)
    {
        var name = type.FullName ?? type.Name;
        var interfaces = HandlerInterfaces(type);
        if (interfaces.Count == 0)
        {
            throw new InvalidOperationException(
                $"Type {name} has [NatsConsumer] but implements neither INatsHandler<T> nor INatsConsumer<T>");
        }

        var both = interfaces.GroupBy(i => i.MessageType).FirstOrDefault(g => g.Count() > 1);
        if (both != null)
        {
            throw new InvalidOperationException(
                $"Consumer {name} implements both INatsHandler<{both.Key.Name}> and INatsConsumer<{both.Key.Name}>; " +
                "implement one of them");
        }

        var valueType = interfaces.FirstOrDefault(i => i.MessageType.IsValueType);
        if (valueType != default)
        {
            throw new InvalidOperationException(
                $"Consumer {name}: the message type {valueType.MessageType.Name} must be a reference type");
        }

        var bindings = new List<(NatsConsumerAttribute Attribute, ConsumerBinding Binding)>();
        foreach (var attribute in attributes)
        {
            var handled = ResolveMessageType(type, attribute, interfaces);
            bindings.Add((attribute, CreateBinding(type, attribute, handled.MessageType, handled.IsLegacy)));
        }

        var unbound = interfaces.FirstOrDefault(i => bindings.All(b => b.Binding.MessageType != i.MessageType));
        if (unbound != default)
        {
            throw new InvalidOperationException(
                $"Consumer {name} handles {unbound.MessageType.Name}, but no [NatsConsumer] has " +
                $"MessageType = typeof({unbound.MessageType.Name})");
        }

        foreach (var group in bindings.GroupBy(b => b.Attribute.Stream))
        {
            yield return CreateDurable(type, group.Key, group.ToList(), options, declared);
        }
    }

    private static (Type MessageType, bool IsLegacy) ResolveMessageType(Type type, NatsConsumerAttribute attribute,
        IReadOnlyList<(Type MessageType, bool IsLegacy)> interfaces)
    {
        if (attribute.MessageType == null)
        {
            if (interfaces.Count > 1)
            {
                throw new InvalidOperationException(
                    $"Consumer {type.FullName} handles several message types " +
                    $"({string.Join(", ", interfaces.Select(i => i.MessageType.Name))}), so every [NatsConsumer] " +
                    "must set MessageType");
            }

            return interfaces[0];
        }

        var match = interfaces.FirstOrDefault(i => i.MessageType == attribute.MessageType);
        if (match == default)
        {
            throw new InvalidOperationException(
                $"Consumer {type.FullName} has [NatsConsumer(MessageType = typeof({attribute.MessageType.Name}))] " +
                "but does not implement a handler interface for that type");
        }

        return match;
    }

    private static ConsumerBinding CreateBinding(Type type, NatsConsumerAttribute attribute, Type messageType,
        bool isLegacy)
    {
        var name = type.FullName;
        if (string.IsNullOrEmpty(attribute.Stream))
        {
            throw new InvalidOperationException($"Consumer {name} has [NatsConsumer] without a Stream");
        }

        if (attribute.Subject.Length > 0 && !SubjectMatcher.IsValid(attribute.Subject))
        {
            throw new InvalidOperationException($"Consumer {name} has an invalid Subject '{attribute.Subject}'");
        }

        if (attribute.MaxDeliveryAttempts < 1)
        {
            throw new InvalidOperationException($"Consumer {name}: MaxDeliveryAttempts must be at least 1");
        }

        if (attribute.RetryDelaysSeconds?.Any(d => d < 0) == true)
        {
            throw new InvalidOperationException($"Consumer {name}: RetryDelaysSeconds must not be negative");
        }

        var deadLetterSubject = string.IsNullOrEmpty(attribute.DeadLetterSubject) ? null : attribute.DeadLetterSubject;
        var delays = attribute.RetryDelaysSeconds?.Select(s => TimeSpan.FromSeconds(s)).ToArray()
                     ?? ConsumerBinding.DefaultRetryDelays;

        return new ConsumerBinding(type, messageType, isLegacy, attribute.Subject, attribute.MaxDeliveryAttempts,
            delays, deadLetterSubject);
    }

    private static ConsumerDurable CreateDurable(Type type, string stream,
        IReadOnlyList<(NatsConsumerAttribute Attribute, ConsumerBinding Binding)> group, NatsMessagingOptions options,
        IReadOnlyDictionary<string, StreamDefinition> declared)
    {
        var name = type.FullName;
        var attributes = group.Select(g => g.Attribute).ToList();
        var bindings = group.Select(g => g.Binding).ToList();

        var queueGroupSetting = Agree(attributes, a => a.QueueGroup, string.Empty, "QueueGroup", type, stream);
        var durableSetting = Agree(attributes, a => a.Durable ?? string.Empty, string.Empty, "Durable", type, stream);
        var ackWaitSeconds = Agree(attributes, a => a.AckWaitSeconds, 0, "AckWaitSeconds", type, stream);
        var maxConcurrency = Agree(attributes, a => a.MaxConcurrency, 0, "MaxConcurrency", type, stream);
        var maxMsgs = Agree(attributes, a => a.MaxMsgs, 0, "MaxMsgs", type, stream);
        var maxAckPending = Agree(attributes, a => a.MaxAckPending, 0, "MaxAckPending", type, stream);
        var deliverFrom = Agree(attributes, a => a.DeliverFrom, DeliverFrom.Default, "DeliverFrom", type, stream);
        var inactiveSeconds = Agree(attributes, a => a.InactiveThresholdSeconds, 0, "InactiveThresholdSeconds", type,
            stream);
        var maxHandlerSeconds = Agree(attributes, a => a.MaxHandlerSeconds, 0, "MaxHandlerSeconds", type, stream);

        if (ackWaitSeconds < 0 || maxConcurrency < 0 || maxMsgs < 0 || maxAckPending < 0 || inactiveSeconds < 0 ||
            maxHandlerSeconds < 0)
        {
            throw new InvalidOperationException(
                $"Consumer {name}: AckWaitSeconds, MaxConcurrency, MaxMsgs, MaxAckPending, InactiveThresholdSeconds " +
                "and MaxHandlerSeconds must not be negative");
        }

        if (options.MaxHandlerTime < TimeSpan.Zero && options.MaxHandlerTime != Timeout.InfiniteTimeSpan)
        {
            throw new InvalidOperationException("NatsMessagingOptions.MaxHandlerTime must not be negative");
        }

        maxConcurrency = maxConcurrency == 0 ? 1 : maxConcurrency;
        maxAckPending = maxAckPending == 0 ? DefaultMaxAckPending : maxAckPending;
        if (maxAckPending < Math.Max(maxConcurrency, maxMsgs))
        {
            throw new InvalidOperationException(
                $"Consumer {name}: MaxAckPending ({maxAckPending}) must be at least MaxConcurrency ({maxConcurrency}) " +
                $"and MaxMsgs ({maxMsgs})");
        }

        var queueGroup = new[] { queueGroupSetting, options.DefaultQueueGroup, options.ServiceName }
                             .FirstOrDefault(g => !string.IsNullOrEmpty(g))
                         ?? throw new InvalidOperationException(
                             $"Consumer {name} has no queue group. Set QueueGroup on the attribute, or " +
                             "DefaultQueueGroup or ServiceName on the options.");

        var durable = NatsMessagingRegistration.CleanName(
            durableSetting.Length > 0 ? durableSetting : $"{queueGroup}_{type.Name}");

        if (bindings.Count > 1 && bindings.Any(b => b.Filter.Length == 0))
        {
            throw new InvalidOperationException(
                $"Consumer {name} on stream '{stream}': an empty Subject is only allowed when it is the durable's only " +
                "binding");
        }

        for (var i = 0; i < bindings.Count; i++)
        {
            for (var j = i + 1; j < bindings.Count; j++)
            {
                if (SubjectMatcher.Overlaps(bindings[i].Filter, bindings[j].Filter))
                {
                    throw new InvalidOperationException(
                        $"Consumer {name} on stream '{stream}': subjects '{bindings[i].Filter}' and " +
                        $"'{bindings[j].Filter}' overlap. The filters of one durable must not match the same subject.");
                }
            }
        }

        if (declared.TryGetValue(stream, out var definition))
        {
            var problem = StreamProblem(bindings.Select(b => b.Filter), deliverFrom, stream,
                definition.AllowMsgSchedules, definition.Retention, TimeSpan.FromSeconds(inactiveSeconds));
            if (problem != null)
            {
                throw new InvalidOperationException($"Consumer {name}: {problem}");
            }
        }

        return new ConsumerDurable(type, stream, queueGroup, durable, options.ServiceName ?? queueGroup, bindings,
            ackWaitSeconds == 0 ? DefaultAckWait : TimeSpan.FromSeconds(ackWaitSeconds), maxConcurrency, maxMsgs,
            maxAckPending, deliverFrom, TimeSpan.FromSeconds(inactiveSeconds),
            maxHandlerSeconds > 0
                ? TimeSpan.FromSeconds(maxHandlerSeconds)
                : options.MaxHandlerTime > TimeSpan.Zero ? options.MaxHandlerTime : TimeSpan.Zero);
    }

    /// <summary>
    /// The stream-dependent rules: on a schedule stream no filter may be empty or match the holder namespace,
    /// a WorkQueue stream only allows DeliverPolicy All, and an Interest stream allows no InactiveThreshold. Null when
    /// the durable follows them.
    /// </summary>
    public static string? StreamProblem(IEnumerable<string> filters, DeliverFrom deliverFrom, string stream,
        bool allowMsgSchedules, StreamConfigRetention retention, TimeSpan inactiveThreshold)
    {
        if (allowMsgSchedules)
        {
            var holder = ScheduleStreamRules.HolderSubject(stream);
            foreach (var filter in filters)
            {
                if (filter.Length == 0)
                {
                    return $"stream '{stream}' has message schedules, so the consumer needs a Subject; an unfiltered " +
                           "consumer would also receive the pending schedules";
                }

                if (SubjectMatcher.Overlaps(filter, holder))
                {
                    return $"subject '{filter}' matches the schedule holders '{holder}' of stream '{stream}'; use a " +
                           "filter that only matches the target subjects";
                }
            }
        }

        if (retention == StreamConfigRetention.Workqueue && deliverFrom == DeliverFrom.New)
        {
            return $"stream '{stream}' has WorkQueue retention, which only allows DeliverFrom.All (or Default)";
        }

        // On an Interest stream the server removes a message once every consumer that is interested has acked it.
        // When it deletes an inactive durable, the messages only that durable wanted go with it (unacked ones too),
        // and messages published until it is created again are not stored at all.
        if (retention == StreamConfigRetention.Interest && inactiveThreshold > TimeSpan.Zero)
        {
            return $"stream '{stream}' has Interest retention, so InactiveThresholdSeconds would lose messages: when " +
                   "the server deletes the inactive durable it also removes the messages only that durable had not " +
                   "acked, and stores none for it until it is created again. Remove InactiveThresholdSeconds";
        }

        return null;
    }

    private static T Agree<T>(IReadOnlyList<NatsConsumerAttribute> attributes, Func<NatsConsumerAttribute, T> read,
        T unset, string property, Type type, string stream)
    {
        var values = attributes.Select(read).Where(v => !EqualityComparer<T>.Default.Equals(v, unset)).Distinct()
            .ToList();
        if (values.Count > 1)
        {
            throw new InvalidOperationException(
                $"Consumer {type.FullName} on stream '{stream}' sets {property} to different values " +
                $"({string.Join(", ", values)}). All bindings of a class on one stream share one durable, so " +
                "consumer-level settings must agree.");
        }

        return values.Count == 1 ? values[0] : unset;
    }
}
