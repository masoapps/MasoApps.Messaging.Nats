using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// One (class, message type, attribute): the handler that runs for messages matching <see cref="Filter"/>.
/// </summary>
internal sealed record ConsumerBinding(
    Type HandlerType,
    Type MessageType,
    bool IsLegacy,
    string Filter,
    int MaxDeliveryAttempts,
    TimeSpan[] RetryDelays,
    string? DeadLetterSubject)
{
    public static readonly TimeSpan[] DefaultRetryDelays =
    [
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(30),
        TimeSpan.FromMinutes(2),
        TimeSpan.FromMinutes(10)
    ];

    /// <summary>
    /// The NAK delay after failed delivery <paramref name="deliveryCount"/> (1-based). The last entry repeats.
    /// </summary>
    public TimeSpan RetryDelay(int deliveryCount) =>
        RetryDelays.Length == 0 ? TimeSpan.Zero : RetryDelays[Math.Clamp(deliveryCount - 1, 0, RetryDelays.Length - 1)];

    public bool Matches(string subject) => Filter.Length == 0 || SubjectMatcher.IsSubsetOf(subject, Filter);
}

/// <summary>
/// One durable consumer: every binding of one class on one stream, with the consumer-level settings they agree on.
/// </summary>
internal sealed record ConsumerDurable(
    Type HandlerType,
    string Stream,
    string QueueGroup,
    string Durable,
    string Owner,
    IReadOnlyList<ConsumerBinding> Bindings,
    TimeSpan AckWait,
    int MaxConcurrency,
    int MaxMsgs,
    int MaxAckPending,
    DeliverFrom DeliverFrom,
    TimeSpan InactiveThreshold,
    TimeSpan MaxHandlerTime = default)
{
    public const string OwnerMetadata = "messaging-nats.owner";
    public const string HandlerMetadata = "messaging-nats.handler";

    public string ComponentName => $"consumer:{Stream}/{Durable}";

    /// <summary>
    /// Messages one pull asks for at most.
    /// </summary>
    public int BatchMax => MaxMsgs > 0 ? MaxMsgs : MaxConcurrency;

    /// <summary>
    /// The binding whose filter matches <paramref name="subject"/>; filters never overlap, so there is at most one.
    /// </summary>
    public ConsumerBinding? BindingFor(string subject)
    {
        foreach (var binding in Bindings)
        {
            if (binding.Matches(subject))
            {
                return binding;
            }
        }

        return null;
    }

    /// <summary>
    /// Sets the filter fields: FilterSubject for one filter, FilterSubjects for several (the server rejects both, 10136),
    /// neither for an unfiltered binding.
    /// </summary>
    public void ApplyFilters(ConsumerConfig config)
    {
        if (Bindings.Count == 1)
        {
            config.FilterSubject = Bindings[0].Filter.Length == 0 ? null : Bindings[0].Filter;
            config.FilterSubjects = null;
        }
        else
        {
            config.FilterSubject = null;
            config.FilterSubjects = Bindings.Select(b => b.Filter).ToList();
        }
    }

    public IDictionary<string, string> Metadata(IDictionary<string, string>? existing)
    {
        var metadata = existing == null ? new Dictionary<string, string>() : new Dictionary<string, string>(existing);
        metadata[OwnerMetadata] = Owner;
        metadata[HandlerMetadata] = HandlerType.FullName ?? HandlerType.Name;
        return metadata;
    }
}
