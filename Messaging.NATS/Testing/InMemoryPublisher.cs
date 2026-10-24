using NATS.Client.Core;

namespace Messaging.NATS.Testing;

/// <summary>
/// One call recorded by <see cref="InMemoryPublisher"/>.
/// </summary>
/// <param name="Subject">The target subject.</param>
/// <param name="Message">The message as passed in.</param>
/// <param name="MessageType">The type the real publisher would serialise with: T for the generic overloads, the
/// runtime type for the 4.x overload.</param>
/// <param name="DeferredBy">The 4.x overload's deferredBy, as passed in.</param>
/// <param name="DeliverAt">Set for schedules.</param>
/// <param name="ScheduleId">Set for schedules: the id returned in <see cref="ScheduleResult.ScheduleId"/>.</param>
/// <param name="ScheduleKey">Set for schedules: the key, generated for unkeyed schedules.</param>
/// <param name="Mode">Set for schedules.</param>
/// <param name="Options">The <see cref="PublishOptions"/> of a publish; null for schedules and 4.x calls.</param>
public sealed record PublishedMessage(string Subject, object Message, Type MessageType,
    TimeSpan? DeferredBy, DateTimeOffset? DeliverAt, string? ScheduleId, string? ScheduleKey, ScheduleMode? Mode,
    PublishOptions? Options)
{
    /// <summary>
    /// The <see cref="ScheduleOptions"/> of a schedule; null otherwise.
    /// </summary>
    public ScheduleOptions? ScheduleOptions { get; init; }

    public bool IsScheduled => ScheduleId != null;
}

/// <summary>
/// A <see cref="IPublisher.CancelScheduleAsync(string, string)"/> call recorded by <see cref="InMemoryPublisher"/>.
/// </summary>
/// <param name="Subject">The target subject.</param>
/// <param name="Key">The schedule key.</param>
/// <param name="Removed">What the call returned: true when a pending schedule was removed.</param>
public sealed record CancelledSchedule(string Subject, string Key, bool Removed);

/// <summary>
/// A <see cref="IPublisher.RequestAsync{TRequest, TResponse}"/> call recorded by <see cref="InMemoryPublisher"/>.
/// </summary>
public sealed record RecordedRequest(string Subject, object? Request, Type RequestType, Type ResponseType,
    TimeSpan? Timeout);

/// <summary>
/// An <see cref="IPublisher"/> that records every call and applies the schedule semantics (Replace,
/// CreateIfAbsent, Cancel) to a list of pending schedules. Nothing is delivered. A schedule fires (it leaves
/// <see cref="PendingSchedules"/> for <see cref="FiredSchedules"/>) once its DeliverAt has passed on
/// <see cref="Time"/>, or when <see cref="Fire"/> is called, so CreateIfAbsent and Cancel behave as on the server
/// after the fire time. <see cref="FailWhen"/>, <see cref="FailCancelWhen"/> and <see cref="FailOn"/> make calls fail,
/// so the error paths of the code under test can run. Thread-safe.
/// </summary>
public sealed class InMemoryPublisher : IPublisher
{
    private const string StreamName = "in-memory";

    private readonly int _defaultRetryAttempts;
    private readonly List<Func<PublishedMessage, Exception?>> _failures = [];
    private readonly List<Func<CancelledSchedule, Exception?>> _cancelFailures = [];
    private readonly object _lock = new();
    private readonly List<PublishedMessage> _published = [];
    private readonly List<PublishedMessage> _pending = [];
    private readonly List<PublishedMessage> _fired = [];
    private readonly List<CancelledSchedule> _cancellations = [];
    private readonly List<RecordedRequest> _requests = [];
    private readonly Dictionary<string, ulong> _messageIds = [];
    private readonly Dictionary<string, ulong> _scheduleSequences = [];
    private readonly Dictionary<string, Func<object?, object?>> _responders = [];
    private ulong _sequence;

    /// <summary>
    /// A fake with the default <see cref="NatsMessagingOptions.PublishRetryAttempts"/> (2).
    /// </summary>
    public InMemoryPublisher()
        : this(new NatsMessagingOptions().PublishRetryAttempts)
    {
    }

    /// <summary>
    /// A fake that uses <see cref="NatsMessagingOptions.PublishRetryAttempts"/> of <paramref name="options"/>, as the
    /// real publisher does.
    /// </summary>
    public InMemoryPublisher(NatsMessagingOptions options)
        : this(options.PublishRetryAttempts)
    {
    }

    /// <param name="defaultRetryAttempts">Used when a publish sets no <see cref="PublishOptions.RetryAttempts"/>.
    /// With 0 such a publish gets no generated message id, so it is never a duplicate.</param>
    public InMemoryPublisher(int defaultRetryAttempts)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(defaultRetryAttempts);
        _defaultRetryAttempts = defaultRetryAttempts;
    }

    /// <summary>
    /// The clock that decides when a schedule fires. Defaults to <see cref="TimeProvider.System"/>; pass a fake to
    /// move time in a test.
    /// </summary>
    public TimeProvider Time { get; init; } = TimeProvider.System;

    /// <summary>
    /// Every publish and schedule, in order, including the 4.x overload. Cancels are in <see cref="Cancellations"/>
    /// and requests in <see cref="Requests"/>.
    /// </summary>
    public IReadOnlyList<PublishedMessage> Published
    {
        get
        {
            lock (_lock)
            {
                return _published.ToList();
            }
        }
    }

    /// <summary>
    /// The schedules that are still pending (not cancelled, replaced or fired), in the order they were stored.
    /// </summary>
    public IReadOnlyList<PublishedMessage> PendingSchedules
    {
        get
        {
            lock (_lock)
            {
                RetireDue();
                return _pending.ToList();
            }
        }
    }

    /// <summary>
    /// The schedules that fired, in the order they fired: their DeliverAt passed on <see cref="Time"/>, or
    /// <see cref="Fire"/> was called. Nothing is delivered.
    /// </summary>
    public IReadOnlyList<PublishedMessage> FiredSchedules
    {
        get
        {
            lock (_lock)
            {
                RetireDue();
                return _fired.ToList();
            }
        }
    }

    /// <summary>
    /// Every <see cref="CancelScheduleAsync(string, string)"/> call that did not fail, in order, with its result.
    /// </summary>
    public IReadOnlyList<CancelledSchedule> Cancellations
    {
        get
        {
            lock (_lock)
            {
                return _cancellations.ToList();
            }
        }
    }

    /// <summary>
    /// Every <see cref="RequestAsync{TRequest, TResponse}"/> call, in order, answered or not.
    /// </summary>
    public IReadOnlyList<RecordedRequest> Requests
    {
        get
        {
            lock (_lock)
            {
                return _requests.ToList();
            }
        }
    }

    /// <summary>
    /// Fires the pending schedule with <paramref name="scheduleId"/> now, before its DeliverAt. False when no such
    /// schedule is pending.
    /// </summary>
    public bool Fire(string scheduleId)
    {
        lock (_lock)
        {
            RetireDue();
            var index = _pending.FindIndex(p => p.ScheduleId == scheduleId);
            if (index < 0)
            {
                return false;
            }

            _fired.Add(_pending[index]);
            _pending.RemoveAt(index);
            return true;
        }
    }

    /// <summary>
    /// The recorded messages of type <typeparamref name="T"/>, optionally on one subject only.
    /// </summary>
    public IEnumerable<T> MessagesOf<T>(string? subject = null) =>
        Published.Where(p => subject == null || p.Subject == subject).Select(p => p.Message).OfType<T>();

    /// <summary>
    /// Answers <see cref="RequestAsync{TRequest, TResponse}"/> on <paramref name="subject"/>.
    /// </summary>
    public void OnRequest<TReq, TRes>(string subject, Func<TReq, TRes> responder)
    {
        lock (_lock)
        {
            _responders[subject] = request => responder((TReq)request!);
        }
    }

    /// <summary>
    /// Makes publishes and schedules fail (the 4.x overload included): the first rule that returns an exception
    /// wins, and the call's task faults with it, for example a <see cref="NatsPublishException"/> with
    /// <see cref="PublishFailure.NoStream"/> or a <see cref="NatsMessagingConfigurationException"/>. A failed call is
    /// not recorded and changes no pending schedule. Rules are kept until <see cref="ClearFailures"/>. Use
    /// <see cref="FailCancelWhen"/> for cancels.
    /// </summary>
    public void FailWhen(Func<PublishedMessage, Exception?> rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        lock (_lock)
        {
            _failures.Add(rule);
        }
    }

    /// <summary>
    /// Makes <see cref="CancelScheduleAsync(string, string)"/> fail, as <see cref="FailWhen"/> does for publishes.
    /// The rule sees the call with <see cref="CancelledSchedule.Removed"/> = false. A failed cancel is not recorded and
    /// removes nothing.
    /// </summary>
    public void FailCancelWhen(Func<CancelledSchedule, Exception?> rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        lock (_lock)
        {
            _cancelFailures.Add(rule);
        }
    }

    /// <summary>
    /// Every publish, schedule and cancel on <paramref name="subject"/> fails with <paramref name="exception"/>.
    /// </summary>
    public void FailOn(string subject, Exception exception)
    {
        FailWhen(p => p.Subject == subject ? exception : null);
        FailCancelWhen(c => c.Subject == subject ? exception : null);
    }

    /// <summary>
    /// Removes every <see cref="FailWhen"/> and <see cref="FailCancelWhen"/> rule.
    /// </summary>
    public void ClearFailures()
    {
        lock (_lock)
        {
            _failures.Clear();
            _cancelFailures.Clear();
        }
    }

    /// <summary>
    /// Forgets every call and every pending and fired schedule. Responders and failure rules are kept.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            _published.Clear();
            _pending.Clear();
            _fired.Clear();
            _cancellations.Clear();
            _requests.Clear();
            _messageIds.Clear();
            _scheduleSequences.Clear();
        }
    }

    public Task PublishAsync(string subject, object message, TimeSpan? deferredBy)
    {
        // The real overload checks only the message before it hands off to an async method.
        ArgumentNullException.ThrowIfNull(message);
        return OnTask(() => LegacyCore(subject, message, deferredBy));
    }

    private Task LegacyCore(string subject, object message, TimeSpan? deferredBy)
    {
        ArgumentException.ThrowIfNullOrEmpty(subject);

        lock (_lock)
        {
            if (deferredBy is { } delay && delay > TimeSpan.Zero)
            {
                ScheduleTargets.ValidateSubject(subject, nameof(subject));
                RetireDue();
                var key = NewId();
                var scheduled = new PublishedMessage(subject, message, message.GetType(), deferredBy,
                    Time.GetUtcNow() + delay, NewId(), key, ScheduleMode.Replace, null);
                if (Failure(scheduled) is { } failure)
                {
                    return Task.FromException(failure);
                }

                _published.Add(scheduled);
                _pending.Add(scheduled);
                _scheduleSequences[scheduled.ScheduleId!] = ++_sequence;
            }
            else
            {
                var published = new PublishedMessage(subject, message, message.GetType(), deferredBy, null, null, null,
                    null, null);
                if (Failure(published) is { } failure)
                {
                    return Task.FromException(failure);
                }

                _published.Add(published);
                _sequence++;
            }
        }

        return Task.CompletedTask;
    }

    public Task<PublishResult> PublishAsync<T>(string subject, T message) =>
        PublishAsync(subject, message, null, CancellationToken.None);

    public Task<PublishResult> PublishAsync<T>(string subject, T message, CancellationToken cancellationToken) =>
        PublishAsync(subject, message, null, cancellationToken);

    public Task<PublishResult> PublishAsync<T>(string subject, T message, PublishOptions? options) =>
        PublishAsync(subject, message, options, CancellationToken.None);

    public Task<PublishResult> PublishAsync<T>(string subject, T message, PublishOptions? options,
        CancellationToken cancellationToken) =>
        OnTask(() => PublishCore(subject, message, options, cancellationToken));

    private Task<PublishResult> PublishCore<T>(string subject, T message, PublishOptions? options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrEmpty(subject);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_lock)
        {
            var published = new PublishedMessage(subject, message, typeof(T), null, null, null, null, null, options);
            if (Failure(published) is { } failure)
            {
                return Task.FromException<PublishResult>(failure);
            }

            _published.Add(published);

            if (options?.FireAndForget == true)
            {
                // Not acked, but stored, and the stream deduplicates on a MessageId that is set.
                if (options.MessageId == null || !_messageIds.ContainsKey(options.MessageId))
                {
                    _sequence++;
                    if (options.MessageId != null)
                    {
                        _messageIds[options.MessageId] = _sequence;
                    }
                }

                return Task.FromResult(new PublishResult(string.Empty, 0, false, options.MessageId));
            }

            var retries = options?.RetryAttempts ?? _defaultRetryAttempts;
            var messageId = options?.MessageId ?? (retries > 0 ? NewId() : null);
            if (messageId != null && _messageIds.TryGetValue(messageId, out var stored))
            {
                return Task.FromResult(new PublishResult(StreamName, stored, true, messageId));
            }

            _sequence++;
            if (messageId != null)
            {
                _messageIds[messageId] = _sequence;
            }

            return Task.FromResult(new PublishResult(StreamName, _sequence, false, messageId));
        }
    }

    public Task<ScheduleResult> ScheduleAsync<T>(string subject, T message, DateTimeOffset deliverAt) =>
        ScheduleAsync(subject, message, deliverAt, null, CancellationToken.None);

    public Task<ScheduleResult> ScheduleAsync<T>(string subject, T message, DateTimeOffset deliverAt,
        ScheduleOptions? options) =>
        ScheduleAsync(subject, message, deliverAt, options, CancellationToken.None);

    public Task<ScheduleResult> ScheduleAsync<T>(string subject, T message, DateTimeOffset deliverAt,
        ScheduleOptions? options, CancellationToken cancellationToken) =>
        OnTask(() => ScheduleCore(subject, message, deliverAt, options, cancellationToken));

    private Task<ScheduleResult> ScheduleCore<T>(string subject, T message, DateTimeOffset deliverAt,
        ScheduleOptions? options, CancellationToken cancellationToken)
    {
        ScheduleTargets.ValidateSubject(subject, nameof(subject));
        ArgumentNullException.ThrowIfNull(message);
        var keyed = options?.Key != null;
        if (keyed)
        {
            ScheduleTargets.ValidateKey(options!.Key, nameof(options));
        }

        if (options?.ScheduleId != null)
        {
            ScheduleTargets.ValidateScheduleId(options.ScheduleId, nameof(options));
        }

        cancellationToken.ThrowIfCancellationRequested();

        var scheduleId = options?.ScheduleId ?? NewId();
        var key = options?.Key ?? (options?.ScheduleId != null ? scheduleId : NewId());
        var mode = keyed ? options!.Mode : ScheduleMode.Replace;

        lock (_lock)
        {
            RetireDue();
            var scheduled = new PublishedMessage(subject, message, typeof(T), null, deliverAt, scheduleId, key, mode, null)
            {
                ScheduleOptions = options
            };
            if (Failure(scheduled) is { } failure)
            {
                return Task.FromException<ScheduleResult>(failure);
            }

            // A retry with the same ScheduleId is a duplicate for the stream, pending or fired.
            if (options?.ScheduleId != null && _scheduleSequences.TryGetValue(scheduleId, out var storedSequence))
            {
                _published.Add(scheduled);
                return Task.FromResult(new ScheduleResult(scheduleId, subject, key, true, storedSequence, deliverAt));
            }

            var existing = _pending.FindIndex(p => p.Subject == subject && p.ScheduleKey == key);
            if (existing >= 0 && mode == ScheduleMode.CreateIfAbsent)
            {
                var pending = _pending[existing];
                _published.Add(scheduled with { ScheduleId = pending.ScheduleId });
                return Task.FromResult(new ScheduleResult(pending.ScheduleId!, subject, key, false,
                    _scheduleSequences[pending.ScheduleId!], deliverAt));
            }

            if (existing >= 0)
            {
                _pending.RemoveAt(existing);
            }

            _published.Add(scheduled);
            _pending.Add(scheduled);
            _scheduleSequences[scheduled.ScheduleId!] = ++_sequence;
            return Task.FromResult(new ScheduleResult(scheduled.ScheduleId!, subject, key, true, _sequence, deliverAt));
        }
    }

    public Task<bool> CancelScheduleAsync(string subject, string key) =>
        CancelScheduleAsync(subject, key, CancellationToken.None);

    public Task<bool> CancelScheduleAsync(string subject, string key, CancellationToken cancellationToken) =>
        OnTask(() => CancelCore(subject, key, cancellationToken));

    private Task<bool> CancelCore(string subject, string key, CancellationToken cancellationToken)
    {
        ScheduleTargets.ValidateSubject(subject, nameof(subject));
        ScheduleTargets.ValidateKey(key, nameof(key));
        cancellationToken.ThrowIfCancellationRequested();

        lock (_lock)
        {
            RetireDue();
            var call = new CancelledSchedule(subject, key, false);
            foreach (var rule in _cancelFailures)
            {
                if (rule(call) is { } failure)
                {
                    return Task.FromException<bool>(failure);
                }
            }

            var removed = _pending.RemoveAll(p => p.Subject == subject && p.ScheduleKey == key) > 0;
            _cancellations.Add(call with { Removed = removed });
            return Task.FromResult(removed);
        }
    }

    /// <summary>
    /// Answers with the responder registered through <see cref="OnRequest{TReq, TRes}"/>.
    /// </summary>
    /// <exception cref="NatsNoRespondersException">No responder is registered for the subject.</exception>
    public Task<TResponse?> RequestAsync<TRequest, TResponse>(string subject, TRequest request,
        TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        OnTask(() => RequestCore<TRequest, TResponse>(subject, request, timeout, cancellationToken));

    private Task<TResponse?> RequestCore<TRequest, TResponse>(string subject, TRequest request, TimeSpan? timeout,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Func<object?, object?>? responder;
        lock (_lock)
        {
            _requests.Add(new RecordedRequest(subject, request, typeof(TRequest), typeof(TResponse), timeout));
            _responders.TryGetValue(subject, out responder);
        }

        if (responder == null)
        {
            throw new NatsNoRespondersException();
        }

        return responder(request) switch
        {
            TResponse response => Task.FromResult<TResponse?>(response),
            null => Task.FromResult<TResponse?>(default),
            var other => throw new InvalidOperationException(
                $"The responder for '{subject}' returned {other.GetType().Name}, not {typeof(TResponse).Name}")
        };
    }

    // Called under _lock. The server fires a schedule once its time has passed, and storing the fire removes its
    // holder, so CreateIfAbsent then creates and Cancel finds nothing.
    private void RetireDue()
    {
        var now = Time.GetUtcNow();
        if (_pending.All(p => p.DeliverAt > now))
        {
            return;
        }

        _fired.AddRange(_pending.Where(p => p.DeliverAt <= now).OrderBy(p => p.DeliverAt));
        _pending.RemoveAll(p => p.DeliverAt <= now);
    }

    // Called under _lock.
    private Exception? Failure(PublishedMessage call)
    {
        foreach (var rule in _failures)
        {
            if (rule(call) is { } exception)
            {
                return exception;
            }
        }

        return null;
    }

    private static string NewId() => Guid.NewGuid().ToString("N");

    // The real publisher's methods are async, so a validation error or a cancellation surfaces on the returned
    // task (a cancellation as a cancelled task), not from the call itself.
    private static async Task<T> OnTask<T>(Func<Task<T>> call) => await call().ConfigureAwait(false);

    private static async Task OnTask(Func<Task> call) => await call().ConfigureAwait(false);
}
