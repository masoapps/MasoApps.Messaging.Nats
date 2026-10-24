namespace Messaging.NATS;

public enum ComponentKind { Connection, Stream, DeadLetter, Consumer, ReplyHandler }

public enum ComponentState { Starting, Running, Retrying, Faulted, Stopped }

/// <summary>
/// The state of one messaging component. Names: "connection", "stream:{name}", "dlq",
/// "consumer:{stream}/{durable}", "reply:{subject}".
/// </summary>
public sealed record ComponentStatus(string Name, ComponentKind Kind, ComponentState State, string? Error,
    string? Warning, DateTimeOffset Since)
{
    /// <summary>
    /// True once the component has reached Running at least once.
    /// </summary>
    public bool HasStarted { get; init; }
}

/// <summary>
/// Live state of every messaging component: the connection, the provisioned streams, the dead-letter check,
/// consumers and reply handlers.
/// </summary>
public sealed class NatsMessagingStatus
{
    private readonly object _lock = new();
    private readonly Dictionary<string, ComponentStatus> _components = new();
    private readonly List<string> _order = new();
    private TaskCompletionSource _changed = NewSignal();

    /// <summary>
    /// True when every component has reached Running at least once.
    /// </summary>
    public bool IsStarted
    {
        get
        {
            lock (_lock)
            {
                return _components.Values.All(c => c.HasStarted);
            }
        }
    }

    public IReadOnlyCollection<ComponentStatus> Components
    {
        get
        {
            lock (_lock)
            {
                return _order.Select(n => _components[n]).ToArray();
            }
        }
    }

    public ComponentStatus? Get(string name)
    {
        lock (_lock)
        {
            return _components.GetValueOrDefault(name);
        }
    }

    /// <summary>
    /// Completes when every component has reached Running at least once.
    /// Throws <see cref="InvalidOperationException"/> as soon as a component is Faulted.
    /// </summary>
    public Task WaitUntilStartedAsync(CancellationToken cancellationToken = default) =>
        WaitAsync(c => c, throwOnFault: true, cancellationToken);

    /// <summary>
    /// Completes when the named component has reached Running at least once. Keeps waiting while it is Faulted.
    /// </summary>
    internal Task WaitUntilRunningAsync(string name, CancellationToken cancellationToken) =>
        WaitAsync(c => c.Where(x => x.Name == name), throwOnFault: false, cancellationToken);

    internal void Register(string name, ComponentKind kind)
    {
        lock (_lock)
        {
            if (_components.ContainsKey(name))
            {
                return;
            }

            _components[name] = new ComponentStatus(name, kind, ComponentState.Starting, null, null, DateTimeOffset.UtcNow);
            _order.Add(name);
        }

        Signal();
    }

    internal void SetRunning(string name) =>
        Update(name, c => c with
        {
            State = ComponentState.Running,
            Error = null,
            HasStarted = true
        });

    internal void SetRetrying(string name, string error) =>
        Update(name, c => c with { State = ComponentState.Retrying, Error = error });

    internal void SetFaulted(string name, string error) =>
        Update(name, c => c with { State = ComponentState.Faulted, Error = error });

    internal void SetStopped(string name) =>
        Update(name, c => c with { State = ComponentState.Stopped });

    internal void SetWarning(string name, string? warning) =>
        Update(name, c => c with { Warning = warning }, keepSince: true);

    private void Update(string name, Func<ComponentStatus, ComponentStatus> change, bool keepSince = false)
    {
        lock (_lock)
        {
            if (!_components.TryGetValue(name, out var current))
            {
                return;
            }

            var next = change(current);
            if (!keepSince && next.State != current.State)
            {
                next = next with { Since = DateTimeOffset.UtcNow };
            }

            _components[name] = next;
        }

        Signal();
    }

    private void Signal()
    {
        TaskCompletionSource previous;
        lock (_lock)
        {
            previous = _changed;
            _changed = NewSignal();
        }

        previous.TrySetResult();
    }

    private async Task WaitAsync(Func<IEnumerable<ComponentStatus>, IEnumerable<ComponentStatus>> select,
        bool throwOnFault, CancellationToken cancellationToken)
    {
        while (true)
        {
            Task changed;
            lock (_lock)
            {
                var selected = select(_order.Select(n => _components[n])).ToArray();
                var faulted = selected.FirstOrDefault(c => c.State == ComponentState.Faulted);
                if (throwOnFault && faulted != null)
                {
                    throw new InvalidOperationException(
                        $"NATS messaging component '{faulted.Name}' is faulted: {faulted.Error}");
                }

                if (selected.All(c => c.HasStarted))
                {
                    return;
                }

                changed = _changed.Task;
            }

            await changed.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
