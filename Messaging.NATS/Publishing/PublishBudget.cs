namespace Messaging.NATS;

/// <summary>
/// The time budget of one publisher call: the caller's token linked with a timeout. Every wait inside the call,
/// the reconnect wait in NATS.Net included, uses <see cref="Token"/>.
/// </summary>
internal sealed class PublishBudget : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly DateTime _deadline;

    public PublishBudget(TimeSpan timeout, CancellationToken callerToken)
    {
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "The publish timeout must be greater than zero");
        }

        CallerToken = callerToken;
        Timeout = timeout;
        _deadline = DateTime.UtcNow + timeout;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(callerToken);
        _cts.CancelAfter(timeout);
    }

    public CancellationToken Token => _cts.Token;

    public CancellationToken CallerToken { get; }

    public TimeSpan Timeout { get; }

    public TimeSpan Remaining
    {
        get
        {
            var remaining = _deadline - DateTime.UtcNow;
            return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
        }
    }

    /// <summary>
    /// True when the timeout (not the caller) ended the budget.
    /// </summary>
    public bool Expired => _cts.IsCancellationRequested && !CallerToken.IsCancellationRequested;

    public void Dispose() => _cts.Dispose();
}
