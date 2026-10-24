using NATS.Client.JetStream;

namespace Messaging.NATS.IntegrationTests.Handlers;

/// <summary>
/// Test consumer with configurable failure behavior. Records the timestamp of every delivery
/// attempt so tests can assert backoff timing, and can be told to throw a regular exception, a
/// <see cref="PoisonMessageException"/>, or to succeed after N attempts.
/// </summary>
public class FlakyHandler : INatsConsumer<OrderEvent>
{
    public enum Mode { AlwaysThrow, AlwaysPoison, SucceedAfterAttempt }

    private readonly FlakyHandlerState _state;

    public FlakyHandler(FlakyHandlerState state)
    {
        _state = state;
    }

    public async Task HandleMessageAsync(INatsJSMsg<OrderEvent> message, CancellationToken cancellationToken)
    {
        var attempt = Interlocked.Increment(ref _state.Attempts);
        _state.AttemptTimestamps.Add(DateTime.UtcNow);

        switch (_state.Mode)
        {
            case Mode.AlwaysThrow:
                throw new InvalidOperationException($"flaky: attempt {attempt}");
            case Mode.AlwaysPoison:
                throw new PoisonMessageException($"poison: attempt {attempt}");
            case Mode.SucceedAfterAttempt when attempt < _state.SucceedOnAttempt:
                throw new InvalidOperationException($"flaky transient: attempt {attempt}");
            case Mode.SucceedAfterAttempt:
                await message.AckAsync(cancellationToken: cancellationToken);
                _state.Succeeded.TrySetResult(attempt);
                break;
        }
    }
}

public class FlakyHandlerState
{
    public FlakyHandler.Mode Mode { get; set; } = FlakyHandler.Mode.AlwaysThrow;
    public int SucceedOnAttempt { get; set; } = 1;
    public int Attempts;
    public List<DateTime> AttemptTimestamps { get; } = new();
    public TaskCompletionSource<int> Succeeded { get; } = new();
}
