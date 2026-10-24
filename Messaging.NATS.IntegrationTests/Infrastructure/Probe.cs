using System.Collections.Concurrent;

namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// What a test handler saw: every call in order with its time, the peak number of calls running at once, and a
/// behaviour the test sets. Register one per host as a singleton.
/// </summary>
public sealed class Probe<T>
{
    private readonly ConcurrentQueue<Call<T>> _calls = new();
    private int _number;
    private int _running;
    private int _peak;

    /// <summary>
    /// Runs inside the handler with the call number (1-based). Return normally, throw, or wait.
    /// </summary>
    public Func<T, int, CancellationToken, Task> Behaviour { get; set; } = (_, _, _) => Task.CompletedTask;

    public IReadOnlyList<Call<T>> Calls => _calls.OrderBy(c => c.Number).ToList();

    public int Count => _calls.Count;

    public int Peak => Volatile.Read(ref _peak);

    public async Task InvokeAsync(T arg, CancellationToken cancellationToken)
    {
        var number = Interlocked.Increment(ref _number);
        _calls.Enqueue(new Call<T>(number, DateTime.UtcNow, arg));

        var running = Interlocked.Increment(ref _running);
        int peak;
        while (running > (peak = Volatile.Read(ref _peak)) &&
               Interlocked.CompareExchange(ref _peak, running, peak) != peak)
        {
        }

        try
        {
            await Behaviour(arg, number, cancellationToken);
        }
        finally
        {
            Interlocked.Decrement(ref _running);
        }
    }

    /// <summary>
    /// Waits until at least <paramref name="count"/> calls have started, and returns all calls.
    /// </summary>
    public async Task<IReadOnlyList<Call<T>>> WaitForCallsAsync(int count, TimeSpan? timeout = null)
    {
        var limit = timeout ?? TimeSpan.FromSeconds(20);
        var deadline = DateTime.UtcNow + limit;
        while (Count < count && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        Assert.IsGreaterThanOrEqualTo(count, Count, $"Expected {count} handler calls within {limit}, got {Count}");
        return Calls;
    }
}

public sealed record Call<T>(int Number, DateTime At, T Arg);

public static class Eventually
{
    /// <summary>
    /// Polls <paramref name="condition"/> until it is true, and fails the test when <paramref name="timeout"/> ends.
    /// </summary>
    public static async Task TrueAsync(Func<Task<bool>> condition, TimeSpan timeout, string message)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(50);
        }

        Assert.IsTrue(await condition(), message);
    }
}
