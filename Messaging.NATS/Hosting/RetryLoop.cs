using System.Diagnostics;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;

namespace Messaging.NATS;

/// <summary>
/// Runs one component's work until it succeeds or the host stops, and never gives up.
/// Transient errors back off (1 s doubling to 30 s, with ±20 % jitter); the delay resets once the component has run
/// for <see cref="StableRun"/>, so a component that fails right after it starts keeps backing off.
/// A permanent error marks the component Faulted, triggers the fault action once, and retries every
/// FaultedRetryInterval. The component stays Faulted until an attempt succeeds, or runs for StableRun.
/// </summary>
internal sealed class RetryLoop
{
    internal static readonly TimeSpan InitialDelay = TimeSpan.FromSeconds(1);
    internal static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(30);
    internal static readonly TimeSpan StableRun = TimeSpan.FromSeconds(30);

    private readonly string _component;
    private readonly NatsMessagingStatus _status;
    private readonly TimeSpan _faultedRetryInterval;
    private readonly Action<string, Exception> _onFaulted;
    private readonly ILogger _logger;

    public RetryLoop(string component, NatsMessagingStatus status, TimeSpan faultedRetryInterval,
        Action<string, Exception> onFaulted, ILogger logger)
    {
        _component = component;
        _status = status;
        _faultedRetryInterval = faultedRetryInterval;
        _onFaulted = onFaulted;
        _logger = logger;
    }

    /// <summary>
    /// Runs <paramref name="attempt"/> until it returns normally (the component is then Running and the loop ends)
    /// or <paramref name="cancellationToken"/> is cancelled. A long-running attempt calls
    /// <see cref="RetryAttempt.Running"/> once it is established, and throws when it stops working.
    /// </summary>
    public async Task RunAsync(Func<RetryAttempt, CancellationToken, Task> attempt, CancellationToken cancellationToken)
    {
        var failures = 0;
        var faulted = false;

        while (!cancellationToken.IsCancellationRequested)
        {
            var context = new RetryAttempt(_component, _status);
            TimeSpan delay;

            try
            {
                await attempt(context, cancellationToken).ConfigureAwait(false);
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                context.Running();
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                if (context.RanFor >= StableRun)
                {
                    failures = 0;
                    faulted = false;
                }

                var errorClass = NatsErrorClassifier.Classify(e);
                var error = Describe(e);

                if (errorClass == ErrorClass.Permanent || faulted)
                {
                    _status.SetFaulted(_component, error);
                    _logger.LogError(e, "NATS messaging component {Component} is faulted: {Error}. Retrying in {Delay}",
                        _component, error, _faultedRetryInterval);

                    if (!faulted)
                    {
                        faulted = true;
                        _onFaulted(_component, e);
                    }

                    delay = _faultedRetryInterval;
                }
                else
                {
                    failures++;
                    delay = Backoff(failures);
                    _status.SetRetrying(_component, error);

                    if (errorClass == ErrorClass.Unknown)
                    {
                        _logger.LogError(e,
                            "NATS messaging component {Component} failed with an unclassified error: {Error}. Retrying in {Delay}",
                            _component, error, delay);
                    }
                    else
                    {
                        _logger.LogWarning("NATS messaging component {Component} failed: {Error}. Retrying in {Delay}",
                            _component, error, delay);
                    }
                }
            }

            try
            {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    internal static TimeSpan Backoff(int failures)
    {
        var exponent = Math.Min(failures - 1, 10);
        var seconds = Math.Min(InitialDelay.TotalSeconds * Math.Pow(2, exponent), MaxDelay.TotalSeconds);
        var jitter = 0.8 + Random.Shared.NextDouble() * 0.4;
        return TimeSpan.FromSeconds(seconds * jitter);
    }

    internal static string Describe(Exception e) => e switch
    {
        NatsJSApiException api => $"{api.Error.Description} (err_code {api.Error.ErrCode})",
        NatsMessagingConfigurationException => e.Message,
        _ => $"{e.GetType().Name}: {e.Message}"
    };
}

internal sealed class RetryAttempt
{
    private readonly string _component;
    private readonly NatsMessagingStatus _status;

    public RetryAttempt(string component, NatsMessagingStatus status)
    {
        _component = component;
        _status = status;
    }

    private long _runningSince;

    /// <summary>
    /// How long the component has been Running in this attempt; zero when it never was.
    /// </summary>
    public TimeSpan RanFor
    {
        get
        {
            var since = Interlocked.Read(ref _runningSince);
            return since == 0 ? TimeSpan.Zero : Stopwatch.GetElapsedTime(since);
        }
    }

    /// <summary>
    /// Marks the component Running. The backoff resets when the attempt fails after running for
    /// <see cref="RetryLoop.StableRun"/>.
    /// </summary>
    public void Running()
    {
        Interlocked.CompareExchange(ref _runningSince, Stopwatch.GetTimestamp(), 0);
        _status.SetRunning(_component);
    }

    public void SetWarning(string? warning) => _status.SetWarning(_component, warning);

    /// <summary>
    /// Marks a running component Retrying (health Degraded) while the attempt keeps going, for a component that is
    /// still up but cannot work, such as a consumer whose fetches get no responders.
    /// </summary>
    public void SetStalled(string reason) => _status.SetRetrying(_component, reason);

    /// <summary>
    /// Ends <see cref="SetStalled"/>: the component is Running again.
    /// </summary>
    public void ClearStalled() => _status.SetRunning(_component);
}
