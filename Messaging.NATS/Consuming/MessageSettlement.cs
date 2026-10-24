using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;

namespace Messaging.NATS;

/// <summary>
/// The settle state of one delivery. Sends the in-progress heartbeat from dispatch until the first settle (Ack, Nak
/// or Term), and records whether anyone settled. Settles use DoubleAck when <c>confirm</c> is set, or when the worker
/// is stopping.
/// </summary>
internal sealed class MessageSettlement : IAsyncDisposable
{
    private readonly INatsJSMsg<byte[]> _msg;
    private readonly Func<bool> _stopping;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _heartbeat = new();
    private Task _heartbeatLoop = Task.CompletedTask;
    private int _settled; // the SettleKind of the first settle

    public MessageSettlement(INatsJSMsg<byte[]> msg, Func<bool> stopping, ILogger logger)
    {
        _msg = msg;
        _stopping = stopping;
        _logger = logger;
    }

    public bool IsSettled => Volatile.Read(ref _settled) != (int)SettleKind.None;

    /// <summary>
    /// The first settle (the one that counts), or <see cref="SettleKind.None"/>.
    /// </summary>
    public SettleKind Kind => (SettleKind)Volatile.Read(ref _settled);

    /// <summary>
    /// Sends AckProgress every <paramref name="interval"/> until the message is settled.
    /// </summary>
    public void StartHeartbeat(TimeSpan interval) => _heartbeatLoop = HeartbeatAsync(interval, _heartbeat.Token);

    /// <summary>
    /// Stops the heartbeat without settling, so the server redelivers the message after AckWait.
    /// </summary>
    public void StopHeartbeat()
    {
        try
        {
            _heartbeat.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    public Task AckAsync(bool confirm = false) =>
        SettleQuietlyAsync(SettleKind.Ack, opts => _msg.AckAsync(opts), new AckOpts { DoubleAck = Confirm(confirm) });

    public Task NakAsync(TimeSpan delay) =>
        SettleQuietlyAsync(SettleKind.Nak, opts => _msg.NakAsync(opts),
            new AckOpts { DoubleAck = Confirm(false), NakDelay = delay > TimeSpan.Zero ? delay : null });

    /// <summary>
    /// A settle the handler asked for (legacy adapter). Exceptions reach the handler, as in 4.x.
    /// </summary>
    public ValueTask SettleForHandlerAsync(SettleKind kind,
        Func<INatsJSMsg<byte[]>, AckOpts?, CancellationToken, ValueTask> settle, AckOpts? opts,
        CancellationToken cancellationToken)
    {
        MarkSettled(kind);
        var effective = (opts ?? new AckOpts()) with { DoubleAck = opts?.DoubleAck ?? (_stopping() ? true : null) };
        return settle(_msg, effective, cancellationToken);
    }

    public ValueTask ProgressForHandlerAsync(AckOpts? opts, CancellationToken cancellationToken) =>
        _msg.AckProgressAsync(opts, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        await _heartbeat.CancelAsync().ConfigureAwait(false);
        await _heartbeatLoop.ConfigureAwait(false);
        _heartbeat.Dispose();
    }

    private bool? Confirm(bool confirm) => confirm || _stopping() ? true : null;

    private bool MarkSettled(SettleKind kind)
    {
        var first = Interlocked.CompareExchange(ref _settled, (int)kind, (int)SettleKind.None) == (int)SettleKind.None;
        if (first)
        {
            _heartbeat.Cancel();
        }

        return first;
    }

    private async Task SettleQuietlyAsync(SettleKind kind, Func<AckOpts, ValueTask> settle, AckOpts opts)
    {
        if (!MarkSettled(kind))
        {
            return;
        }

        try
        {
            await settle(opts).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // The server redelivers after AckWait, so a lost settle means a second delivery, not a lost message.
            _logger.LogWarning(e, "Could not {Kind} message {Stream}:{Sequence} on {Subject}", kind.ToString().ToLowerInvariant(),
                _msg.Metadata?.Stream, _msg.Metadata?.Sequence.Stream, _msg.Subject);
        }
    }

    private async Task HeartbeatAsync(TimeSpan interval, CancellationToken cancellationToken)
    {
        try
        {
            using var timer = new PeriodicTimer(interval);
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                if (IsSettled)
                {
                    return;
                }

                try
                {
                    await _msg.AckProgressAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    _logger.LogDebug(e, "In-progress heartbeat for {Subject} failed", _msg.Subject);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}

internal enum SettleKind { None, Ack, Nak, Term }
