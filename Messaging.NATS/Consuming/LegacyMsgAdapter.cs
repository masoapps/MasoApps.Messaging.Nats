using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Messaging.NATS;

/// <summary>
/// The <see cref="INatsJSMsg{T}"/> a legacy <see cref="INatsConsumer{TMessage}"/> receives. It forwards every call
/// to the delivered message, and records whether the handler settled it (Ack, Nak or Term; AckProgress does not
/// count), so the library acks only messages the handler left unsettled. A null or unreadable payload is passed
/// through as in 4.x: <see cref="Data"/> is null and <see cref="Error"/> says why.
/// </summary>
internal sealed class LegacyMsgAdapter<T> : INatsJSMsg<T>
{
    private readonly INatsJSMsg<byte[]> _msg;
    private readonly MessageSettlement _settlement;

    public LegacyMsgAdapter(INatsJSMsg<byte[]> msg, T? data, NatsException? error, MessageSettlement settlement)
    {
        _msg = msg;
        Data = data;
        Error = error;
        _settlement = settlement;
    }

    public string Subject => _msg.Subject;

    public int Size => _msg.Size;

    public T? Data { get; }

    public INatsConnection? Connection => _msg.Connection;

    public NatsJSMsgMetadata? Metadata => _msg.Metadata;

    public string? ReplyTo => _msg.ReplyTo;

    public NatsException? Error { get; }

    public NatsHeaders? Headers => _msg.Headers;

    public void EnsureSuccess()
    {
        if (Error != null)
        {
            throw Error;
        }
    }

    [Obsolete("ReplyAsync is not valid when using JetStream. The reply message will never reach the Requestor as NATS will reply to all JetStream publish by default.")]
    public ValueTask ReplyAsync(NatsHeaders? headers = null, string? replyTo = null, NatsPubOpts? opts = null,
        CancellationToken cancellationToken = default) =>
        _msg.ReplyAsync(headers, replyTo, opts, cancellationToken);

    public ValueTask AckAsync(AckOpts? opts = null, CancellationToken cancellationToken = default) =>
        _settlement.SettleForHandlerAsync(SettleKind.Ack, (m, o, ct) => m.AckAsync(o, ct), opts, cancellationToken);

    public ValueTask NakAsync(AckOpts? opts = null, CancellationToken cancellationToken = default) =>
        _settlement.SettleForHandlerAsync(SettleKind.Nak, (m, o, ct) => m.NakAsync(o, ct), opts, cancellationToken);

    public ValueTask AckTerminateAsync(AckOpts? opts = null, CancellationToken cancellationToken = default) =>
        _settlement.SettleForHandlerAsync(SettleKind.Term, (m, o, ct) => m.AckTerminateAsync(o, ct), opts, cancellationToken);

    public ValueTask AckProgressAsync(AckOpts? opts = null, CancellationToken cancellationToken = default) =>
        _settlement.ProgressForHandlerAsync(opts, cancellationToken);
}
