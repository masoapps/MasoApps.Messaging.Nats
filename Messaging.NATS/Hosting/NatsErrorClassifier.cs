using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Messaging.NATS;

internal enum ErrorClass
{
    /// <summary>Retry with backoff.</summary>
    Transient,

    /// <summary>Retrying will not help until someone changes the configuration.</summary>
    Permanent,

    /// <summary>A JetStream error the table does not know. Treated as transient, but logged at Error.</summary>
    Unknown
}

/// <summary>
/// Classifies errors by JetStream err_code, never by HTTP code (10012, 10052 and 10069 are HTTP 500).
/// Codes checked against server/jetstream_errors_generated.go of nats-server 2.12.1.
/// </summary>
internal static class NatsErrorClassifier
{
    internal static readonly IReadOnlySet<int> PermanentCodes = new HashSet<int>
    {
        10012, // consumer create/update failed (includes "deliver policy can not be updated")
        10052, // invalid or immutable stream config
        10058, // stream name in use with a different config
        10065, // subjects overlap another stream
        10069, // stream update failed
        10093, // consumer filter not a subset of the stream subjects
        10094, // invalid consumer policy
        10099, // several unfiltered consumers on a work-queue stream
        10100, // filtered consumer not unique on a work-queue stream
        10101, // work-queue consumer must deliver all
        10116, // MaxDeliver must be greater than the number of Backoff values
        10136, // FilterSubject and FilterSubjects both set
        10138, // consumer filters overlap
        10139, // empty filter in FilterSubjects
        10188, // message schedules disabled on the stream
        10190, // invalid schedule target
    };

    internal static readonly IReadOnlySet<int> TransientCodes = new HashSet<int>
    {
        // every HTTP 503 code of 2.12.1
        10004, // incomplete results
        10008, // JetStream temporarily unavailable (cluster)
        10010, // clustering support required
        10023, // insufficient resources
        10035, // account not found
        10036, // not supported in clustered mode
        10039, // JetStream not enabled for account
        10063, // expected stream sequence does not match
        10075, // peer remap failed
        10076, // JetStream not enabled
        10077, // stream store failed
        10163, // expected last sequence per subject temporarily unavailable
        // not found: wait for the owner, or recreate
        10059, // stream not found
        10014, // consumer not found
    };

    public static ErrorClass Classify(Exception exception) => exception switch
    {
        NatsMessagingConfigurationException => ErrorClass.Permanent,

        // The connection gave up for good (the same authentication error twice in a row) and never reconnects.
        NatsConnectionFailedException => ErrorClass.Permanent,
        NatsMessagingTransientException => ErrorClass.Transient,
        NatsJSApiException api => ClassifyCode(api.Error.ErrCode),
        AggregateException { InnerExceptions.Count: 1 } aggregate => Classify(aggregate.InnerExceptions[0]),
        NatsException or TimeoutException or OperationCanceledException or ObjectDisposedException
            or IOException or System.Net.Sockets.SocketException => ErrorClass.Transient,
        _ => ErrorClass.Unknown
    };

    public static ErrorClass ClassifyCode(int errCode)
    {
        if (PermanentCodes.Contains(errCode))
        {
            return ErrorClass.Permanent;
        }

        return TransientCodes.Contains(errCode) ? ErrorClass.Transient : ErrorClass.Unknown;
    }
}
