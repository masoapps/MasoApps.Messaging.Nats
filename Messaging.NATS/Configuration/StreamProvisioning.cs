namespace Messaging.NATS;

public enum StreamProvisioning
{
    /// <summary>
    /// Create the stream when it is absent. When it exists, log any drift from the definition and only apply
    /// additive schedule enablement.
    /// </summary>
    CreateIfMissing,

    /// <summary>
    /// Never create or update. Wait until the stream exists, then validate it.
    /// </summary>
    VerifyOnly,

    /// <summary>
    /// Create the stream, or update it to match the definition. Fields the definition does not model are kept.
    /// Use only in the service that owns the stream.
    /// </summary>
    CreateOrUpdate
}
