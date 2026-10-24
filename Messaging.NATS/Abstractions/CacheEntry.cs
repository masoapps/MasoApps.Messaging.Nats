namespace Messaging.NATS;

public record CacheEntry<T>(T? Value, ulong Revision);