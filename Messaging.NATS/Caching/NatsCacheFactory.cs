using System.Collections.Concurrent;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;

namespace Messaging.NATS;

public class NatsCacheFactory(NatsJSContext jsContext)
{
    private readonly NatsKVContext _context = new(jsContext);
    private readonly ConcurrentDictionary<string, INatsCache> _caches = new();
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public async Task<INatsCache> CreateAsync(string cacheName, TimeSpan? defaultTtl = null, CancellationToken cancellationToken = default)
    {
        if (_caches.TryGetValue(cacheName, out var existing))
            return existing;

        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            if (_caches.TryGetValue(cacheName, out existing))
                return existing;

            var kvConfig = new NatsKVConfig(cacheName)
            {
                MaxAge = defaultTtl ?? TimeSpan.FromDays(1)
            };

            var kv = await _context.CreateStoreAsync(kvConfig, cancellationToken: cancellationToken);
            var cache = new NatsCache(kv);
            _caches.TryAdd(cacheName, cache);
            return cache;
        }
        finally
        {
            _semaphore.Release();
        }
    }
}
