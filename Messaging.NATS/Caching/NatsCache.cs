using NATS.Client.KeyValueStore;

namespace Messaging.NATS;

public class NatsCache(INatsKVStore kv) : INatsCache
{
    public async Task<List<string>> ListKeys(string filter, CancellationToken cancellationToken = default)
    {
        var list = new List<string>();
        await foreach (var key in kv.GetKeysAsync([filter], cancellationToken: cancellationToken))
        {
            list.Add(key);
        }

        return list;
    }

    public async Task<CacheEntry<T>> GetAsync<T>(string key, CancellationToken cancellationToken = default) where T : class
    {
        var entry = await kv.GetEntryAsync<T>(key, cancellationToken: cancellationToken);
        return new CacheEntry<T>(entry.Value, entry.Revision);
    }

    public async Task CreateAsync<T>(string key, T value, CancellationToken cancellationToken = default) where T : class
    {
        await kv.PutAsync(key, value, cancellationToken: cancellationToken);
    }

    public async Task UpdateAsync<T>(string key, T value, ulong revision,
        CancellationToken cancellationToken = default) where T : class
    {
        await kv.UpdateAsync(key, value, revision, cancellationToken: cancellationToken);
    }

    public async Task DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        await kv.DeleteAsync(key, cancellationToken: cancellationToken);
    }
}
