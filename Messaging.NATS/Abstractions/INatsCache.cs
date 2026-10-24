namespace Messaging.NATS;

public interface INatsCache
{
    Task<List<string>> ListKeys(string filter, CancellationToken cancellationToken = default);
    Task<CacheEntry<T>> GetAsync<T>(string key, CancellationToken cancellationToken = default) where T : class;
    Task CreateAsync<T>(string key, T value, CancellationToken cancellationToken = default) where T : class;
    Task UpdateAsync<T>(string key, T value, ulong revision, CancellationToken cancellationToken = default) where T : class;
    Task DeleteAsync(string key, CancellationToken cancellationToken = default);
}
