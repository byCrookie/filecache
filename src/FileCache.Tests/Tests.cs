using System.Text;
using Microsoft.Extensions.Caching.Distributed;

namespace FileCache.Tests;

public class FileCacheTests
{
    [Test]
    public async Task SetThenGetReturnsPersistedValue()
    {
        await WithCacheAsync(
            async (cache, _) =>
            {
                const string key = "set-get";
                var payload = "cached-value"u8.ToArray();

                cache.Set(key, payload, new DistributedCacheEntryOptions());
                var result = cache.Get(key);

                await Assert.That(result).IsNotNull();
                await Assert.That(Encoding.UTF8.GetString(result ?? [])).IsEqualTo("cached-value");
            }
        );
    }

    [Test]
    public async Task GetReturnsNullWhenAbsoluteExpirationHasPassed()
    {
        await WithCacheAsync(
            async (cache, clock) =>
            {
                const string key = "absolute-expiration";
                cache.Set(
                    key,
                    "value"u8.ToArray(),
                    new DistributedCacheEntryOptions
                    {
                        AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(5),
                    }
                );

                clock.Advance(TimeSpan.FromSeconds(6));

                var result = cache.Get(key);

                await Assert.That(result).IsNull();
            }
        );
    }

    [Test]
    public async Task GetAsyncReturnsNullWhenAbsoluteExpirationHasPassed()
    {
        await WithCacheAsync(
            async (cache, clock) =>
            {
                const string key = "absolute-expiration-async";
                await cache.SetAsync(
                    key,
                    "value"u8.ToArray(),
                    new DistributedCacheEntryOptions
                    {
                        AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(5),
                    }
                );

                clock.Advance(TimeSpan.FromSeconds(6));

                var result = await cache.GetAsync(key);

                await Assert.That(result).IsNull();
            }
        );
    }

    [Test]
    public async Task SlidingExpirationExtendsOnAccess()
    {
        await WithCacheAsync(
            async (cache, clock) =>
            {
                const string key = "sliding";
                cache.Set(
                    key,
                    "value"u8.ToArray(),
                    new DistributedCacheEntryOptions
                    {
                        SlidingExpiration = TimeSpan.FromSeconds(10),
                    }
                );

                clock.Advance(TimeSpan.FromSeconds(5));
                var firstRead = cache.Get(key);
                await Assert.That(firstRead).IsNotNull();

                clock.Advance(TimeSpan.FromSeconds(7));
                var secondRead = cache.Get(key);
                await Assert.That(secondRead).IsNotNull();

                clock.Advance(TimeSpan.FromSeconds(11));
                await Assert.That(cache.Get(key)).IsNull();
            }
        );
    }

    [Test]
    public async Task RefreshExtendsSlidingExpirationWithoutFetchingValue()
    {
        await WithCacheAsync(
            async (cache, clock) =>
            {
                const string key = "refresh";
                cache.Set(
                    key,
                    "value"u8.ToArray(),
                    new DistributedCacheEntryOptions
                    {
                        SlidingExpiration = TimeSpan.FromSeconds(10),
                    }
                );

                clock.Advance(TimeSpan.FromSeconds(9));
                cache.Refresh(key);

                clock.Advance(TimeSpan.FromSeconds(9));
                var stillValid = cache.Get(key);
                await Assert.That(stillValid).IsNotNull();

                clock.Advance(TimeSpan.FromSeconds(11));
                await Assert.That(cache.Get(key)).IsNull();
            }
        );
    }

    [Test]
    public async Task RefreshAsyncExtendsSlidingExpirationWithoutFetchingValue()
    {
        await WithCacheAsync(
            async (cache, clock) =>
            {
                const string key = "refresh-async";
                await cache.SetAsync(
                    key,
                    "value"u8.ToArray(),
                    new DistributedCacheEntryOptions
                    {
                        SlidingExpiration = TimeSpan.FromSeconds(10),
                    }
                );

                clock.Advance(TimeSpan.FromSeconds(9));
                await cache.RefreshAsync(key);

                clock.Advance(TimeSpan.FromSeconds(9));
                var stillValid = await cache.GetAsync(key);
                await Assert.That(stillValid).IsNotNull();

                clock.Advance(TimeSpan.FromSeconds(11));
                var expired = await cache.GetAsync(key);
                await Assert.That(expired).IsNull();
            }
        );
    }

    [Test]
    public async Task RemoveDeletesEntry()
    {
        await WithCacheAsync(
            async (cache, _) =>
            {
                const string key = "remove";
                cache.Set(key, "value"u8.ToArray(), new DistributedCacheEntryOptions());

                cache.Remove(key);

                await Assert.That(cache.Get(key)).IsNull();
            }
        );
    }

    [Test]
    public async Task RemoveAsyncDeletesEntry()
    {
        await WithCacheAsync(
            async (cache, _) =>
            {
                const string key = "remove-async";
                await cache.SetAsync(key, "value"u8.ToArray(), new DistributedCacheEntryOptions());

                await cache.RemoveAsync(key);

                var result = await cache.GetAsync(key);
                await Assert.That(result).IsNull();
            }
        );
    }

    [Test]
    public async Task SetAsyncThenGetAsyncRoundTrips()
    {
        await WithCacheAsync(
            async (cache, _) =>
            {
                const string key = "async";
                var payload = "async-value"u8.ToArray();

                await cache.SetAsync(key, payload, new DistributedCacheEntryOptions());
                var result = await cache.GetAsync(key);

                await Assert.That(result).IsNotNull();
                await Assert.That(Encoding.UTF8.GetString(result ?? [])).IsEqualTo("async-value");
            }
        );
    }

    private static async Task WithCacheAsync(Func<FileCache, FakeClock, Task> test)
    {
        var clock = new FakeClock();
        var path = CreateUniquePath();

        var cache = CreateCache(clock, path);
        try
        {
            await test(cache, clock);
        }
        finally
        {
            CleanupArtifacts(path);
        }
    }

    private static FileCache CreateCache(FakeClock clock, string path) =>
        new(new FileCache.FileCacheOptions { FilePath = path, UtcNow = clock.UtcNow });

    private static string CreateUniquePath()
    {
        var directory = Path.Combine(Path.GetTempPath(), "filecache-tests");
        Directory.CreateDirectory(directory);
        return Path.Combine(directory, $"{Guid.NewGuid():N}.db");
    }

    private static void CleanupArtifacts(string databasePath)
    {
        var directory = Path.GetDirectoryName(databasePath) ?? Directory.GetCurrentDirectory();
        var fileName = Path.GetFileName(databasePath);

        foreach (var path in Directory.EnumerateFiles(directory, $"{fileName}*"))
        {
            TryDelete(path);
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Ignore cleanup failures in tests
        }
    }

    private sealed class FakeClock
    {
        private DateTimeOffset _utcNow = DateTimeOffset.FromUnixTimeSeconds(1_000_000);

        public DateTimeOffset UtcNow() => _utcNow;

        public void Advance(TimeSpan duration) => _utcNow = _utcNow.Add(duration);
    }
}
