using System.Collections.Concurrent;
using System.Text;
using Microsoft.Data.Sqlite;
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

    [Test]
    public async Task ConcurrentSimulationMaintainsLatestWrites()
    {
        await WithCacheAsync(
            async (cache, _) =>
            {
                var options = new DistributedCacheEntryOptions
                {
                    AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5),
                    SlidingExpiration = TimeSpan.FromMinutes(2),
                };

                const int workerCount = 4;
                const int iterations = 120;

                var expectedValues = new ConcurrentDictionary<string, string>();

                var workers = Enumerable.Range(0, workerCount).Select(
                    worker =>
                        Task.Run(
                            async () =>
                            {
                                for (var i = 0; i < iterations; i++)
                                {
                                    var key = $"shared-{i % 16}";
                                    var valueText = $"worker:{worker}-iteration:{i}";
                                    var payload = Encoding.UTF8.GetBytes(valueText);

                                    await cache.SetAsync(key, payload, options);
                                    expectedValues.AddOrUpdate(key, valueText, (_, _) => valueText);

                                    if (i % 3 == 0)
                                    {
                                        var readAsync = await cache.GetAsync(key);
                                        await Assert.That(readAsync).IsNotNull();
                                    }

                                    if (i % 5 == 0)
                                    {
                                        await cache.RefreshAsync(key);
                                    }

                                    if (i % 7 == 0)
                                    {
                                        var readSync = cache.Get(key);
                                        await Assert.That(readSync).IsNotNull();
                                    }
                                }
                            }
                        )
                );

                await Task.WhenAll(workers);

                foreach (var entry in expectedValues)
                {
                    var stored = await cache.GetAsync(entry.Key);
                    await Assert.That(stored).IsNotNull();
                    await Assert
                        .That(Encoding.UTF8.GetString(stored ?? []))
                        .IsEqualTo(entry.Value);
                }
            }
        );
    }

    [Test]
    public async Task LongRunningSimulationExpiresAndRefreshesCorrectly()
    {
        await WithCacheAsync(
            async (cache, clock) =>
            {
                var start = clock.UtcNow();

                await cache.SetAsync(
                    "sliding-keep",
                    "keep"u8.ToArray(),
                    new DistributedCacheEntryOptions
                    {
                        SlidingExpiration = TimeSpan.FromMinutes(5),
                    }
                );

                await cache.SetAsync(
                    "sliding-drop",
                    "drop"u8.ToArray(),
                    new DistributedCacheEntryOptions
                    {
                        SlidingExpiration = TimeSpan.FromMinutes(5),
                    }
                );

                await Assert.That(cache.Get("sliding-drop")).IsNotNull();

                await cache.SetAsync(
                    "absolute",
                    "absolute"u8.ToArray(),
                    new DistributedCacheEntryOptions
                    {
                        AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(30),
                    }
                );

                for (var minute = 0; minute < 40; minute++)
                {
                    clock.Advance(TimeSpan.FromMinutes(1));

                    if (minute == 10)
                    {
                        clock.Advance(TimeSpan.FromMinutes(3));
                    }

                    var elapsed = clock.UtcNow() - start;

                    if (minute % 2 == 0)
                    {
                        var keepPayload = cache.Get("sliding-keep");
                        await Assert.That(keepPayload).IsNotNull();
                        cache.Refresh("sliding-keep");
                    }

                    if (minute % 3 == 0)
                    {
                        var absolutePayload = await cache.GetAsync("absolute");
                        if (elapsed < TimeSpan.FromMinutes(30))
                        {
                            await Assert.That(absolutePayload).IsNotNull();
                        }
                        else
                        {
                            await Assert.That(absolutePayload).IsNull();
                        }
                    }

                }

                await Assert.That(cache.Get("sliding-keep")).IsNotNull();
                cache.Refresh("sliding-keep");

                await Assert.That(cache.Get("sliding-drop")).IsNull();
                await Assert.That(await cache.GetAsync("absolute")).IsNull();
            }
        );
    }

    [Test]
    public async Task RecoversAfterTransientDatabaseLock()
    {
        await WithCacheAsync(
            async (cache, _, path) =>
            {
                var options = new DistributedCacheEntryOptions
                {
                    AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(10),
                };

                await cache.SetAsync("baseline", "baseline"u8.ToArray(), options);

                await using var blocker = new SqliteConnection($"Data Source={path}");
                await blocker.OpenAsync();

                await using (var begin = blocker.CreateCommand())
                {
                    begin.CommandText = "BEGIN EXCLUSIVE";
                    await begin.ExecuteNonQueryAsync();
                }

                SqliteException? captured = null;
                try
                {
                    await cache.SetAsync("locked", "value"u8.ToArray(), options);
                }
                catch (SqliteException ex)
                {
                    captured = ex;
                }

                await Assert.That(captured).IsNotNull();

                await using (var commit = blocker.CreateCommand())
                {
                    commit.CommandText = "COMMIT";
                    await commit.ExecuteNonQueryAsync();
                }

                await cache.SetAsync("locked", "value"u8.ToArray(), options);

                var baselinePayload = await cache.GetAsync("baseline");
                var recoveredPayload = await cache.GetAsync("locked");

                await Assert.That(baselinePayload).IsNotNull();
                await Assert
                    .That(Encoding.UTF8.GetString(baselinePayload ?? []))
                    .IsEqualTo("baseline");

                await Assert.That(recoveredPayload).IsNotNull();
                await Assert
                    .That(Encoding.UTF8.GetString(recoveredPayload ?? []))
                    .IsEqualTo("value");
            }
        );
    }

    private static Task WithCacheAsync(Func<FileCache, FakeClock, Task> test) =>
        WithCacheAsync((cache, clock, _) => test(cache, clock));

    private static async Task WithCacheAsync(Func<FileCache, FakeClock, string, Task> test)
    {
        var clock = new FakeClock();
        var path = CreateUniquePath();

        var cache = CreateCache(clock, path);
        try
        {
            await test(cache, clock, path);
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
