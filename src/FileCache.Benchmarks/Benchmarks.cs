using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.Caching.Distributed;
using Cache = FileCache.FileCache;
using CacheOptions = FileCache.FileCache.FileCacheOptions;

namespace FileCache.Benchmarks;

[MemoryDiagnoser]
[ThreadingDiagnoser]
public class Benchmarks : IDisposable
{
    private Cache _cache = null!;
    private byte[] _payload = null!;
    private DistributedCacheEntryOptions _absoluteExpiryOptions = null!;
    private DistributedCacheEntryOptions _slidingExpiryOptions = null!;
    private string _dbPath = null!;
    private long _keyCounter;
    private string _slidingKey = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"filecache-bench-{Guid.NewGuid():N}.db");
        _cache = new Cache(new CacheOptions { FilePath = _dbPath });

        _payload = new byte[4 * 1024];
        Random.Shared.NextBytes(_payload);

        _absoluteExpiryOptions = new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(10),
        };

        _slidingExpiryOptions = new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(5),
        };

        _slidingKey = "sliding-hit";
        _cache.Set(_slidingKey, _payload, _slidingExpiryOptions);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (File.Exists(_dbPath))
        {
            try
            {
                File.Delete(_dbPath);
            }
            catch
            {
                // Ignore cleanup failures: benchmark results are still valid.
            }
        }
    }

    private string NextKey() => Interlocked.Increment(ref _keyCounter).ToString();

    [Benchmark]
    public void Set_NewKey() => _cache.Set($"set-{NextKey()}", _payload, _absoluteExpiryOptions);

    [Benchmark]
    public byte[]? Get_CacheHit() => _cache.Get(_slidingKey);

    [Benchmark]
    public byte[]? Get_CacheMiss() => _cache.Get($"miss-{NextKey()}");

    [Benchmark]
    public void Refresh_SlidingKey() => _cache.Refresh(_slidingKey);

    [Benchmark]
    public Task SetAsync_NewKey() =>
        _cache.SetAsync(
            $"set-async-{NextKey()}",
            _payload,
            _slidingExpiryOptions,
            CancellationToken.None
        );

    [Benchmark]
    public Task<byte[]?> GetAsync_CacheHit() =>
        _cache.GetAsync(_slidingKey, CancellationToken.None);

    public void Dispose()
    {
        GlobalCleanup();
    }
}

public static class Program
{
    public static void Main(string[] args)
    {
        _ = BenchmarkRunner.Run<Benchmarks>();
    }
}
