import time
import json
import statistics
import concurrent.futures
from pxkv.cache.ai import compute_ai_cache_key
from pxkv.core.sharded import ShardedKeyValueStore

def run_ai_bench(shards=4, total_ops=10000, hit_ratio=0.5):
    store = ShardedKeyValueStore(shards=shards, per_shard_max=100000)
    
    # Pre-populate some keys for hits
    num_hits = int(total_ops * hit_ratio)
    for i in range(num_hits):
        k, _ = compute_ai_cache_key(f"prompt_{i}", "gpt-4", {"temp": 0.7})
        store.create(f"ai:cache:{k}", f"cached_response_{i}")
        
    latencies = []
    
    start_total = time.time()
    for i in range(total_ops):
        # prompt_i for hits, prompt_i+total_ops for misses
        p_idx = i if i < num_hits else i + total_ops
        prompt = f"prompt_{p_idx}"
        
        start = time.time()
        k, canon = compute_ai_cache_key(prompt, "gpt-4", {"temp": 0.7})
        storage_key = f"ai:cache:{k}"
        try:
            store.read(storage_key)
        except KeyError:
            store.create(storage_key, f"new_response_{i}")
        latencies.append((time.time() - start) * 1000)
        
    total_time = time.time() - start_total
    qps = total_ops / total_time
    
    p50 = statistics.median(latencies)
    latencies.sort()
    p95 = latencies[int(len(latencies) * 0.95)]
    
    return {
        "qps": qps,
        "p50_ms": p50,
        "p95_ms": p95,
        "hit_ratio": hit_ratio
    }

if __name__ == "__main__":
    print(f"{'Hit Ratio':<12} {'QPS':<10} {'p50(ms)':<10} {'p95(ms)':<10}")
    print("-" * 50)
    for hr in [0.0, 0.5, 0.9, 1.0]:
        res = run_ai_bench(total_ops=10000, hit_ratio=hr)
        print(f"{res['hit_ratio']:<12.1f} {res['qps']:<10.1f} {res['p50_ms']:<10.3f} {res['p95_ms']:<10.3f}")
