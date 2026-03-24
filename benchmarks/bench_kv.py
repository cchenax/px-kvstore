import time
import statistics
import concurrent.futures
from pxkv.core.sharded import ShardedKeyValueStore

def run_bench(shards=4, clients=4, total_ops=10000, op_type="put"):
    store = ShardedKeyValueStore(shards=shards, per_shard_max=100000)
    
    latencies = []
    
    def worker(client_id, num_ops):
        local_latencies = []
        for i in range(num_ops):
            key = f"key_{client_id}_{i}"
            val = f"val_{i}"
            start = time.time()
            if op_type == "put":
                store.create(key, val)
            else:
                try:
                    store.read(key)
                except KeyError:
                    pass
            local_latencies.append((time.time() - start) * 1000)
        return local_latencies

    ops_per_client = total_ops // clients
    start_total = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as executor:
        futures = [executor.submit(worker, i, ops_per_client) for i in range(clients)]
        for f in concurrent.futures.as_completed(futures):
            latencies.extend(f.result())
    
    total_time = time.time() - start_total
    qps = total_ops / total_time
    
    p50 = statistics.median(latencies)
    latencies.sort()
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    
    return {
        "shards": shards,
        "clients": clients,
        "qps": qps,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99
    }

if __name__ == "__main__":
    print(f"{'Shards':<8} {'Clients':<8} {'QPS':<10} {'p50(ms)':<10} {'p95(ms)':<10}")
    print("-" * 50)
    for s in [1, 4, 16]:
        for c in [1, 4, 16]:
            res = run_bench(shards=s, clients=c, total_ops=20000, op_type="put")
            print(f"{res['shards']:<8} {res['clients']:<8} {res['qps']:<10.1f} {res['p50_ms']:<10.3f} {res['p95_ms']:<10.3f}")
