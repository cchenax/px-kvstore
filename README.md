# PX-KVStore

**A lightweight, high-performance KV engine for deterministic LLM caching and fast local inference experimentation.**

PX-KVStore is a sharded, in-memory key-value store designed for low-latency workloads, with a specific focus on AI engineering and reproducibility. It bridges the gap between simple "scripts" and heavy production databases by providing a layered, observable, and extensible architecture.

---

## **Architecture**

### **Request Flow**
1. **API Layer**: Requests enter via a standard HTTP interface. Every request is assigned a unique `X-Request-Id` for tracing.
2. **Routing Layer**: Keys are routed to specific shards using **Consistent Hashing with Virtual Nodes** (default 100 vnodes/shard). This ensures uniform distribution and minimizes re-mapping when shard counts change.
3. **Core Engine**: Each shard is a self-contained unit with its own:
   - **Storage**: Hash map for O(1) lookups.
   - **Eviction Policy**: Pluggable LRU or LFU.
   - **Index**: A sorted string-key index for efficient lexicographic scans (K-way merge across shards).
   - **Locking**: Fine-grained per-shard locks to maximize concurrency.

### **Shard Layout**
PX-KVStore uses a consistent hashing ring. This avoids the "thundering herd" re-distribution problem common with simple `hash % N` sharding.
- **Virtual Nodes**: Map each physical shard to multiple points on the ring to prevent "hot shards".
- **Lock Model**: No global lock. Shards operate independently, allowing throughput to scale linearly with the number of CPU cores.

### **Persistence Lifecycle**
PX-KVStore provides two durability tiers:
1. **WAL (Write-Ahead Log)**: Every write is appended to a synchronous log file. On crash, the engine replays the WAL to recover the exact state.
2. **Atomic Snapshots**: Periodic full-state dumps. Snapshots use `os.replace` for atomic swaps, ensuring the database never starts from a corrupted file.
3. **TTL Survival**: Unlike many KV stores, PX-KVStore snapshots store *remaining TTL*. Upon restore, absolute expiry is re-calculated as `now + remaining_ttl`.

### **AI Cache-Key Lifecycle**
1. **Normalization**: Prompts are normalized (lowercase, whitespace collapse) to improve hit rates.
2. **Canonicalization**: LLM parameters are sorted and converted to a canonical JSON string.
3. **Hashing**: A SHA-256 hash of `(normalized_prompt, model, params, version)` forms the deterministic cache key.
4. **Compression**: Responses can be transparently compressed (Gzip + Base64) to save memory for large LLM outputs.

---

## **Tradeoffs**

| Decision | Why? | Tradeoff |
| :--- | :--- | :--- |
| **Lazy TTL** | Simplicity and performance. Avoids the overhead of a global timer wheel and constant wakeups. | Expired memory is only reclaimed on access or by the background expirer thread. |
| **Snapshot + WAL** | Provides both fast recovery (Snapshot) and zero-data-loss (WAL). | Incremental disk I/O for every write; larger snapshots for large datasets. |
| **No Cross-Shard Atomicity** | Maximum throughput. Global transactions require expensive coordination (2PC/Paxos). | `mset/mget` are atomic *per shard*, but not across the entire store. |
| **In-Memory First** | Designed for caching and low-latency inference, where speed is the primary constraint. | Dataset size is limited by available RAM. |

---

## **Benchmarks**

*Environment: macOS, Python 3.12, Shards: 16*

### **Basic KV Performance**
| Shards | Clients | QPS (PUT) | p50 Latency | p95 Latency |
| :--- | :--- | :--- | :--- | :--- |
| 1 | 1 | 12.3k | 0.081ms | 0.151ms |
| 4 | 4 | 42.7k | 0.028ms | 0.381ms |
| 16 | 16 | **138.1k** | **0.006ms** | 0.012ms |

### **AI Cache Efficiency**
| Scenario | Hit Ratio | QPS | p50 Latency |
| :--- | :--- | :--- | :--- |
| **Cold Cache** | 0% | 40.4k | 0.024ms |
| **Mixed Workload** | 50% | 42.1k | 0.023ms |
| **Warm Cache** | 100% | 43.4k | 0.023ms |

---

## **Quick Start**

### **Installation**
```bash
pip install -e .
```

### **Run Server**
```bash
# Enable WAL and Snapshots
export PXKV_WAL_FILE=./data/wal.log
export PXKV_SNAPSHOT_FILE=./data/snap.json
export PXKV_SNAPSHOT_INTERVAL=60
python server.py
```

---

## **Roadmap**

- [x] **WAL + Crash Recovery**: Persistent transaction logging.
- [x] **Consistent Hashing**: Scalable sharding with virtual nodes.
- [x] **Background Expiration**: Proactive memory reclamation.
- [ ] **Redis Protocol Compatibility**: Use `redis-py` or `redis-cli` directly.
- [ ] **Asynchronous Replication**: Leader-Follower setup for high availability.
- [ ] **Pluggable Storage Backends**: Support for disk-backed shards (e.g., RocksDB).
- [ ] **Semantic Cache Key Hooks**: Custom prompt normalization and versioning logic.

---

**Author**: Chao Chen
