# PX-KVStore

**A lightweight, high-performance KV engine for deterministic LLM caching and fast local inference experimentation.**

PX-KVStore is a sharded, in-memory key-value store designed for low-latency workloads, with a specific focus on AI engineering and reproducibility. It bridges the gap between simple "scripts" and heavy production databases by providing a layered, observable, and extensible architecture.

### **Replication Lifecycle**
PX-KVStore supports asynchronous Leader-Follower replication:
1. **Initial Full Sync**: Follower pulls the full state (Snapshot) from the Leader on startup.
2. **Incremental Sync**: Leader pushes real-time changes to all registered Followers via a background queue.
3. **Fault Tolerance**: Replication is non-blocking on the Leader, ensuring high write availability.

### **Observability**
- **JSON API**: Detailed metrics at `/admin/metrics`.
- **Prometheus**: Native exposition format at `/admin/metrics?format=prometheus`.
- **Latency Tracking**: Per-route latency histograms and counters.

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

### **Redis Protocol Support**
You can connect to PX-KVStore using any standard Redis client:
```bash
redis-cli -p 6379 SET mykey "Hello Redis"
redis-cli -p 6379 GET mykey
```
Supported commands: `SET` (with EX/PX), `GET`, `DEL`, `EXISTS`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `EXPIRE`, `TTL`, `PTTL`, `PERSIST`, `PING`, `INFO`, `DBSIZE`, `FLUSHALL`, `XADD`, `XRANGE`, `XREAD`, `XGROUP CREATE`, `XREADGROUP`, `XACK`, `XPENDING`.

### **Top-K Heavy Hitters**
Enable bounded-memory hot-key tracking for high-cardinality read workloads:
```bash
export PXKV_HEAVY_HITTERS_ENABLED=true
export PXKV_HEAVY_HITTERS_K=20
export PXKV_HEAVY_HITTERS_CMS_WIDTH=4096
export PXKV_HEAVY_HITTERS_CMS_DEPTH=4
export PXKV_HEAVY_HITTERS_THRESHOLD_COUNT=1000
```

The tracker records successful `GET`/`MGET` reads into a Count-Min Sketch and keeps only the top-K candidate keys in memory. Inspect it with `GET /admin/heavy-hitters?limit=20`, query a single key with `GET /admin/heavy-hitters?key=mykey`, reset it with `POST /admin/heavy-hitters/reset`, or scrape the `pxkv_heavy_hitters_*` Prometheus metrics.

### **Per-Namespace Hot Key Reports**
When namespaces are enabled, hot-key reports can be scoped per tenant:
```bash
export PXKV_NAMESPACE_ENABLED=true
export PXKV_NAMESPACE_CONFIGS='{
  "tenant-a": {"hot_keys": {"top_k": 5, "threshold_qps": 100}},
  "tenant-b": {"hot_keys": {"top_k": 20, "threshold_qps": 25}}
}'
```

Use `GET /admin/hotkeys?namespace=tenant-a` for one namespace or `GET /admin/hotkeys/namespaces` for all configured namespaces. Reports strip the internal namespace prefix from keys and expose `pxkv_namespace_hot_keys_*` Prometheus metrics.

### **Query Plan Cache**
Repeated Redis commands and scan requests can reuse parsed plans:
```bash
export PXKV_QUERY_PLAN_CACHE_ENABLED=true
export PXKV_QUERY_PLAN_CACHE_MAX_ENTRIES=4096
```

The cache is bounded and stores parsed command/scan metadata, not values or query results. Inspect it through `/admin/metrics`, scrape `pxkv_query_plan_cache_*` Prometheus metrics, or clear it with `POST /admin/query-plan-cache/reset`.

---

## **Roadmap**

- [x] **WAL + Crash Recovery**: Persistent transaction logging.
- [x] **Consistent Hashing**: Scalable sharding with virtual nodes.
- [x] **Background Expiration**: Proactive memory reclamation.
- [x] **Redis Protocol Compatibility**: Use `redis-py` or `redis-cli` directly on port 6379.
- [x] **Asynchronous Replication**: Leader-Follower setup with full sync and incremental sync.
- [x] **Prometheus Metrics**: Export metrics in Prometheus format for integration with Grafana.
- [x] **Config hot reload**: Dynamic configuration updates.
- [x] **Replica Ack & Lag Metrics**: Track follower replication lag and expose per-follower ack position.
- [x] **Read-Only Follower Mode**: Serve safe reads from followers with staleness visibility.
- [x] **Snapshot Streaming Compression**: Reduce full-sync bandwidth and recovery time for large datasets.
- [x] **Memory Tiering Hooks**: Add extension points for spill-to-disk or external object storage.
- [x] **Multi-Key Routing Helpers**: Co-locate related keys via hash-tag strategy for better cross-key operations.
- [x] **Auth & ACL**: Add optional token/password auth and command-level access control.
- [x] **Follower Read Routing**: Optionally route read traffic to followers with max-staleness guardrails.
- [x] **Replication Backpressure**: Bound replication queue growth and shed load with clear metrics.
- [x] **WAL Compaction & Rotation**: Automatic WAL rotation with safe truncation after snapshot.
- [x] **Tiering Backends (S3/HTTP)**: Pluggable external object-store backends with async prefetch.
- [x] **Keyspace Notifications**: Publish key events (set/del/expire) over HTTP SSE or Redis Pub/Sub.
- [x] **Rate Limiting**: Per-route token bucket limits with admin-configurable policies.
- [x] **TLS / HTTPS**: Optional TLS listener for HTTP and Redis endpoints.
- [x] **Request Tracing**: Add OpenTelemetry trace spans for API and replication paths.
- [x] **Redis TTL Parity**: Support TTL/PTTL/PERSIST for better Redis compatibility.
- [x] **SCAN Cursor**: True cursor-based scan for large keyspaces without `start_after` hacks.
- [x] **ETag / Conditional GET**: Return ETag for GET /kv and honor If-None-Match to save bandwidth.
- [x] **Read Repair / Anti-Entropy**: Background verification and repair for follower divergence.
- [x] **Multi-Region / Cross-Cluster Replication**: Async replication between clusters with configurable conflict resolution.
- [x] **Cluster Autoscaling Hooks**: Integrate with autoscalers to expand or shrink shard count safely.
- [x] **Point-in-Time Recovery (PITR)**: Restore data to arbitrary LSN or timestamp using WAL + snapshot archives.
- [x] **JSON Patch / Partial Updates**: Support atomic JSON patch operations instead of full-value PUTs.
- [x] **Compression for On-Disk Data**: Gzip compression for WAL and snapshot files.
- [x] **Gossip Membership / Discovery**: Auto-discover peers for dynamic follower lists without static config.
- [x] **Disk Usage Throttling**: Throttle writes when disk usage exceeds configurable thresholds.
- [x] **Multi-Tenancy (Namespaces)**: Isolate keyspaces into distinct namespaces with separate auth/rate limits.
- [x] **Hot Key Detection**: Identify and flag frequently accessed keys for optimization.
- [x] **Hot Key Mitigation**: Automatic per-key caching, request coalescing, or replication of detected hot keys.
- [x] **Adaptive TTL**: Tune per-key TTL based on access frequency and recency to maximize hit ratio.
- [x] **Cold Key Eviction Hints**: Use access-frequency signal to bias LRU/LFU eviction toward truly cold keys.
- [x] **Top-K Heavy Hitters via Count-Min Sketch**: Bounded-memory approximate hot-key tracking for high-cardinality workloads.
- [x] **Per-Namespace Hot Key Reports**: Scope hot-key detection by namespace with separate thresholds and top-K lists.
- [x] **Query Plan Cache**: Cache parsed Redis command pipelines and scan plans for repeated workloads.
- [ ] **Vector Indexing**: Native support for vector similarity search (HNSW) for embedding workloads.
- [ ] **Snapshot Diff / Incremental Backup**: Ship only changed pages between snapshots to cut backup cost.
- [x] **Redis Streams Compatibility**: Support XADD/XREAD/XGROUP-style event streams for log-like workloads.
- [ ] **Lua / Server-Side Scripting**: Add atomic script execution for multi-key workflows.
- [ ] **Online Backup Restore Preview**: Validate backup archives and estimate restore impact before applying.
- [ ] **Shard Rebalancing Progress API**: Expose live migration progress, throttling, and cancellation controls.
- [ ] **Per-Namespace Quotas**: Enforce tenant-level memory, key-count, and request-budget limits.
- [ ] **Eviction Simulation Mode**: Preview which keys would be evicted under policy changes before enabling them.
- [ ] **Read-Through Write-Behind Cache Adapters**: Integrate external databases with configurable consistency policies.
- [ ] **Admin Audit Log**: Persist config changes, auth events, and privileged operations for compliance.

---

**Author**: Chao Chen
