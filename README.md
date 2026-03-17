px-kvstore
==========

An in-memory, sharded, LRU key-value store with TTL, HTTP API, basic metrics, and optional disk snapshots.

## What’s interesting

- **Sharded + per-shard LRU**: keys are routed to shards by \(crc32(key) \bmod N\). Each shard maintains its own LRU list and lock, so hot keys don’t contend with cold shards.
- **TTL that survives restarts (remaining TTL snapshots)**: snapshots store *remaining TTL* (not absolute timestamps). On restore, keys regain their remaining lifetime from the moment you restart.
- **Atomic snapshot writes**: snapshots are written to `*.tmp` and swapped in with `os.replace`, reducing the chance of partial/corrupt files on crash.
- **Simple “cursor scan” API**: lexicographic scan with `prefix`, `start_after`, and `limit` makes it easy to paginate keys without dumping the whole keyspace.
- **AI prompt/response cache (deterministic keys)**: compute a content-addressed cache key from `(prompt, model, params)` using canonical JSON + SHA-256, so semantically identical requests map to the same key.
- **AI cache observability**: `/admin/metrics` exposes `ai_cache` counters (lookups/hits/misses/stores) so you can track hit rate and cost savings.
- **AI cache + TTL + snapshots**: cached responses can have TTL and (optionally) survive restarts via “remaining TTL snapshots”, enabling reproducible experiments and warm-start behavior.

## Semantics (important details)

- **Value encoding**:
  - `PUT /kv/<key>` tries to parse the request body as JSON; if JSON parsing fails, it stores the raw bytes.
  - JSON responses will decode bytes as UTF-8 with replacement on invalid sequences.
- **TTL behavior**:
  - TTL is enforced lazily (checked/purged on read/write/keys/snapshot operations), not by a global timer wheel.
  - Snapshot stores remaining TTL per key; restore recomputes absolute expiry as `now + ttl_remaining`.
- **Atomicity & concurrency**:
  - Operations on the **same key** are serialized by that key’s shard lock.
  - `mset/mget` are shard-grouped (each shard is updated under its own lock); cross-shard atomicity is **not** provided.

## Quick start

- **Install (editable)**:

```bash
pip install -e .
```

- **Run server**:

```bash
python server.py
```

Server listens on `http://0.0.0.0:8000` by default.

## Core APIs

- **Single write with TTL (60s)**:

```bash
curl -X PUT "http://localhost:8000/kv/foo?ttl=60" -d "bar"
```

- **Read**:

```bash
curl http://localhost:8000/kv/foo
```

Response:

```json
{"key":"foo","value":"bar"}
```

## AI cache APIs (prompt/response cache)

These endpoints implement a deterministic, content-addressed cache key computed from `(prompt, model, params)`.

- **Why this is useful for AI systems**:
  - **Cost control**: cache repeated calls (same prompt/model/params) to reduce LLM tokens and latency.
  - **Reproducibility**: a canonical JSON representation makes cache keys stable across languages/clients.
  - **Operational visibility**: hit/miss counters help you measure ROI and tune TTL/eviction.

- **Lookup (hit/miss + deterministic key)**:

```bash
curl -X POST http://localhost:8000/ai/cache/lookup \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "Explain CRC32 sharding in one sentence.",
    "model": "gpt-4.1-mini",
    "params": {"temperature": 0.2, "max_tokens": 64}
  }'
```

- **Store a response** (upsert with optional TTL):

```bash
curl -X POST http://localhost:8000/ai/cache \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "Explain CRC32 sharding in one sentence.",
    "model": "gpt-4.1-mini",
    "params": {"temperature": 0.2, "max_tokens": 64},
    "value": {"text": "CRC32 routes each key to a shard by taking a fast hash modulo the shard count."},
    "ttl": 3600
  }'
```

- **Get by cache key**:

```bash
curl http://localhost:8000/ai/cache/<key-from-lookup-or-store>
```

- **Batch write**:

```bash
curl -X POST http://localhost:8000/kv/batch \
     -H "Content-Type: application/json" \
     -d '{"items": {"a":1,"b":2,"c":3}, "ttl":120}'
```

- **Batch read**:

```bash
curl "http://localhost:8000/kv/batch?keys=a,b,x"
```

Example:

```json
{"a":1,"b":2}
```

## Advanced APIs

- **Scan keys**:

```bash
curl "http://localhost:8000/kv/scan?prefix=f&limit=50"
```

Query params:

- `prefix` (optional): only return keys starting with this prefix.
- `limit` (optional, default 100): max number of keys.
- `start_after` (optional): only return keys lexicographically greater than this key.

Response:

```json
{"keys":["foo","foo2"]}
```

- **Atomic increment**:

```bash
curl -X POST "http://localhost:8000/kv/incr/counter?delta=1&ttl=300"
```

If the key does not exist, it starts from `0`. The stored value becomes a floating-point number.

Response:

```json
{"key":"counter","value":1.0}
```

## Admin & observability

- **Health**:

```bash
curl http://localhost:8000/admin/health
```

- **Metrics**:

```bash
curl http://localhost:8000/admin/metrics
```

Returns a JSON object with uptime, request counts, and error counts.
Also includes:

- `latency_ms.by_route`: per-route latency histogram (ms)
- `ai_cache`: lookups/hits/misses/stores counters for AI cache endpoints

- **Manual snapshot** (if snapshotting is enabled, see below):

```bash
curl http://localhost:8000/admin/snapshot
```

## Configuration & persistence

Environment variables:

- `PXKV_HOST` – bind host (default `0.0.0.0`)
- `PXKV_PORT` – bind port (default `8000`)
- `PXKV_SHARD_COUNT` – number of in-memory shards (default `4`)
- `PXKV_PER_SHARD_MAX` – max entries per shard (approximate LRU cap, default `1000`)
- `PXKV_EVICTION_POLICY` – eviction policy: `lru` (default) or `lfu`
- `PXKV_SNAPSHOT_FILE` – path to JSON snapshot file (e.g. `./data/pxkv.json`)
- `PXKV_SNAPSHOT_INTERVAL` – snapshot interval in seconds; if `> 0`, enables periodic snapshots
- `PXKV_FAULT_LATENCY_MS` – fault injection: add fixed latency (ms) per request (default `0`)
- `PXKV_FAULT_LATENCY_JITTER_MS` – fault injection: add uniform random latency in `[0, jitter]` ms (default `0`)
- `PXKV_FAULT_SNAPSHOT_FAIL_P` – fault injection: probability of snapshot failure (0..1, default `0`)

On startup, if `PXKV_SNAPSHOT_FILE` exists, the store will attempt to load it and restore previous keys (respecting remaining TTL).

When snapshots are enabled, a background thread periodically writes the in-memory state to disk in a JSON format.

## Reproducible experiments

### Quick correctness smoke

```bash
python server.py &
sleep 0.2

# write JSON value with TTL
curl -sS -X PUT "http://localhost:8000/kv/x?ttl=2" -d '123'
curl -sS http://localhost:8000/kv/x && echo
sleep 2.2
curl -i http://localhost:8000/kv/x
```

### Snapshot / restore TTL survival

```bash
export PXKV_SNAPSHOT_FILE=./data/pxkv.json
export PXKV_SNAPSHOT_INTERVAL=1
python server.py &
sleep 0.2

# key should have ~5s remaining after restart
curl -sS -X PUT "http://localhost:8000/kv/ttl_demo?ttl=5" -d '"v"'
sleep 2
curl -sS http://localhost:8000/admin/snapshot && echo

pkill -f "python server.py" || true
python server.py &
sleep 0.2
curl -i http://localhost:8000/kv/ttl_demo
```

### Simple load test (no extra deps)

```bash
python - <<'PY'
import json, time, urllib.request
base="http://localhost:8000"
n=2000
t0=time.time()
for i in range(n):
    k=f"k{i}"
    req=urllib.request.Request(f"{base}/kv/{k}?ttl=60", data=json.dumps(i).encode(), method="PUT")
    urllib.request.urlopen(req).read()
dt=time.time()-t0
print(f"PUT {n} keys: {n/dt:.1f} req/s")
PY
```

## Roadmap ideas

- **Pluggable eviction policy (DONE)**: set `PXKV_EVICTION_POLICY=lru|lfu`.
- **Better scan scalability (DONE)**: scan uses a per-shard sorted string-key index + k-way merge (avoids full key collection + global sort per scan).
- **Request tracing (DONE)**: every response includes `X-Request-Id`; `/admin/metrics` includes per-route latency histogram.
- **Fault-injection mode (DONE)**: enable artificial request latency and probabilistic snapshot failures via env vars above.

---

**Author**: Chao Chen
