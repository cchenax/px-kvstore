px-kvstore
==========

An in-memory, sharded, LRU key-value store with TTL, HTTP API, basic metrics, and optional disk snapshots.

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
- `PXKV_SNAPSHOT_FILE` – path to JSON snapshot file (e.g. `./data/pxkv.json`)
- `PXKV_SNAPSHOT_INTERVAL` – snapshot interval in seconds; if `> 0`, enables periodic snapshots

On startup, if `PXKV_SNAPSHOT_FILE` exists, the store will attempt to load it and restore previous keys (respecting remaining TTL).

When snapshots are enabled, a background thread periodically writes the in-memory state to disk in a JSON format.

---

**Author**: Tony Chen
