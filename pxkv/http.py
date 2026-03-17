#!/usr/bin/env python
# -*- coding: utf-8 -*-

import http.server as BaseHTTPServer
import json
import logging
import os
import signal
import sys
import time
import urllib.parse as urlparse
import uuid
import random
from typing import Any, Dict, Tuple

from .store import ShardedKeyValueStore
from .persistence import SnapshotManager, load_snapshot
from .ai import compute_ai_cache_key


HOST = os.getenv("PXKV_HOST", "0.0.0.0")
PORT = int(os.getenv("PXKV_PORT", "8000"))
SHARDS = int(os.getenv("PXKV_SHARD_COUNT", os.getenv("SHARD_COUNT", "4")))
PER_SHARD_MAX = int(os.getenv("PXKV_PER_SHARD_MAX", "1000"))
EVICTION_POLICY = os.getenv("PXKV_EVICTION_POLICY", "lru")

FAULT_LATENCY_MS = float(os.getenv("PXKV_FAULT_LATENCY_MS", "0") or "0")
FAULT_LATENCY_JITTER_MS = float(os.getenv("PXKV_FAULT_LATENCY_JITTER_MS", "0") or "0")

STORE = ShardedKeyValueStore(shards=SHARDS, per_shard_max=PER_SHARD_MAX, eviction_policy=EVICTION_POLICY)

SNAPSHOT_FILE = os.getenv("PXKV_SNAPSHOT_FILE", "")
SNAPSHOT_INTERVAL = float(os.getenv("PXKV_SNAPSHOT_INTERVAL", "0"))

if SNAPSHOT_FILE:
    load_snapshot(STORE, SNAPSHOT_FILE)

_SNAPSHOT_MANAGER: SnapshotManager | None = None
if SNAPSHOT_FILE and SNAPSHOT_INTERVAL > 0:
    _SNAPSHOT_MANAGER = SnapshotManager(STORE, SNAPSHOT_FILE, SNAPSHOT_INTERVAL)
    _SNAPSHOT_MANAGER.start()

_LATENCY_BUCKETS_MS = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]

_METRICS: Dict[str, Any] = {
    "started_at": time.time(),
    "requests_total": 0,
    "requests_by_method": {},
    "errors_total": 0,
    "latency_ms": {
        "buckets_ms": _LATENCY_BUCKETS_MS,
        "by_route": {},
    },
    "ai_cache": {
        "lookups": 0,
        "hits": 0,
        "misses": 0,
        "stores": 0,
    },
}


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class KVHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    server_version = "PX-KVStore/2.0"

    def _fault_sleep(self) -> None:
        if FAULT_LATENCY_MS <= 0 and FAULT_LATENCY_JITTER_MS <= 0:
            return
        base = max(0.0, FAULT_LATENCY_MS)
        jitter = max(0.0, FAULT_LATENCY_JITTER_MS)
        extra = random.random() * jitter if jitter > 0 else 0.0
        time.sleep((base + extra) / 1000.0)

    def _ensure_request_context(self) -> None:
        if not hasattr(self, "_request_id"):
            self._request_id = uuid.uuid4().hex
        if not hasattr(self, "_request_started_at"):
            self._request_started_at = time.time()

    def _parse(self) -> Tuple[list[str], Dict[str, list[str]]]:
        parsed = urlparse.urlparse(self.path)
        parts = parsed.path.strip("/").split("/")
        if parts == [""]:
            parts = []
        query = urlparse.parse_qs(parsed.query)
        return parts, query

    def _body(self) -> bytes:
        size = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(size) if size else b""

    def _send(self, code: int, body: Any = b"", mime: str = "text/plain; charset=utf-8") -> None:
        self._ensure_request_context()
        if not isinstance(body, (bytes, bytearray)):
            body = str(body).encode("utf-8")
        self.send_response(code)
        self.send_header("X-Request-Id", self._request_id)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, obj: Any) -> None:
        self._ensure_request_context()
        def _default(v: Any):
            if isinstance(v, (bytes, bytearray)):
                return v.decode("utf-8", errors="replace")
            raise TypeError

        payload = json.dumps(obj, default=_default, ensure_ascii=False)
        self._send(code, payload.encode("utf-8"), "application/json")

    def _observe_latency(self, route: str, elapsed_ms: float) -> None:
        if elapsed_ms < 0:
            elapsed_ms = 0.0
        bucket = "inf"
        for b in _LATENCY_BUCKETS_MS:
            if elapsed_ms <= b:
                bucket = str(b)
                break
        latency = _METRICS.setdefault("latency_ms", {"buckets_ms": _LATENCY_BUCKETS_MS, "by_route": {}})
        by_route = latency.setdefault("by_route", {})
        r = by_route.setdefault(
            route,
            {
                "count": 0,
                "sum_ms": 0.0,
                "buckets": {},
            },
        )
        r["count"] += 1
        r["sum_ms"] += float(elapsed_ms)
        buckets = r.setdefault("buckets", {})
        buckets[bucket] = buckets.get(bucket, 0) + 1

    def _inc_metrics(self, method: str, route: str = "", error: bool = False) -> None:
        _METRICS["requests_total"] += 1
        by_method = _METRICS.setdefault("requests_by_method", {})
        by_method[method] = by_method.get(method, 0) + 1
        if error:
            _METRICS["errors_total"] += 1
        if route:
            self._ensure_request_context()
            elapsed_ms = (time.time() - self._request_started_at) * 1000.0
            self._observe_latency(route, elapsed_ms)

    # Core KV APIs
    def do_GET(self) -> None:
        self._request_id = uuid.uuid4().hex
        self._request_started_at = time.time()
        self._fault_sleep()
        try:
            parts, query = self._parse()
            if not parts:
                self._json(200, {"status": "ok"})
                return

            if parts[0] == "admin":
                self._handle_admin_get(parts[1:], query)
                return

            if parts[0] == "ai":
                if len(parts) >= 2 and parts[1] == "cache":
                    # GET /ai/cache/<key>
                    if len(parts) != 3 or not parts[2]:
                        raise ValueError
                    cache_key = parts[2]
                    storage_key = f"ai:cache:{cache_key}"
                    value = STORE.read(storage_key)
                    self._json(200, {"key": cache_key, "value": value})
                    self._inc_metrics("GET", route="GET /ai/cache/:key")
                    return
                raise ValueError

            if parts[0] != "kv":
                raise ValueError

            if len(parts) >= 2 and parts[1] == "batch":
                if "keys" not in query:
                    self._send(400, "keys query param required")
                    self._inc_metrics("GET", route="GET /kv/batch", error=True)
                    return
                keys = query["keys"][0].split(",")
                self._json(200, STORE.mget(keys))
                self._inc_metrics("GET", route="GET /kv/batch")
                return

            if len(parts) >= 2 and parts[1] == "scan":
                prefix = None
                start_after = None
                limit = 100
                if "prefix" in query:
                    prefix = query["prefix"][0]
                if "start_after" in query:
                    start_after = query["start_after"][0]
                if "limit" in query:
                    try:
                        limit = int(query["limit"][0])
                    except ValueError:
                        self._send(400, "limit must be int")
                        self._inc_metrics("GET", route="GET /kv/scan", error=True)
                        return
                keys = STORE.scan(prefix=prefix, limit=limit, start_after=start_after)
                self._json(200, {"keys": keys})
                self._inc_metrics("GET", route="GET /kv/scan")
                return

            if len(parts) < 2:
                raise ValueError

            key = parts[1]
            value = STORE.read(key)
            self._json(200, {"key": key, "value": value})
            self._inc_metrics("GET", route="GET /kv/:key")
        except KeyError as e:
            self._send(404, str(e))
            # Best-effort: keep route labels stable for metrics even on miss.
            if self.path.startswith("/ai/cache/"):
                self._inc_metrics("GET", route="GET /ai/cache/:key", error=True)
            else:
                self._inc_metrics("GET", route="GET /kv/:key", error=True)
        except ValueError:
            self._send(404, "Not Found")
            self._inc_metrics("GET", route="GET (not_found)", error=True)

    def do_PUT(self) -> None:
        self._request_id = uuid.uuid4().hex
        self._request_started_at = time.time()
        self._fault_sleep()
        try:
            parts, query = self._parse()
            if len(parts) != 2 or parts[0] != "kv" or parts[1] == "":
                raise ValueError
            key = parts[1]
            ttl = float(query["ttl"][0]) if "ttl" in query else None

            raw = self._body()
            try:
                value = json.loads(raw or b"")
            except ValueError:
                value = raw

            if key in STORE.mget([key]):
                STORE.update(key, value, ttl)
                self._send(204)
            else:
                STORE.create(key, value, ttl)
                self._send(201)
            self._inc_metrics("PUT", route="PUT /kv/:key")
        except KeyError as e:
            self._send(409, str(e))
            self._inc_metrics("PUT", route="PUT /kv/:key", error=True)
        except ValueError:
            self._send(404, "Not Found")
            self._inc_metrics("PUT", route="PUT (not_found)", error=True)

    def do_DELETE(self) -> None:
        self._request_id = uuid.uuid4().hex
        self._request_started_at = time.time()
        self._fault_sleep()
        try:
            parts, _ = self._parse()
            if len(parts) != 2 or parts[0] != "kv" or parts[1] == "":
                raise ValueError
            STORE.delete(parts[1])
            self._send(204)
            self._inc_metrics("DELETE", route="DELETE /kv/:key")
        except KeyError as e:
            self._send(404, str(e))
            self._inc_metrics("DELETE", route="DELETE /kv/:key", error=True)
        except ValueError:
            self._send(404, "Not Found")
            self._inc_metrics("DELETE", route="DELETE (not_found)", error=True)

    def do_POST(self) -> None:
        self._request_id = uuid.uuid4().hex
        self._request_started_at = time.time()
        self._fault_sleep()
        try:
            parts, _ = self._parse()
            if len(parts) >= 1 and parts[0] == "ai":
                if parts == ["ai", "cache", "lookup"]:
                    payload = json.loads(self._body() or b"{}")
                    prompt = payload.get("prompt", "")
                    model = payload.get("model", "")
                    params = payload.get("params", {}) or {}
                    if not isinstance(prompt, str) or not isinstance(model, str) or not isinstance(params, dict):
                        self._send(400, "prompt/model must be string; params must be object")
                        self._inc_metrics("POST", route="POST /ai/cache/lookup", error=True)
                        return
                    key, canon = compute_ai_cache_key(prompt=prompt, model=model, params=params)
                    storage_key = f"ai:cache:{key}"
                    _METRICS["ai_cache"]["lookups"] += 1
                    try:
                        cached = STORE.read(storage_key)
                    except KeyError:
                        _METRICS["ai_cache"]["misses"] += 1
                        self._json(200, {"hit": False, "key": key, "canonical": canon})
                        self._inc_metrics("POST", route="POST /ai/cache/lookup")
                        return
                    _METRICS["ai_cache"]["hits"] += 1
                    self._json(200, {"hit": True, "key": key, "canonical": canon, "value": cached})
                    self._inc_metrics("POST", route="POST /ai/cache/lookup")
                    return

                if parts == ["ai", "cache"]:
                    # POST /ai/cache
                    # Body: {prompt, model, params, value, ttl?}
                    payload = json.loads(self._body() or b"{}")
                    prompt = payload.get("prompt", "")
                    model = payload.get("model", "")
                    params = payload.get("params", {}) or {}
                    value = payload.get("value")
                    ttl = payload.get("ttl")
                    if not isinstance(prompt, str) or not isinstance(model, str) or not isinstance(params, dict):
                        self._send(400, "prompt/model must be string; params must be object")
                        self._inc_metrics("POST", route="POST /ai/cache", error=True)
                        return
                    ttl_f = None
                    if ttl is not None:
                        try:
                            ttl_f = float(ttl)
                        except (TypeError, ValueError):
                            self._send(400, "ttl must be numeric")
                            self._inc_metrics("POST", route="POST /ai/cache", error=True)
                            return
                    key, canon = compute_ai_cache_key(prompt=prompt, model=model, params=params)
                    storage_key = f"ai:cache:{key}"
                    # Upsert semantics for cache store.
                    if storage_key in STORE.mget([storage_key]):
                        STORE.update(storage_key, value, ttl_f)
                    else:
                        STORE.create(storage_key, value, ttl_f)
                    _METRICS["ai_cache"]["stores"] += 1
                    self._json(201, {"key": key, "canonical": canon})
                    self._inc_metrics("POST", route="POST /ai/cache")
                    return

            if len(parts) >= 3 and parts[0] == "kv" and parts[1] == "incr":
                key = parts[2]
                delta = 1.0
                ttl = None
                # Parse query parameters for delta and ttl if provided
                _, query = self._parse()
                if "delta" in query:
                    try:
                        delta = float(query["delta"][0])
                    except ValueError:
                        self._send(400, "delta must be numeric")
                        self._inc_metrics("POST", route="POST /kv/incr/:key", error=True)
                        return
                if "ttl" in query:
                    try:
                        ttl = float(query["ttl"][0])
                    except ValueError:
                        self._send(400, "ttl must be numeric")
                        self._inc_metrics("POST", route="POST /kv/incr/:key", error=True)
                        return
                try:
                    new_val = STORE.incr(key, delta=delta, ttl=ttl)
                except TypeError as e:
                    self._send(400, str(e))
                    self._inc_metrics("POST", route="POST /kv/incr/:key", error=True)
                    return
                self._json(200, {"key": key, "value": new_val})
                self._inc_metrics("POST", route="POST /kv/incr/:key")
                return
            if parts == ["kv", "batch"]:
                payload = json.loads(self._body() or b"{}")
                items = payload.get("items", {})
                ttl = payload.get("ttl")
                if not isinstance(items, dict):
                    self._send(400, "items must be dict")
                    self._inc_metrics("POST", route="POST /kv/batch", error=True)
                    return
                STORE.mset(items, ttl)
                self._send(201)
                self._inc_metrics("POST", route="POST /kv/batch")
                return

            self._send(404, "Not Found")
            self._inc_metrics("POST", route="POST (not_found)", error=True)
        except ValueError:
            self._send(400, "Bad JSON")
            self._inc_metrics("POST", route="POST (bad_json)", error=True)

    # Admin & metrics APIs
    def _handle_admin_get(self, parts: list[str], query: Dict[str, list[str]]) -> None:
        if not parts:
            self._json(200, {"status": "ok", "shards": SHARDS})
            self._inc_metrics("GET", route="GET /admin")
            return
        if parts[0] == "health":
            self._json(
                200,
                {
                    "status": "ok",
                    "uptime_seconds": time.time() - _METRICS["started_at"],
                    "shards": SHARDS,
                },
            )
            self._inc_metrics("GET", route="GET /admin/health")
            return
        if parts[0] == "metrics":
            self._json(200, _METRICS)
            self._inc_metrics("GET", route="GET /admin/metrics")
            return
        if parts[0] == "snapshot":
            if _SNAPSHOT_MANAGER is None or not SNAPSHOT_FILE:
                self._send(400, "snapshotting is disabled")
                self._inc_metrics("GET", route="GET /admin/snapshot", error=True)
                return
            try:
                _SNAPSHOT_MANAGER.snapshot_once()
                self._json(200, {"status": "ok", "path": SNAPSHOT_FILE})
                self._inc_metrics("GET", route="GET /admin/snapshot")
            except Exception as e:
                self._send(500, f"snapshot failed: {e}")
                self._inc_metrics("GET", route="GET /admin/snapshot", error=True)
            return
        self._send(404, "Not Found")
        self._inc_metrics("GET", route="GET /admin (not_found)", error=True)

    def log_message(self, fmt: str, *args: Any) -> None:
        rid = getattr(self, "_request_id", "-")
        logging.info("%s rid=%s - %s", self.address_string(), rid, fmt % args)


def run() -> None:
    httpd = BaseHTTPServer.HTTPServer((HOST, PORT), KVHandler)
    logging.info(
        "Serving on http://%s:%d  shards=%d per_shard_max=%d",
        HOST,
        PORT,
        SHARDS,
        PER_SHARD_MAX,
    )

    def stop(sig: int, _frame: Any) -> None:
        logging.info("Shutting down (%s)…", sig)
        httpd.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    httpd.serve_forever()


if __name__ == "__main__":
    run()

