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

from ..core.sharded import ShardedKeyValueStore
from ..persistence.snapshot import SnapshotManager, load_snapshot
from ..persistence.wal import recover_from_wal
from ..cache.ai import ai_cache_manager
from ..metrics.registry import registry
from ..metrics.prometheus import registry_to_prometheus
from ..core.expiration import BackgroundExpirer
from ..config.settings import settings
from ..api.redis_server import RedisServer

STORE = ShardedKeyValueStore(
    shards=settings.SHARDS,
    per_shard_max=settings.PER_SHARD_MAX,
    eviction_policy=settings.EVICTION_POLICY,
    wal_path=settings.WAL_FILE
)

_EXPIRER = BackgroundExpirer(STORE, interval=60.0)
_EXPIRER.start()

if settings.SNAPSHOT_FILE:
    load_snapshot(STORE, settings.SNAPSHOT_FILE)

if settings.WAL_FILE:
    recover_from_wal(STORE, STORE._wal)

_REDIS_SERVER: RedisServer | None = None
if settings.REDIS_ENABLED:
    _REDIS_SERVER = RedisServer(STORE, settings.REDIS_HOST, settings.REDIS_PORT)
    _REDIS_SERVER.start()

_SNAPSHOT_MANAGER: SnapshotManager | None = None
if settings.SNAPSHOT_FILE and settings.SNAPSHOT_INTERVAL > 0:
    _SNAPSHOT_MANAGER = SnapshotManager(STORE, settings.SNAPSHOT_FILE, settings.SNAPSHOT_INTERVAL)
    _SNAPSHOT_MANAGER.start()

def _apply_runtime_config() -> None:
    global _REDIS_SERVER
    global _SNAPSHOT_MANAGER

    if settings.REDIS_ENABLED:
        if _REDIS_SERVER is None:
            _REDIS_SERVER = RedisServer(STORE, settings.REDIS_HOST, settings.REDIS_PORT)
            _REDIS_SERVER.start()
    else:
        if _REDIS_SERVER is not None:
            try:
                _REDIS_SERVER.stop()
            finally:
                _REDIS_SERVER = None

    if settings.SNAPSHOT_FILE and settings.SNAPSHOT_INTERVAL > 0:
        if _SNAPSHOT_MANAGER is None:
            _SNAPSHOT_MANAGER = SnapshotManager(STORE, settings.SNAPSHOT_FILE, settings.SNAPSHOT_INTERVAL)
            _SNAPSHOT_MANAGER.start()
    else:
        if _SNAPSHOT_MANAGER is not None:
            try:
                _SNAPSHOT_MANAGER.stop()
            finally:
                _SNAPSHOT_MANAGER = None

class KVHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    server_version = "PX-KVStore/2.0"

    def _fault_sleep(self) -> None:
        if settings.FAULT_LATENCY_MS <= 0 and settings.FAULT_LATENCY_JITTER_MS <= 0:
            return
        base = max(0.0, settings.FAULT_LATENCY_MS)
        jitter = max(0.0, settings.FAULT_LATENCY_JITTER_MS)
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

    def _inc_metrics(self, method: str, route: str = "", error: bool = False) -> None:
        registry.inc_requests(method, error)
        if route:
            self._ensure_request_context()
            elapsed_ms = (time.time() - self._request_started_at) * 1000.0
            registry.observe_latency(route, elapsed_ms)

    def do_GET(self) -> None:
        self._request_id = uuid.uuid4().hex
        self._request_started_at = time.time()
        self._fault_sleep()
        try:
            parts, query = self._parse()
            if not parts:
                self._json(200, {"status": "ok"})
                return

            if parts == ["replication", "snapshot"]:
                if settings.REPLICATION_ROLE != "leader":
                    self._send(403, "Only leader can provide snapshot")
                    return
                data = STORE.dump()
                data["_lsn"] = STORE._wal._lsn
                self._json(200, data)
                self._inc_metrics("GET", route="GET /replication/snapshot")
                return

            if parts == ["replication", "wal"]:
                if settings.REPLICATION_ROLE != "leader":
                    self._send(403, "Only leader can provide WAL")
                    return
                start_lsn = int(query.get("start_lsn", [0])[0])
                oldest = STORE._wal.get_oldest_lsn()
                if oldest and start_lsn < oldest - 1:
                    self._send(410, "WAL truncated, full sync required")
                    self._inc_metrics("GET", route="GET /replication/wal", error=True)
                    return
                entries = STORE._wal.get_entries(start_lsn)
                self._json(200, {"changes": entries})
                self._inc_metrics("GET", route="GET /replication/wal")
                return

            if parts[0] == "admin":
                self._handle_admin_get(parts[1:], query)
                return

            if parts[0] == "ai":
                if len(parts) >= 2 and parts[1] == "cache":
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
            
            if parts == ["replication", "sync"]:
                if settings.REPLICATION_ROLE != "follower":
                    self._send(403, "Only followers can receive sync")
                    return
                payload = json.loads(self._body() or b"{}")
                changes = payload.get("changes", [])
                STORE._replication.apply_changes(changes)
                self._json(200, {"status": "ok", "applied": len(changes)})
                self._inc_metrics("POST", route="POST /replication/sync")
                return

            if len(parts) >= 1 and parts[0] == "ai":
                if parts == ["ai", "cache", "lookup"]:
                    payload = json.loads(self._body() or b"{}")
                    prompt = payload.get("prompt", "")
                    model = payload.get("model", "")
                    model_version = payload.get("model_version")
                    params = payload.get("params", {}) or {}
                    if not isinstance(prompt, str) or not isinstance(model, str) or not isinstance(params, dict):
                        self._send(400, "prompt/model must be string; params must be object")
                        self._inc_metrics("POST", route="POST /ai/cache/lookup", error=True)
                        return
                    key, canon = ai_cache_manager.compute_key(prompt, model, params, model_version=model_version)
                    storage_key = f"ai:cache:{key}"
                    registry.inc_ai_cache("lookups")
                    try:
                        cached = STORE.read(storage_key)
                        cached = ai_cache_manager.decompress_value(cached)
                    except KeyError:
                        registry.inc_ai_cache("misses")
                        self._json(200, {"hit": False, "key": key, "canonical": canon})
                        self._inc_metrics("POST", route="POST /ai/cache/lookup")
                        return
                    registry.inc_ai_cache("hits")
                    self._json(200, {"hit": True, "key": key, "canonical": canon, "value": cached})
                    self._inc_metrics("POST", route="POST /ai/cache/lookup")
                    return

                if parts == ["ai", "cache"]:
                    payload = json.loads(self._body() or b"{}")
                    prompt = payload.get("prompt", "")
                    model = payload.get("model", "")
                    model_version = payload.get("model_version")
                    params = payload.get("params", {}) or {}
                    value = payload.get("value")
                    ttl = payload.get("ttl")
                    compress = payload.get("compress", False)
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
                    key, canon = ai_cache_manager.compute_key(prompt, model, params, model_version=model_version)
                    storage_key = f"ai:cache:{key}"
                    if compress:
                        value = ai_cache_manager.compress_value(value)
                    if storage_key in STORE.mget([storage_key]):
                        STORE.update(storage_key, value, ttl_f)
                    else:
                        STORE.create(storage_key, value, ttl_f)
                    registry.inc_ai_cache("stores")
                    self._json(201, {"key": key, "canonical": canon})
                    self._inc_metrics("POST", route="POST /ai/cache")
                    return

            if len(parts) >= 3 and parts[0] == "kv" and parts[1] == "incr":
                key = parts[2]
                delta = 1.0
                ttl = None
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

            if parts == ["admin", "config"]:
                payload = json.loads(self._body() or b"{}")
                settings.update(payload)
                _apply_runtime_config()
                self._json(200, {"status": "ok", "config": settings.to_dict()})
                self._inc_metrics("POST", route="POST /admin/config")
                return

            if parts == ["admin", "config", "reload"]:
                settings.reload()
                _apply_runtime_config()
                self._json(200, {"status": "ok", "config": settings.to_dict()})
                self._inc_metrics("POST", route="POST /admin/config/reload")
                return

            self._send(404, "Not Found")
            self._inc_metrics("POST", route="POST (not_found)", error=True)
        except ValueError:
            self._send(400, "Bad JSON")
            self._inc_metrics("POST", route="POST (bad_json)", error=True)

    def _handle_admin_get(self, parts: list[str], query: Dict[str, list[str]]) -> None:
        if not parts:
            self._json(200, {"status": "ok", "shards": settings.SHARDS, "role": settings.REPLICATION_ROLE})
            self._inc_metrics("GET", route="GET /admin")
            return
        if parts[0] == "health":
            self._json(
                200,
                {
                    "status": "ok",
                    "uptime_seconds": time.time() - registry.get_all()["started_at"],
                    "shards": settings.SHARDS,
                },
            )
            self._inc_metrics("GET", route="GET /admin/health")
            return
        if parts[0] == "metrics":
            fmt = query.get("format", ["json"])[0]
            if fmt == "prometheus":
                prom_data = registry_to_prometheus(registry.get_all())
                self._send(200, prom_data, "text/plain; version=0.0.4; charset=utf-8")
            else:
                self._json(200, registry.get_all())
            self._inc_metrics("GET", route="GET /admin/metrics")
            return
        if parts[0] == "snapshot":
            if _SNAPSHOT_MANAGER is None or not settings.SNAPSHOT_FILE:
                self._send(400, "snapshotting is disabled")
                self._inc_metrics("GET", route="GET /admin/snapshot", error=True)
                return
            try:
                _SNAPSHOT_MANAGER.snapshot_once()
                self._json(200, {"status": "ok", "path": settings.SNAPSHOT_FILE})
                self._inc_metrics("GET", route="GET /admin/snapshot")
            except Exception as e:
                self._send(500, f"snapshot failed: {e}")
                self._inc_metrics("GET", route="GET /admin/snapshot", error=True)
            return
        
        if parts[0] == "config":
            self._json(200, settings.to_dict())
            self._inc_metrics("GET", route="GET /admin/config")
            return

        self._send(404, "Not Found")
        self._inc_metrics("GET", route="GET /admin (not_found)", error=True)

    def log_message(self, fmt: str, *args: Any) -> None:
        rid = getattr(self, "_request_id", "-")
        logging.info("%s rid=%s - %s", self.address_string(), rid, fmt % args)


def run() -> None:
    STORE._replication.start()

    httpd = BaseHTTPServer.HTTPServer((settings.HOST, settings.PORT), KVHandler)
    logging.info(
        "Serving on http://%s:%d  shards=%d per_shard_max=%d",
        settings.HOST,
        settings.PORT,
        settings.SHARDS,
        settings.PER_SHARD_MAX,
    )

    def stop(sig: int, _frame: Any) -> None:
        logging.info("Shutting down (%s)…", sig)
        httpd.shutdown()
        sys.exit(0)

    def reload_config(sig: int, _frame: Any) -> None:
        logging.info("SIGHUP received, reloading config from environment…")
        settings.reload()
        _apply_runtime_config()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    if hasattr(signal, 'SIGHUP'):
        signal.signal(signal.SIGHUP, reload_config)
    httpd.serve_forever()

if __name__ == "__main__":
    run()
