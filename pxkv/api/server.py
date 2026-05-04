#!/usr/bin/env python
# -*- coding: utf-8 -*-

import http.server as BaseHTTPServer
import json
import logging
import os
import signal
import sys
import time
import math
import urllib.parse as urlparse
import urllib.request
import urllib.error
import uuid
import random
import gzip
from threading import RLock
from queue import Empty
from typing import Any, Dict, Tuple, Optional

from ..core.sharded import ShardedKeyValueStore
from ..persistence.snapshot import SnapshotManager, load_snapshot
from ..persistence.wal import recover_from_wal
from ..cache.ai import ai_cache_manager
from ..metrics.registry import registry
from ..metrics.prometheus import registry_to_prometheus
from ..core.expiration import BackgroundExpirer
from ..config.settings import settings
from ..api.redis_server import RedisServer
from ..auth import ROLE_ADMIN, ROLE_READER, ROLE_WRITER, best_role_for_secret, parse_basic_password, parse_bearer, role_satisfies
from ..notifications import notifier

STORE = ShardedKeyValueStore(
    shards=settings.SHARDS,
    per_shard_max=settings.PER_SHARD_MAX,
    eviction_policy=settings.EVICTION_POLICY,
    wal_path=settings.WAL_FILE,
    tiering_dir=settings.TIERING_DIR,
    tiering_backend=settings.TIERING_BACKEND,
    tiering_http_base_url=settings.TIERING_HTTP_BASE_URL,
    tiering_http_timeout=settings.TIERING_HTTP_TIMEOUT,
    tiering_s3_bucket=settings.TIERING_S3_BUCKET,
    tiering_s3_prefix=settings.TIERING_S3_PREFIX,
    tiering_s3_region=settings.TIERING_S3_REGION,
    tiering_s3_endpoint_url=settings.TIERING_S3_ENDPOINT_URL,
    tiering_prefetch_enabled=settings.TIERING_PREFETCH_ENABLED,
    tiering_prefetch_workers=settings.TIERING_PREFETCH_WORKERS,
    tiering_prefetch_wait_ms=settings.TIERING_PREFETCH_WAIT_MS,
    tiering_prefetch_cache_max=settings.TIERING_PREFETCH_CACHE_MAX,
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

class _TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: int):
        self.rate_per_sec = float(rate_per_sec)
        self.capacity = int(capacity)
        self.tokens = float(capacity)
        self.updated_at = time.monotonic()
        self._lock = RLock()

    def update_limits(self, rate_per_sec: float, capacity: int) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.updated_at)
            self.tokens = min(float(self.capacity), self.tokens + elapsed * float(self.rate_per_sec))
            self.updated_at = now
            self.rate_per_sec = float(rate_per_sec)
            self.capacity = int(capacity)
            self.tokens = min(float(self.capacity), self.tokens)

    def allow(self, cost: float = 1.0) -> tuple[bool, float]:
        with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.updated_at)
            self.tokens = min(float(self.capacity), self.tokens + elapsed * float(self.rate_per_sec))
            self.updated_at = now

            if self.tokens >= float(cost):
                self.tokens -= float(cost)
                return True, 0.0

            if self.rate_per_sec <= 0:
                return False, 3600.0

            missing = float(cost) - self.tokens
            retry_after = missing / float(self.rate_per_sec)
            return False, retry_after


class _RateLimiter:
    def __init__(self):
        self._lock = RLock()
        self._enabled = False
        self._default_policy: dict = {"rps": 0.0, "burst": 0, "per_ip": True}
        self._route_policies: dict = {}
        self._buckets: dict[str, _TokenBucket] = {}

    def configure(self, enabled: bool, default_policy: dict, route_policies: dict) -> None:
        with self._lock:
            self._enabled = bool(enabled)
            self._default_policy = default_policy or {"rps": 0.0, "burst": 0, "per_ip": True}
            self._route_policies = route_policies or {}
            self._buckets = {}

    def configure_from_settings(self) -> None:
        self.configure(
            enabled=getattr(settings, "RATE_LIMIT_ENABLED", False),
            default_policy=getattr(settings, "RATE_LIMIT_DEFAULT", None) or {"rps": 0.0, "burst": 0, "per_ip": True},
            route_policies=getattr(settings, "RATE_LIMIT_ROUTES", None) or {},
        )

    def _policy_for(self, route: str) -> tuple[float, int, bool] | None:
        pol = (self._route_policies or {}).get(route)
        if not isinstance(pol, dict):
            pol = self._default_policy or {}

        try:
            rps = float(pol.get("rps", 0.0) or 0.0)
            burst = int(pol.get("burst", 0) or 0)
            per_ip = bool(pol.get("per_ip", True))
        except Exception:
            return None

        if rps <= 0 or burst <= 0:
            return None
        return rps, burst, per_ip

    def allow(self, route: str, client_ip: str) -> tuple[bool, int]:
        with self._lock:
            if not self._enabled:
                return True, 0
            policy = self._policy_for(route)
            if policy is None:
                return True, 0
            rps, burst, per_ip = policy
            dim = client_ip if per_ip else "*"
            key = f"{route}|{dim}"
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _TokenBucket(rps, burst)
                self._buckets[key] = bucket
            elif bucket.rate_per_sec != rps or bucket.capacity != burst:
                bucket.update_limits(rps, burst)

        ok, retry_after = bucket.allow(1.0)
        if ok:
            return True, 0
        retry_s = int(max(1.0, math.ceil(retry_after)))
        return False, retry_s


_RATE_LIMITER = _RateLimiter()
_RATE_LIMITER.configure_from_settings()

def _apply_runtime_config() -> None:
    global _REDIS_SERVER
    global _SNAPSHOT_MANAGER

    _RATE_LIMITER.configure_from_settings()

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
    protocol_version = "HTTP/1.1"

    def _client_ip(self) -> str:
        xff = self.headers.get("X-Forwarded-For", "") or ""
        if xff.strip():
            return xff.split(",")[0].strip()
        try:
            return str(self.client_address[0])
        except Exception:
            return ""

    def _rate_limit(self, route: str) -> bool:
        if route in ("GET /admin/config", "POST /admin/config", "POST /admin/config/reload"):
            return True
        ok, retry_after_s = _RATE_LIMITER.allow(route, self._client_ip())
        if ok:
            return True
        self._json(
            429,
            {"error": "rate_limited", "route": route, "retry_after_seconds": retry_after_s},
            headers={"Retry-After": str(retry_after_s)},
        )
        self._inc_metrics(self.command or "GET", route=route, error=True)
        return False

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

    def _send(
        self,
        code: int,
        body: Any = b"",
        mime: str = "text/plain; charset=utf-8",
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self._ensure_request_context()
        if not isinstance(body, (bytes, bytearray)):
            body = str(body).encode("utf-8")
        self.send_response(code)
        self.send_header("X-Request-Id", self._request_id)
        self.send_header("Connection", "close")
        if headers:
            for k, v in headers.items():
                if v is None:
                    continue
                self.send_header(k, str(v))
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, obj: Any, headers: Optional[Dict[str, str]] = None) -> None:
        self._ensure_request_context()
        def _default(v: Any):
            if isinstance(v, (bytes, bytearray)):
                return v.decode("utf-8", errors="replace")
            raise TypeError

        payload = json.dumps(obj, default=_default, ensure_ascii=False)
        self._send(code, payload.encode("utf-8"), "application/json", headers=headers)

    def _staleness_headers(self) -> Dict[str, str]:
        if settings.REPLICATION_ROLE != "follower":
            return {}
        st = STORE._replication.get_staleness()
        return {
            "X-PXKV-Role": st.get("role", ""),
            "X-PXKV-Replication-Last-Applied-LSN": str(st.get("last_applied_lsn", 0)),
            "X-PXKV-Replication-Known-Leader-LSN": str(st.get("known_leader_lsn", 0)),
            "X-PXKV-Replication-Lag-LSN": str(st.get("lag_lsn", 0)),
            "X-PXKV-Replication-Last-Applied-Age-MS": str(st.get("last_applied_age_ms", 0.0)),
        }

    def _reject_readonly(self, route: str) -> None:
        self._send(403, "READONLY You can't write against a read-only follower.", headers=self._staleness_headers())
        self._inc_metrics("WRITE", route=route, error=True)

    def _auth_enabled(self) -> bool:
        return any(
            [
                settings.AUTH_ADMIN_TOKEN,
                settings.AUTH_WRITER_TOKEN,
                settings.AUTH_READER_TOKEN,
                settings.AUTH_ADMIN_PASSWORD,
                settings.AUTH_WRITER_PASSWORD,
                settings.AUTH_READER_PASSWORD,
            ]
        )

    def _auth_role(self) -> Optional[str]:
        authorization = self.headers.get("Authorization", "") or ""
        token = parse_bearer(authorization) or (self.headers.get("X-Auth-Token", "") or "")
        password = parse_basic_password(authorization) or (self.headers.get("X-Auth-Password", "") or "")

        if token:
            role = best_role_for_secret(
                token,
                admin_token=settings.AUTH_ADMIN_TOKEN,
                writer_token=settings.AUTH_WRITER_TOKEN,
                reader_token=settings.AUTH_READER_TOKEN,
                admin_password=settings.AUTH_ADMIN_PASSWORD,
                writer_password=settings.AUTH_WRITER_PASSWORD,
                reader_password=settings.AUTH_READER_PASSWORD,
            )
            if role:
                return role

        if password:
            role = best_role_for_secret(
                password,
                admin_token=settings.AUTH_ADMIN_TOKEN,
                writer_token=settings.AUTH_WRITER_TOKEN,
                reader_token=settings.AUTH_READER_TOKEN,
                admin_password=settings.AUTH_ADMIN_PASSWORD,
                writer_password=settings.AUTH_WRITER_PASSWORD,
                reader_password=settings.AUTH_READER_PASSWORD,
            )
            if role:
                return role

        return None

    def _require_role(self, required: str) -> bool:
        if not self._auth_enabled():
            return True
        role = self._auth_role()
        if role is None:
            self._send(
                401,
                "Unauthorized",
                headers={
                    "WWW-Authenticate": 'Bearer realm="pxkv", charset="UTF-8"',
                },
            )
            self._inc_metrics("AUTH", route="AUTH (missing)", error=True)
            return False
        if not role_satisfies(role, required):
            self._send(403, "Forbidden")
            self._inc_metrics("AUTH", route="AUTH (forbidden)", error=True)
            return False
        return True

    def _follower_read_routing_enabled(self) -> bool:
        return settings.REPLICATION_ROLE == "leader" and settings.FOLLOWER_READ_ENABLED and bool(STORE._replication.followers)

    def _parse_int(self, s: Any, default: int) -> int:
        try:
            return int(s)
        except Exception:
            return int(default)

    def _parse_float(self, s: Any, default: float) -> float:
        try:
            return float(s)
        except Exception:
            return float(default)

    def _select_follower_for_read(self) -> Optional[str]:
        followers = list(STORE._replication.followers or [])
        if not followers:
            return None

        strategy = (settings.FOLLOWER_READ_STRATEGY or "").lower()
        if strategy == "random":
            return random.choice(followers)

        metrics = registry.get_all().get("replication", {}).get("followers", {}) or {}
        best = None
        best_lag = None
        for f in followers:
            try:
                lag = int((metrics.get(f, {}) or {}).get("lag_lsn", 0) or 0)
            except Exception:
                continue
            if best is None or (best_lag is not None and lag < best_lag):
                best = f
                best_lag = lag
        return best or random.choice(followers)

    def _forward_auth_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        for k in ("Authorization", "X-Auth-Token", "X-Auth-Password"):
            v = self.headers.get(k)
            if v:
                headers[k] = v
        return headers

    def _http_get_bytes(self, url: str, headers: Dict[str, str], timeout: float) -> tuple[int, bytes, Dict[str, str]]:
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status = int(getattr(resp, "status", 0) or 0)
                body = resp.read()
                return status, body, dict(resp.headers.items())
        except urllib.error.HTTPError as e:
            try:
                body = e.read()
            except Exception:
                body = b""
            return int(getattr(e, "code", 500)), body, dict(getattr(e, "headers", {}) or {})
        except Exception:
            return 0, b"", {}

    def _staleness_ok(self, hdrs: Dict[str, str], max_lag_lsn: int, max_age_ms: float) -> bool:
        lag = self._parse_int(hdrs.get("X-PXKV-Replication-Lag-LSN", "0"), 0)
        age = self._parse_float(hdrs.get("X-PXKV-Replication-Last-Applied-Age-MS", "0"), 0.0)
        if max_lag_lsn > 0 and lag > max_lag_lsn:
            return False
        if max_age_ms > 0 and age > max_age_ms:
            return False
        return True

    def _maybe_route_read_to_follower(self, parts: list[str], query: Dict[str, list[str]]) -> bool:
        if not self._follower_read_routing_enabled():
            return False
        if self.headers.get("X-PXKV-Proxy", "") == "1":
            return False
        if not parts or parts[0] != "kv":
            return False

        read_from = (query.get("read_from", [""])[0] or "").lower()
        if read_from == "leader":
            return False
        if read_from not in ("", "auto", "follower"):
            return False

        follower = self._select_follower_for_read()
        if not follower:
            return False

        max_lag_lsn = self._parse_int(query.get("max_lag_lsn", [settings.FOLLOWER_READ_MAX_LAG_LSN])[0], settings.FOLLOWER_READ_MAX_LAG_LSN)
        max_age_ms = self._parse_float(query.get("max_age_ms", [settings.FOLLOWER_READ_MAX_AGE_MS])[0], settings.FOLLOWER_READ_MAX_AGE_MS)

        upstream_headers = {"X-PXKV-Proxy": "1"}
        upstream_headers.update(self._forward_auth_headers())
        upstream_url = f"http://{follower}{self.path}"
        status, body, hdrs = self._http_get_bytes(upstream_url, upstream_headers, timeout=2.0)

        ok = self._staleness_ok(hdrs, max_lag_lsn=max_lag_lsn, max_age_ms=max_age_ms)
        if status in (200, 404) and ok:
            out_headers = {
                "X-PXKV-Read-Source": "follower",
                "X-PXKV-Read-Follower": follower,
                "X-PXKV-Read-Max-Lag-LSN": str(max_lag_lsn),
                "X-PXKV-Read-Max-Age-MS": str(max_age_ms),
            }
            for k in (
                "X-PXKV-Role",
                "X-PXKV-Replication-Last-Applied-LSN",
                "X-PXKV-Replication-Known-Leader-LSN",
                "X-PXKV-Replication-Lag-LSN",
                "X-PXKV-Replication-Last-Applied-Age-MS",
            ):
                if k in hdrs:
                    out_headers[k] = hdrs[k]
            if status == 200:
                try:
                    obj = json.loads(body.decode("utf-8")) if body else {}
                    self._json(200, obj, headers=out_headers)
                except Exception:
                    self._send(200, body, headers=out_headers)
                self._inc_metrics("GET", route="GET /kv (routed_to_follower)")
                return True
            self._send(404, body or b"Not Found", headers=out_headers)
            self._inc_metrics("GET", route="GET /kv (routed_404_follower)")
            return True

        self._fallback_headers = {
            "X-PXKV-Read-Source": "leader",
            "X-PXKV-Read-Follower": follower,
            "X-PXKV-Read-Fallback": "stale_or_error",
            "X-PXKV-Read-Upstream-Status": str(status),
            "X-PXKV-Read-Upstream-Lag-LSN": str(self._parse_int(hdrs.get("X-PXKV-Replication-Lag-LSN", "0"), 0)),
            "X-PXKV-Read-Upstream-Age-MS": str(self._parse_float(hdrs.get("X-PXKV-Replication-Last-Applied-Age-MS", "0"), 0.0)),
        }
        return False

    def _send_snapshot_ndjson(self, compress: bool) -> None:
        self._ensure_request_context()

        def _default(v: Any):
            if isinstance(v, (bytes, bytearray)):
                return v.decode("utf-8", errors="replace")
            raise TypeError

        class _ChunkedWriter:
            def __init__(self, wfile):
                self.wfile = wfile

            def write(self, b: Any) -> int:
                if not b:
                    return 0
                if not isinstance(b, (bytes, bytearray)):
                    b = str(b).encode("utf-8")
                self.wfile.write(f"{len(b):X}\r\n".encode("utf-8"))
                self.wfile.write(b)
                self.wfile.write(b"\r\n")
                return len(b)

            def flush(self) -> None:
                try:
                    self.wfile.flush()
                except Exception:
                    pass

            def close(self) -> None:
                self.flush()

        self.send_response(200)
        self.send_header("X-Request-Id", self._request_id)
        self.send_header("Connection", "close")
        self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
        if compress:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        writer = _ChunkedWriter(self.wfile)
        out: Any = writer
        if compress:
            out = gzip.GzipFile(fileobj=writer, mode="wb")

        def _write_line(obj: Any) -> None:
            line = (json.dumps(obj, default=_default, ensure_ascii=False) + "\n").encode("utf-8")
            out.write(line)

        lsn, data = STORE.dump_with_lsn()
        _write_line({"_lsn": int(lsn), "shards": settings.SHARDS})
        for idx_str, shard_state in sorted(data.items(), key=lambda kv: int(kv[0])):
            _write_line({"shard": int(idx_str), "state": shard_state})

        try:
            if compress:
                out.close()
        finally:
            writer.wfile.write(b"0\r\n\r\n")
            writer.flush()

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
                if not self._rate_limit("GET /"):
                    return
                self._json(200, {"status": "ok"})
                self._inc_metrics("GET", route="GET /")
                return

            if parts == ["events", "keyspace"]:
                if not self._rate_limit("GET /events/keyspace"):
                    return
                if not self._require_role(ROLE_READER):
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.end_headers()
                sid, q = notifier.subscribe()
                try:
                    while True:
                        try:
                            ev = q.get(timeout=15.0)
                            payload = ev.to_json()
                            self.wfile.write(b"event: keyspace\n")
                            self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                            try:
                                self.wfile.flush()
                            except Exception:
                                pass
                        except Empty:
                            try:
                                self.wfile.write(b": ping\n\n")
                                self.wfile.flush()
                            except Exception:
                                break
                        except (BrokenPipeError, ConnectionResetError):
                            break
                finally:
                    notifier.unsubscribe(sid)
                return

            if parts == ["replication", "snapshot"]:
                if not self._rate_limit("GET /replication/snapshot"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
                if settings.REPLICATION_ROLE != "leader":
                    self._send(403, "Only leader can provide snapshot")
                    return
                fmt = query.get("format", ["json"])[0]
                compress = query.get("compress", [""])[0].lower() == "gzip"
                if fmt == "ndjson":
                    self._send_snapshot_ndjson(compress=compress)
                else:
                    lsn, data = STORE.dump_with_lsn()
                    payload = dict(data)
                    payload["_lsn"] = int(lsn)
                    self._json(200, payload)
                self._inc_metrics("GET", route="GET /replication/snapshot")
                return

            if parts == ["replication", "wal"]:
                if not self._rate_limit("GET /replication/wal"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
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
                self._json(200, {"leader_lsn": STORE._wal._lsn, "changes": entries})
                self._inc_metrics("GET", route="GET /replication/wal")
                return

            if parts[0] == "admin":
                if not self._require_role(ROLE_ADMIN):
                    return
                self._handle_admin_get(parts[1:], query)
                return

            if parts[0] == "ai":
                if not self._rate_limit("GET /ai/cache/:key"):
                    return
                if not self._require_role(ROLE_READER):
                    return
                if len(parts) >= 2 and parts[1] == "cache":
                    if len(parts) != 3 or not parts[2]:
                        raise ValueError
                    cache_key = parts[2]
                    storage_key = f"ai:cache:{cache_key}"
                    value = STORE.read(storage_key)
                    self._json(200, {"key": cache_key, "value": value}, headers=self._staleness_headers())
                    self._inc_metrics("GET", route="GET /ai/cache/:key")
                    return
                raise ValueError

            if parts[0] != "kv":
                raise ValueError

            if len(parts) >= 2 and parts[1] == "batch":
                if not self._rate_limit("GET /kv/batch"):
                    return
            elif len(parts) >= 2 and parts[1] == "scan":
                if not self._rate_limit("GET /kv/scan"):
                    return
            else:
                if not self._rate_limit("GET /kv/:key"):
                    return

            if parts and parts[0] == "kv":
                if self._maybe_route_read_to_follower(parts, query):
                    return

            if not self._require_role(ROLE_READER):
                return
            if len(parts) >= 2 and parts[1] == "batch":
                if "keys" not in query:
                    self._send(400, "keys query param required")
                    self._inc_metrics("GET", route="GET /kv/batch", error=True)
                    return
                keys = query["keys"][0].split(",")
                extra = getattr(self, "_fallback_headers", None)
                headers = self._staleness_headers()
                if isinstance(extra, dict):
                    headers = {**headers, **extra}
                    self._fallback_headers = None
                self._json(200, STORE.mget(keys), headers=headers)
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
                extra = getattr(self, "_fallback_headers", None)
                headers = self._staleness_headers()
                if isinstance(extra, dict):
                    headers = {**headers, **extra}
                    self._fallback_headers = None
                self._json(200, {"keys": keys}, headers=headers)
                self._inc_metrics("GET", route="GET /kv/scan")
                return

            if len(parts) < 2:
                raise ValueError

            key = parts[1]
            value = STORE.read(key)
            extra = getattr(self, "_fallback_headers", None)
            headers = self._staleness_headers()
            if isinstance(extra, dict):
                headers = {**headers, **extra}
                self._fallback_headers = None
            self._json(200, {"key": key, "value": value}, headers=headers)
            self._inc_metrics("GET", route="GET /kv/:key")
        except KeyError as e:
            extra = getattr(self, "_fallback_headers", None)
            headers = self._staleness_headers()
            if isinstance(extra, dict):
                headers = {**headers, **extra}
                self._fallback_headers = None
            self._send(404, str(e), headers=headers)
            if self.path.startswith("/ai/cache/"):
                self._inc_metrics("GET", route="GET /ai/cache/:key", error=True)
            else:
                self._inc_metrics("GET", route="GET /kv/:key", error=True)
        except ValueError:
            extra = getattr(self, "_fallback_headers", None)
            headers = self._staleness_headers()
            if isinstance(extra, dict):
                headers = {**headers, **extra}
                self._fallback_headers = None
            self._send(404, "Not Found", headers=headers)
            self._inc_metrics("GET", route="GET (not_found)", error=True)

    def do_PUT(self) -> None:
        self._request_id = uuid.uuid4().hex
        self._request_started_at = time.time()
        self._fault_sleep()
        try:
            if not self._rate_limit("PUT /kv/:key"):
                return
            if not self._require_role(ROLE_WRITER):
                return
            if settings.REPLICATION_ROLE == "follower":
                self._reject_readonly(route="PUT /kv/:key")
                return
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
            if not self._rate_limit("DELETE /kv/:key"):
                return
            if not self._require_role(ROLE_WRITER):
                return
            if settings.REPLICATION_ROLE == "follower":
                self._reject_readonly(route="DELETE /kv/:key")
                return
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
                if not self._rate_limit("POST /replication/sync"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
                if settings.REPLICATION_ROLE != "follower":
                    self._send(403, "Only followers can receive sync")
                    return
                payload = json.loads(self._body() or b"{}")
                STORE._replication.set_known_leader_lsn(int(payload.get("leader_lsn", 0) or 0))
                changes = payload.get("changes", [])
                STORE._replication.apply_changes(changes)
                self._json(
                    200,
                    {
                        "status": "ok",
                        "applied": len(changes),
                        "last_applied_lsn": STORE._replication._last_applied_lsn,
                    },
                )
                self._inc_metrics("POST", route="POST /replication/sync")
                return

            if len(parts) >= 1 and parts[0] == "ai":
                if parts == ["ai", "cache", "lookup"]:
                    if not self._rate_limit("POST /ai/cache/lookup"):
                        return
                    if not self._require_role(ROLE_READER):
                        return
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
                    if not self._rate_limit("POST /ai/cache"):
                        return
                    if not self._require_role(ROLE_WRITER):
                        return
                    if settings.REPLICATION_ROLE == "follower":
                        self._reject_readonly(route="POST /ai/cache")
                        return
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
                if not self._rate_limit("POST /kv/incr/:key"):
                    return
                if not self._require_role(ROLE_WRITER):
                    return
                if settings.REPLICATION_ROLE == "follower":
                    self._reject_readonly(route="POST /kv/incr/:key")
                    return
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
                if not self._rate_limit("POST /kv/batch"):
                    return
                if not self._require_role(ROLE_WRITER):
                    return
                if settings.REPLICATION_ROLE == "follower":
                    self._reject_readonly(route="POST /kv/batch")
                    return
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
                if not self._rate_limit("POST /admin/config"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
                payload = json.loads(self._body() or b"{}")
                settings.update(payload)
                _apply_runtime_config()
                self._json(200, {"status": "ok", "config": settings.to_dict()})
                self._inc_metrics("POST", route="POST /admin/config")
                return

            if parts == ["admin", "config", "reload"]:
                if not self._rate_limit("POST /admin/config/reload"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
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
            if not self._rate_limit("GET /admin"):
                return
            self._json(200, {"status": "ok", "shards": settings.SHARDS, "role": settings.REPLICATION_ROLE})
            self._inc_metrics("GET", route="GET /admin")
            return
        if parts[0] == "health":
            if not self._rate_limit("GET /admin/health"):
                return
            repl = STORE._replication.get_staleness() if settings.REPLICATION_ROLE == "follower" else None
            self._json(
                200,
                {
                    "status": "ok",
                    "uptime_seconds": time.time() - registry.get_all()["started_at"],
                    "shards": settings.SHARDS,
                    "replication": repl,
                },
            )
            self._inc_metrics("GET", route="GET /admin/health")
            return
        if parts[0] == "metrics":
            if not self._rate_limit("GET /admin/metrics"):
                return
            fmt = query.get("format", ["json"])[0]
            if fmt == "prometheus":
                prom_data = registry_to_prometheus(registry.get_all())
                self._send(200, prom_data, "text/plain; version=0.0.4; charset=utf-8")
            else:
                self._json(200, registry.get_all())
            self._inc_metrics("GET", route="GET /admin/metrics")
            return
        if parts[0] == "snapshot":
            if not self._rate_limit("GET /admin/snapshot"):
                return
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
            if not self._rate_limit("GET /admin/config"):
                return
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

    httpd = BaseHTTPServer.ThreadingHTTPServer((settings.HOST, settings.PORT), KVHandler)
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
