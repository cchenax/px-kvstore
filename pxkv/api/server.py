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
import ssl
import threading
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
from .. import tracing
from ..namespaces import NAMESPACE_HEADER, namespace_manager
from ..persistence.disk_throttle import disk_throttler
from ..persistence.gossip import initialize_gossip, get_gossip_membership
from ..core.query_plan_cache import query_plan_cache

def _server_ssl_context(cert_file: str, key_file: str) -> Optional[ssl.SSLContext]:
    if not cert_file or not key_file:
        return None
    if not os.path.exists(cert_file) or not os.path.exists(key_file):
        logging.warning("TLS enabled but cert/key file missing: cert=%s key=%s", cert_file, key_file)
        return None
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
        return ctx
    except Exception as e:
        logging.warning("Failed to initialize TLS context: %s", e)
        return None

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

_REDIS_TLS_SERVER: RedisServer | None = None
if getattr(settings, "REDIS_TLS_ENABLED", False):
    ctx = _server_ssl_context(getattr(settings, "REDIS_TLS_CERT_FILE", ""), getattr(settings, "REDIS_TLS_KEY_FILE", ""))
    if ctx is not None:
        _REDIS_TLS_SERVER = RedisServer(STORE, settings.REDIS_HOST, settings.REDIS_TLS_PORT, ssl_context=ctx)
        _REDIS_TLS_SERVER.start()

_SNAPSHOT_MANAGER: SnapshotManager | None = None
if settings.SNAPSHOT_FILE and settings.SNAPSHOT_INTERVAL > 0:
    _SNAPSHOT_MANAGER = SnapshotManager(STORE, settings.SNAPSHOT_FILE, settings.SNAPSHOT_INTERVAL)
    _SNAPSHOT_MANAGER.start()

registry.observe_disk_usage(disk_throttler.sample(force=True))

# Initialize gossip membership
_GOSSIP = None
if getattr(settings, "GOSSIP_ENABLED", False):
    self_addr = f"{settings.HOST}:{settings.PORT}"
    _GOSSIP = initialize_gossip(
        self_addr=self_addr,
        interval=getattr(settings, "GOSSIP_INTERVAL", 1.0),
        failure_timeout=getattr(settings, "GOSSIP_FAILURE_TIMEOUT", 5.0),
        seed_peers=getattr(settings, "GOSSIP_SEED_PEERS", []),
    )
    _GOSSIP.start()
    logging.info(f"Gossip membership initialized at {self_addr}, seed peers: {getattr(settings, 'GOSSIP_SEED_PEERS', [])}")

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

    def _policy_for(self, route: str, default_policy: Optional[dict] = None, route_policies: Optional[dict] = None) -> tuple[float, int, bool] | None:
        policies = route_policies if isinstance(route_policies, dict) else self._route_policies
        pol = (policies or {}).get(route)
        if not isinstance(pol, dict):
            pol = default_policy if isinstance(default_policy, dict) else self._default_policy or {}

        try:
            rps = float(pol.get("rps", 0.0) or 0.0)
            burst = int(pol.get("burst", 0) or 0)
            per_ip = bool(pol.get("per_ip", True))
        except Exception:
            return None

        if rps <= 0 or burst <= 0:
            return None
        return rps, burst, per_ip

    def allow(
        self,
        route: str,
        client_ip: str,
        *,
        enabled: Optional[bool] = None,
        scope: str = "",
        default_policy: Optional[dict] = None,
        route_policies: Optional[dict] = None,
    ) -> tuple[bool, int]:
        with self._lock:
            if not (self._enabled if enabled is None else bool(enabled)):
                return True, 0
            policy = self._policy_for(route, default_policy=default_policy, route_policies=route_policies)
            if policy is None:
                return True, 0
            rps, burst, per_ip = policy
            dim = client_ip if per_ip else "*"
            prefix = f"{scope}|" if scope else ""
            key = f"{prefix}{route}|{dim}"
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
    global _REDIS_TLS_SERVER
    global _SNAPSHOT_MANAGER

    _RATE_LIMITER.configure_from_settings()

    STORE._hotkeys.configure(
        enabled=getattr(settings, "HOT_KEY_DETECTION_ENABLED", False),
        window_seconds=getattr(settings, "HOT_KEY_WINDOW_SECONDS", 60.0),
        buckets=getattr(settings, "HOT_KEY_BUCKETS", 60),
        top_k=getattr(settings, "HOT_KEY_TOP_K", 10),
        threshold_qps=getattr(settings, "HOT_KEY_THRESHOLD_QPS", 0.0),
        sample_rate=getattr(settings, "HOT_KEY_SAMPLE_RATE", 1.0),
    )

    STORE._adaptive_ttl.configure(
        enabled=getattr(settings, "ADAPTIVE_TTL_ENABLED", False),
        min_ttl_seconds=getattr(settings, "ADAPTIVE_TTL_MIN_SECONDS", 1.0),
        max_ttl_seconds=getattr(settings, "ADAPTIVE_TTL_MAX_SECONDS", 86400.0),
        default_base_ttl_seconds=getattr(settings, "ADAPTIVE_TTL_DEFAULT_BASE_SECONDS", 60.0),
        hit_extend_factor=getattr(settings, "ADAPTIVE_TTL_HIT_EXTEND_FACTOR", 2.0),
        miss_shrink_factor=getattr(settings, "ADAPTIVE_TTL_MISS_SHRINK_FACTOR", 0.5),
        recency_half_life_seconds=getattr(settings, "ADAPTIVE_TTL_RECENCY_HALF_LIFE_SECONDS", 300.0),
        max_tracked_keys=getattr(settings, "ADAPTIVE_TTL_MAX_TRACKED_KEYS", 10000),
    )

    STORE._heavy_hitters.configure(
        enabled=getattr(settings, "HEAVY_HITTERS_ENABLED", False),
        k=getattr(settings, "HEAVY_HITTERS_K", 10),
        cms_width=getattr(settings, "HEAVY_HITTERS_CMS_WIDTH", 2048),
        cms_depth=getattr(settings, "HEAVY_HITTERS_CMS_DEPTH", 4),
        decay_interval_seconds=getattr(settings, "HEAVY_HITTERS_DECAY_INTERVAL_SECONDS", 60.0),
        decay_factor=getattr(settings, "HEAVY_HITTERS_DECAY_FACTOR", 0.5),
        threshold_count=getattr(settings, "HEAVY_HITTERS_THRESHOLD_COUNT", 0),
    )

    query_plan_cache.configure(
        enabled=getattr(settings, "QUERY_PLAN_CACHE_ENABLED", False),
        max_entries=getattr(settings, "QUERY_PLAN_CACHE_MAX_ENTRIES", 1024),
    )

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

    if getattr(settings, "REDIS_TLS_ENABLED", False):
        if _REDIS_TLS_SERVER is None:
            ctx = _server_ssl_context(getattr(settings, "REDIS_TLS_CERT_FILE", ""), getattr(settings, "REDIS_TLS_KEY_FILE", ""))
            if ctx is not None:
                _REDIS_TLS_SERVER = RedisServer(STORE, settings.REDIS_HOST, settings.REDIS_TLS_PORT, ssl_context=ctx)
                _REDIS_TLS_SERVER.start()
    else:
        if _REDIS_TLS_SERVER is not None:
            try:
                _REDIS_TLS_SERVER.stop()
            finally:
                _REDIS_TLS_SERVER = None

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

    def _namespace_from_query(self, query: Optional[Dict[str, list[str]]] = None) -> Optional[str]:
        if query is None:
            _, query = self._parse()
        explicit = None
        if "namespace" in query and query["namespace"]:
            explicit = query["namespace"][0]
        elif "ns" in query and query["ns"]:
            explicit = query["ns"][0]
        else:
            explicit = self.headers.get(NAMESPACE_HEADER, "") or ""
        return namespace_manager.resolve(explicit)

    def _namespace_or_400(self, query: Optional[Dict[str, list[str]]] = None) -> Optional[str]:
        namespace = self._namespace_from_query(query)
        if namespace is not None:
            return namespace
        self._send(400, "Invalid namespace")
        return None

    def _with_namespace_headers(self, headers: Optional[Dict[str, str]], namespace: Optional[str]) -> Dict[str, str]:
        out = dict(headers or {})
        if namespace is not None:
            out[NAMESPACE_HEADER] = namespace
        return out

    def _ns_key(self, namespace: Optional[str], key: Any) -> Any:
        ns = namespace_manager.resolve(namespace)
        return namespace_manager.key(ns or namespace_manager.default(), key)

    def _ns_strip(self, namespace: Optional[str], key: Any) -> Any:
        ns = namespace_manager.resolve(namespace)
        return namespace_manager.strip(ns or namespace_manager.default(), key)

    def _ns_prefix(self, namespace: Optional[str], prefix: Optional[str]) -> Optional[str]:
        ns = namespace_manager.resolve(namespace)
        return namespace_manager.user_prefix(ns or namespace_manager.default(), prefix)

    def _hot_key_report_for_namespace(self, namespace: str, limit: Optional[int] = None) -> Dict[str, Any]:
        top_k = namespace_manager.hot_key_top_k(namespace) if limit is None else max(1, int(limit))
        threshold_qps = namespace_manager.hot_key_threshold_qps(namespace)
        return STORE._hotkeys.report(
            limit=top_k,
            threshold_qps=threshold_qps,
            key_filter=lambda k, ns=namespace: namespace_manager.belongs(ns, k),
            key_mapper=lambda k, ns=namespace: namespace_manager.strip(ns, k),
            namespace=namespace,
        )

    def _hot_key_namespace_reports(self, limit: Optional[int] = None) -> Dict[str, Any]:
        reports = [self._hot_key_report_for_namespace(ns, limit=limit) for ns in namespace_manager.known_namespaces()]
        return {
            "enabled": STORE._hotkeys.is_enabled(),
            "namespaces": reports,
            "namespace_count": len(reports),
        }

    def _client_ip(self) -> str:
        xff = self.headers.get("X-Forwarded-For", "") or ""
        if xff.strip():
            return xff.split(",")[0].strip()
        try:
            return str(self.client_address[0])
        except Exception:
            return ""

    def _rate_limit(self, route: str, namespace: Optional[str] = None) -> bool:
        if route in (
            "GET /admin/config",
            "POST /admin/config",
            "POST /admin/config/reload",
            "GET /admin/hotkeys",
            "POST /admin/hotkeys/reset",
            "GET /admin/heavy-hitters",
            "POST /admin/heavy-hitters/reset",
            "POST /admin/query-plan-cache/reset",
            "GET /admin/adaptive-ttl",
            "POST /admin/adaptive-ttl/reset",
        ):
            return True
        ns = namespace_manager.resolve(namespace)
        if ns is None:
            self._send(400, "Invalid namespace")
            self._inc_metrics(self.command or "GET", route=route, error=True)
            return False
        ns_cfg = namespace_manager.config(ns)
        ns_rate_enabled = "rate_limit_default" in ns_cfg or "rate_limit_routes" in ns_cfg
        ok, retry_after_s = _RATE_LIMITER.allow(
            route,
            self._client_ip(),
            enabled=bool(getattr(settings, "RATE_LIMIT_ENABLED", False) or ns_rate_enabled),
            scope=namespace_manager.scope(ns),
            default_policy=namespace_manager.rate_limit_default(ns),
            route_policies=namespace_manager.rate_limit_routes(ns),
        )
        if ok:
            return True
        self._json(
            429,
            {"error": "rate_limited", "route": route, "retry_after_seconds": retry_after_s, "namespace": ns},
            headers={"Retry-After": str(retry_after_s), NAMESPACE_HEADER: ns},
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
        span = getattr(self, "_span", None)
        if span is not None:
            tracing.set_attribute(span, "http.status_code", int(code))
            if int(code) >= 400:
                tracing.set_status_error(span, f"HTTP {int(code)}")
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

    def _enforce_disk_write_budget(self, route: str) -> bool:
        decision = disk_throttler.gate_write()
        registry.observe_disk_usage(decision)
        delay_ms = float(decision.get("delay_ms", 0.0) or 0.0)
        if delay_ms > 0 and not decision.get("rejected", False):
            registry.inc_disk_throttle(delay_ms)
        if not decision.get("rejected", False):
            return True

        reason = str(decision.get("reason", "") or "disk usage threshold exceeded")
        registry.inc_disk_reject(reason)
        body = {
            "error": "disk_throttled",
            "reason": reason,
            "path": decision.get("last_path", ""),
            "used_percent": float(decision.get("used_percent", 0.0) or 0.0),
            "used_bytes": int(decision.get("used_bytes", 0) or 0),
            "free_bytes": int(decision.get("free_bytes", 0) or 0),
        }
        self._json(507, body)
        self._inc_metrics(self.command or "WRITE", route=route, error=True)
        return False

    def _auth_enabled(self, namespace: Optional[str] = None) -> bool:
        return namespace_manager.auth_enabled(namespace)

    def _auth_role(self, namespace: Optional[str] = None) -> Optional[str]:
        authorization = self.headers.get("Authorization", "") or ""
        token = parse_bearer(authorization) or (self.headers.get("X-Auth-Token", "") or "")
        password = parse_basic_password(authorization) or (self.headers.get("X-Auth-Password", "") or "")

        if token:
            role = namespace_manager.role_for_secret(namespace, token)
            if role:
                return role

        if password:
            role = namespace_manager.role_for_secret(namespace, password)
            if role:
                return role

        return None

    def _require_role(self, required: str, namespace: Optional[str] = None) -> bool:
        if not self._auth_enabled(namespace):
            return True
        role = self._auth_role(namespace)
        if role is None:
            self._send(
                401,
                "Unauthorized",
                headers={
                    "WWW-Authenticate": 'Bearer realm="pxkv", charset="UTF-8"',
                    **self._with_namespace_headers({}, namespace_manager.resolve(namespace)),
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
        for k in ("Authorization", "X-Auth-Token", "X-Auth-Password", NAMESPACE_HEADER):
            v = self.headers.get(k)
            if v:
                headers[k] = v
        return headers

    def _http_get_bytes(self, url: str, headers: Dict[str, str], timeout: float) -> tuple[int, bytes, Dict[str, str]]:
        tracing.inject_headers(headers)
        with tracing.start_span(
            "http get follower-proxy",
            attributes={"http.method": "GET", "http.url": url},
            kind="client",
        ) as span:
            req = urllib.request.Request(url, headers=headers, method="GET")
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    status = int(getattr(resp, "status", 0) or 0)
                    body = resp.read()
                    tracing.set_attribute(span, "http.status_code", status)
                    return status, body, dict(resp.headers.items())
            except urllib.error.HTTPError as e:
                try:
                    body = e.read()
                except Exception:
                    body = b""
                code = int(getattr(e, "code", 500))
                tracing.set_attribute(span, "http.status_code", code)
                tracing.set_status_error(span, f"HTTP {code}")
                return code, body, dict(getattr(e, "headers", {}) or {})
            except Exception as e:
                tracing.set_status_error(span, str(e))
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
        vector_state = data.get("_vectors")
        stream_state = data.get("_streams")
        shard_items = [(k, v) for k, v in data.items() if str(k).isdigit()]
        for idx_str, shard_state in sorted(shard_items, key=lambda kv: int(kv[0])):
            _write_line({"shard": int(idx_str), "state": shard_state})
        if isinstance(vector_state, dict):
            _write_line({"vectors": vector_state})
        if isinstance(stream_state, dict):
            _write_line({"streams": stream_state})

        try:
            if compress:
                out.close()
        finally:
            writer.wfile.write(b"0\r\n\r\n")
            writer.flush()

    def _request_span(self, method: str):
        return tracing.start_span(
            f"http {method.lower()}",
            attributes={
                "http.method": method,
                "http.target": self.path,
                "http.scheme": "http",
                "net.peer.ip": self._client_ip(),
                "pxkv.request_id": getattr(self, "_request_id", ""),
            },
            kind="server",
        )

    def _inc_metrics(self, method: str, route: str = "", error: bool = False) -> None:
        registry.inc_requests(method, error)
        span = getattr(self, "_span", None)
        if span is not None and route:
            tracing.set_attribute(span, "http.route", route)
            if error:
                tracing.set_attribute(span, "pxkv.error", True)
        if route:
            self._ensure_request_context()
            elapsed_ms = (time.time() - self._request_started_at) * 1000.0
            registry.observe_latency(route, elapsed_ms)

    def do_GET(self) -> None:
        self._request_id = uuid.uuid4().hex
        self._request_started_at = time.time()
        self._fault_sleep()
        with tracing.extract_context(self.headers), self._request_span("GET") as span:
            self._span = span
            self._do_GET_inner()

    def _do_GET_inner(self) -> None:
        try:
            parts, query = self._parse()
            if not parts:
                if not self._rate_limit("GET /"):
                    return
                self._json(200, {"status": "ok"})
                self._inc_metrics("GET", route="GET /")
                return

            if parts == ["events", "keyspace"]:
                namespace = self._namespace_or_400(query)
                if namespace is None:
                    self._inc_metrics("GET", route="GET /events/keyspace", error=True)
                    return
                if not self._rate_limit("GET /events/keyspace", namespace=namespace):
                    return
                if not self._require_role(ROLE_READER, namespace=namespace):
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.send_header(NAMESPACE_HEADER, namespace)
                self.end_headers()
                sid, q = notifier.subscribe()
                try:
                    while True:
                        try:
                            ev = q.get(timeout=15.0)
                            if not namespace_manager.belongs(namespace, ev.key):
                                continue
                            payload = {
                                "op": ev.op,
                                "key": self._ns_strip(namespace, ev.key),
                                "lsn": ev.lsn,
                                "shard": ev.shard,
                                "ts": ev.ts,
                            }
                            self.wfile.write(b"event: keyspace\n")
                            self.wfile.write(f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8"))
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

            if parts == ["gossip", "membership"]:
                if not self._rate_limit("GET /gossip/membership"):
                    return
                gossip = get_gossip_membership()
                if not gossip:
                    self._json(200, {"enabled": False, "peers": [], "alive_peers": []})
                    self._inc_metrics("GET", route="GET /gossip/membership")
                    return
                with gossip._lock:
                    peers_list = []
                    for addr, peer in gossip.peers.items():
                        peers_list.append({
                            "addr": addr,
                            "alive": peer.is_alive,
                            "incarnation": peer.incarnation,
                            "last_seen": peer.last_seen
                        })
                self._json(200, {
                    "enabled": True,
                    "self_addr": gossip.self_addr,
                    "peers": peers_list,
                    "alive_peers": gossip.get_alive_peers(),
                })
                self._inc_metrics("GET", route="GET /gossip/membership")
                return

            if parts[0] == "admin":
                if not self._require_role(ROLE_ADMIN):
                    return
                self._handle_admin_get(parts[1:], query)
                return

            if parts[0] == "vector":
                namespace = self._namespace_or_400(query)
                if namespace is None:
                    self._inc_metrics("GET", route="GET /vector/:key", error=True)
                    return
                if not self._rate_limit("GET /vector/:key", namespace=namespace):
                    return
                if not self._require_role(ROLE_READER, namespace=namespace):
                    return
                if len(parts) == 2 and parts[1] == "stats":
                    self._json(200, STORE.vector_stats(), headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("GET", route="GET /vector/stats")
                    return
                if len(parts) != 2 or not parts[1]:
                    raise ValueError
                key = parts[1]
                vector = STORE.vector_get(self._ns_key(namespace, key))
                if vector is None:
                    raise KeyError(key)
                self._json(200, {"key": key, "vector": vector}, headers=self._with_namespace_headers({}, namespace))
                self._inc_metrics("GET", route="GET /vector/:key")
                return

            if parts[0] == "streams":
                namespace = self._namespace_or_400(query)
                if namespace is None:
                    self._inc_metrics("GET", route="GET /streams", error=True)
                    return
                if not self._require_role(ROLE_READER, namespace=namespace):
                    return
                if len(parts) == 2 and parts[1] == "stats":
                    if not self._rate_limit("GET /streams/stats", namespace=namespace):
                        return
                    self._json(200, STORE.stream_stats(), headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("GET", route="GET /streams/stats")
                    return
                if len(parts) == 3 and parts[2] == "range":
                    if not self._rate_limit("GET /streams/:key/range", namespace=namespace):
                        return
                    start = query.get("start", ["-"])[0]
                    end = query.get("end", ["+"])[0]
                    count = None
                    if "count" in query:
                        try:
                            count = int(query["count"][0])
                        except ValueError:
                            self._send(400, "count must be int")
                            self._inc_metrics("GET", route="GET /streams/:key/range", error=True)
                            return
                    entries = STORE.stream_xrange(self._ns_key(namespace, parts[1]), start=start, end=end, count=count)
                    self._json(200, {"stream": parts[1], "entries": entries}, headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("GET", route="GET /streams/:key/range")
                    return
                if len(parts) == 5 and parts[2] == "groups" and parts[4] == "pending":
                    if not self._rate_limit("GET /streams/:key/groups/:group/pending", namespace=namespace):
                        return
                    summary = STORE.stream_xpending(self._ns_key(namespace, parts[1]), parts[3])
                    self._json(200, summary, headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("GET", route="GET /streams/:key/groups/:group/pending")
                    return
                raise ValueError

            if parts[0] == "ai":
                namespace = self._namespace_or_400(query)
                if namespace is None:
                    self._inc_metrics("GET", route="GET /ai/cache/:key", error=True)
                    return
                if not self._rate_limit("GET /ai/cache/:key", namespace=namespace):
                    return
                if not self._require_role(ROLE_READER, namespace=namespace):
                    return
                if len(parts) >= 2 and parts[1] == "cache":
                    if len(parts) != 3 or not parts[2]:
                        raise ValueError
                    cache_key = parts[2]
                    storage_key = self._ns_key(namespace, f"ai:cache:{cache_key}")
                    value = STORE.read(storage_key)
                    headers = self._with_namespace_headers(self._staleness_headers(), namespace)
                    self._json(200, {"key": cache_key, "value": value}, headers=headers)
                    self._inc_metrics("GET", route="GET /ai/cache/:key")
                    return
                raise ValueError

            if parts[0] != "kv":
                raise ValueError

            namespace = self._namespace_or_400(query)
            if namespace is None:
                self._inc_metrics("GET", route="GET /kv (invalid_namespace)", error=True)
                return

            if len(parts) >= 2 and parts[1] == "batch":
                if not self._rate_limit("GET /kv/batch", namespace=namespace):
                    return
            elif len(parts) >= 2 and parts[1] == "scan":
                if not self._rate_limit("GET /kv/scan", namespace=namespace):
                    return
            else:
                if not self._rate_limit("GET /kv/:key", namespace=namespace):
                    return

            if parts and parts[0] == "kv":
                if self._maybe_route_read_to_follower(parts, query):
                    return

            if not self._require_role(ROLE_READER, namespace=namespace):
                return
            if len(parts) >= 2 and parts[1] == "batch":
                if "keys" not in query:
                    self._send(400, "keys query param required")
                    self._inc_metrics("GET", route="GET /kv/batch", error=True)
                    return
                keys = query["keys"][0].split(",")
                prefixed = [self._ns_key(namespace, k) for k in keys]
                raw_items = STORE.mget(prefixed)
                scoped_items = {
                    str(self._ns_strip(namespace, stored_key)): value
                    for stored_key, value in raw_items.items()
                }
                extra = getattr(self, "_fallback_headers", None)
                headers = self._with_namespace_headers(self._staleness_headers(), namespace)
                if isinstance(extra, dict):
                    headers = {**headers, **extra}
                    self._fallback_headers = None
                self._json(200, scoped_items, headers=headers)
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
                plan = query_plan_cache.scan_plan(
                    namespace=namespace,
                    prefix=prefix,
                    start_after=start_after,
                    cursor=None,
                    limit=limit,
                    cursor_mode=False,
                    ns_prefix_fn=self._ns_prefix,
                    ns_key_fn=self._ns_key,
                )
                keys = STORE.scan(
                    prefix=plan.storage_prefix,
                    limit=plan.limit,
                    start_after=plan.storage_start_after,
                )
                keys = [str(self._ns_strip(namespace, key)) for key in keys]
                extra = getattr(self, "_fallback_headers", None)
                headers = self._with_namespace_headers(self._staleness_headers(), namespace)
                if isinstance(extra, dict):
                    headers = {**headers, **extra}
                    self._fallback_headers = None
                self._json(200, {"keys": keys}, headers=headers)
                self._inc_metrics("GET", route="GET /kv/scan")
                return

            if len(parts) >= 2 and parts[1] == "scan-cursor":
                prefix = None
                cursor = None
                limit = 100
                if "prefix" in query:
                    prefix = query["prefix"][0]
                if "cursor" in query:
                    cursor = query["cursor"][0]
                if "limit" in query:
                    try:
                        limit = int(query["limit"][0])
                    except ValueError:
                        self._send(400, "limit must be int")
                        self._inc_metrics("GET", route="GET /kv/scan-cursor", error=True)
                        return
                plan = query_plan_cache.scan_plan(
                    namespace=namespace,
                    prefix=prefix,
                    start_after=None,
                    cursor=cursor,
                    limit=limit,
                    cursor_mode=True,
                    ns_prefix_fn=self._ns_prefix,
                    ns_key_fn=self._ns_key,
                )
                next_cursor, keys = STORE.scan_with_cursor(
                    cursor=plan.cursor,
                    prefix=plan.storage_prefix,
                    limit=plan.limit,
                )
                keys = [str(self._ns_strip(namespace, key)) for key in keys]
                extra = getattr(self, "_fallback_headers", None)
                headers = self._with_namespace_headers(self._staleness_headers(), namespace)
                if isinstance(extra, dict):
                    headers = {**headers, **extra}
                    self._fallback_headers = None
                self._json(200, {"cursor": next_cursor, "keys": keys}, headers=headers)
                self._inc_metrics("GET", route="GET /kv/scan-cursor")
                return

            if len(parts) < 2:
                raise ValueError

            key = parts[1]
            storage_key = self._ns_key(namespace, key)
            value, etag = STORE.read_with_etag(storage_key)
            
            if_none_match = self.headers.get("If-None-Match", "")
            if if_none_match == etag or (if_none_match == "*"):
                extra = getattr(self, "_fallback_headers", None)
                headers = self._with_namespace_headers(self._staleness_headers(), namespace)
                if isinstance(extra, dict):
                    headers = {**headers, **extra}
                    self._fallback_headers = None
                headers["ETag"] = etag
                self._send(304, headers=headers)
                self._inc_metrics("GET", route="GET /kv/:key (not_modified)")
                return

            extra = getattr(self, "_fallback_headers", None)
            headers = self._with_namespace_headers(self._staleness_headers(), namespace)
            if isinstance(extra, dict):
                headers = {**headers, **extra}
                self._fallback_headers = None
            headers["ETag"] = etag
            self._json(200, {"key": key, "value": value}, headers=headers)
            self._inc_metrics("GET", route="GET /kv/:key")
        except KeyError as e:
            extra = getattr(self, "_fallback_headers", None)
            namespace = self._namespace_from_query(query)
            headers = self._with_namespace_headers(self._staleness_headers(), namespace)
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
            namespace = self._namespace_from_query(query)
            headers = self._with_namespace_headers(self._staleness_headers(), namespace)
            if isinstance(extra, dict):
                headers = {**headers, **extra}
                self._fallback_headers = None
            self._send(404, "Not Found", headers=headers)
            self._inc_metrics("GET", route="GET (not_found)", error=True)

    def do_PUT(self) -> None:
        self._request_id = uuid.uuid4().hex
        self._request_started_at = time.time()
        self._fault_sleep()
        with tracing.extract_context(self.headers), self._request_span("PUT") as span:
            self._span = span
            self._do_PUT_inner()

    def _do_PUT_inner(self) -> None:
        try:
            parts, query = self._parse()
            namespace = self._namespace_or_400(query)
            if namespace is None:
                self._inc_metrics("PUT", route="PUT /kv/:key", error=True)
                return
            if not self._rate_limit("PUT /kv/:key", namespace=namespace):
                return
            if not self._require_role(ROLE_WRITER, namespace=namespace):
                return
            if settings.REPLICATION_ROLE == "follower":
                self._reject_readonly(route="PUT /kv/:key")
                return
            if not self._enforce_disk_write_budget("PUT /kv/:key"):
                return
            if len(parts) != 2 or parts[0] != "kv" or parts[1] == "":
                raise ValueError
            key = parts[1]
            storage_key = self._ns_key(namespace, key)
            ttl = float(query["ttl"][0]) if "ttl" in query else None

            raw = self._body()
            try:
                value = json.loads(raw or b"")
            except ValueError:
                value = raw

            if storage_key in STORE.mget([storage_key]):
                STORE.update(storage_key, value, ttl)
                self._send(204, headers=self._with_namespace_headers({}, namespace))
            else:
                STORE.create(storage_key, value, ttl)
                self._send(201, headers=self._with_namespace_headers({}, namespace))
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
        with tracing.extract_context(self.headers), self._request_span("DELETE") as span:
            self._span = span
            self._do_DELETE_inner()

    def _do_DELETE_inner(self) -> None:
        try:
            parts, query = self._parse()
            namespace = self._namespace_or_400(query)
            if namespace is None:
                self._inc_metrics("DELETE", route="DELETE /kv/:key", error=True)
                return
            if not self._rate_limit("DELETE /kv/:key", namespace=namespace):
                return
            if not self._require_role(ROLE_WRITER, namespace=namespace):
                return
            if settings.REPLICATION_ROLE == "follower":
                self._reject_readonly(route="DELETE /kv/:key")
                return
            if len(parts) == 2 and parts[0] == "vector" and parts[1] != "":
                deleted = STORE.vector_delete(self._ns_key(namespace, parts[1]))
                if not deleted:
                    raise KeyError(parts[1])
                self._send(204, headers=self._with_namespace_headers({}, namespace))
                self._inc_metrics("DELETE", route="DELETE /vector/:key")
                return
            if len(parts) == 2 and parts[0] == "streams" and parts[1] != "":
                deleted = STORE.stream_delete(self._ns_key(namespace, parts[1]))
                if not deleted:
                    raise KeyError(parts[1])
                self._send(204, headers=self._with_namespace_headers({}, namespace))
                self._inc_metrics("DELETE", route="DELETE /streams/:key")
                return
            if len(parts) != 2 or parts[0] != "kv" or parts[1] == "":
                raise ValueError
            STORE.delete(self._ns_key(namespace, parts[1]))
            self._send(204, headers=self._with_namespace_headers({}, namespace))
            self._inc_metrics("DELETE", route="DELETE /kv/:key")
        except KeyError as e:
            self._send(404, str(e))
            self._inc_metrics("DELETE", route="DELETE /kv/:key", error=True)
        except ValueError:
            self._send(404, "Not Found")
            self._inc_metrics("DELETE", route="DELETE (not_found)", error=True)

    def do_PATCH(self) -> None:
        self._request_id = uuid.uuid4().hex
        self._request_started_at = time.time()
        self._fault_sleep()
        with tracing.extract_context(self.headers), self._request_span("PATCH") as span:
            self._span = span
            self._do_PATCH_inner()

    def _do_PATCH_inner(self) -> None:
        try:
            parts, query = self._parse()
            namespace = self._namespace_or_400(query)
            if namespace is None:
                self._inc_metrics("PATCH", route="PATCH /kv/:key", error=True)
                return
            if not self._rate_limit("PATCH /kv/:key", namespace=namespace):
                return
            if not self._require_role(ROLE_WRITER, namespace=namespace):
                return
            if settings.REPLICATION_ROLE == "follower":
                self._reject_readonly(route="PATCH /kv/:key")
                return
            if not self._enforce_disk_write_budget("PATCH /kv/:key"):
                return
            if len(parts) != 2 or parts[0] != "kv" or parts[1] == "":
                raise ValueError
            key = parts[1]
            storage_key = self._ns_key(namespace, key)
            ttl = float(query["ttl"][0]) if "ttl" in query else None
            raw = self._body()
            try:
                patches = json.loads(raw or b"")
            except ValueError:
                self._send(400, "Invalid JSON Patch payload")
                self._inc_metrics("PATCH", route="PATCH /kv/:key", error=True)
                return
            if not isinstance(patches, list):
                self._send(400, "JSON Patch payload must be an array of operations")
                self._inc_metrics("PATCH", route="PATCH /kv/:key", error=True)
                return
            new_value, etag = STORE.patch(storage_key, patches, ttl)
            headers = self._with_namespace_headers(self._staleness_headers(), namespace)
            headers["ETag"] = etag
            self._json(200, {"key": key, "value": new_value}, headers=headers)
            self._inc_metrics("PATCH", route="PATCH /kv/:key")
        except KeyError as e:
            self._send(404, str(e))
            self._inc_metrics("PATCH", route="PATCH /kv/:key", error=True)
        except ValueError:
            self._send(404, "Not Found")
            self._inc_metrics("PATCH", route="PATCH (not_found)", error=True)
        except Exception as e:
            self._send(400, str(e))
            self._inc_metrics("PATCH", route="PATCH /kv/:key", error=True)

    def do_POST(self) -> None:
        self._request_id = uuid.uuid4().hex
        self._request_started_at = time.time()
        self._fault_sleep()
        with tracing.extract_context(self.headers), self._request_span("POST") as span:
            self._span = span
            self._do_POST_inner()

    def _do_POST_inner(self) -> None:
        try:
            parts, query = self._parse()
            
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

            if parts == ["gossip", "membership"]:
                if not self._rate_limit("POST /gossip/membership"):
                    return
                gossip = get_gossip_membership()
                if not gossip:
                    self._json(200, {"status": "gossip not enabled"})
                    self._inc_metrics("POST", route="POST /gossip/membership")
                    return
                try:
                    payload = json.loads(self._body() or b"{}")
                except ValueError:
                    self._send(400, "Invalid JSON payload")
                    self._inc_metrics("POST", route="POST /gossip/membership", error=True)
                    return
                
                # First, if sender is a peer we don't know, add them
                sender_addr = payload.get("addr")
                if sender_addr and sender_addr != gossip.self_addr:
                    gossip.add_peer(sender_addr)
                
                # Now process their peer list
                their_peers = payload.get("peers", {})
                if isinstance(their_peers, dict):
                    for addr, peer_info in their_peers.items():
                        if addr == gossip.self_addr:
                            continue
                        # Check if this is a new peer
                        is_alive = bool(peer_info.get("alive", True))
                        incarnation = int(peer_info.get("incarnation", 0))
                        gossip.update_peer(addr, is_alive, incarnation)
                
                # Return our own membership state
                with gossip._lock:
                    peers_list = []
                    for addr, peer in gossip.peers.items():
                        peers_list.append({
                            "addr": addr,
                            "alive": peer.is_alive,
                            "incarnation": peer.incarnation,
                            "last_seen": peer.last_seen
                        })
                
                self._json(200, {
                    "addr": gossip.self_addr,
                    "peers": {
                        addr: {
                            "alive": peer.is_alive,
                            "incarnation": peer.incarnation,
                            "last_seen": peer.last_seen
                        } for addr, peer in gossip.peers.items()
                    }
                })
                self._inc_metrics("POST", route="POST /gossip/membership")
                return

            if len(parts) >= 1 and parts[0] == "ai":
                if parts == ["ai", "cache", "lookup"]:
                    namespace = self._namespace_or_400(query)
                    if namespace is None:
                        self._inc_metrics("POST", route="POST /ai/cache/lookup", error=True)
                        return
                    if not self._rate_limit("POST /ai/cache/lookup", namespace=namespace):
                        return
                    if not self._require_role(ROLE_READER, namespace=namespace):
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
                    storage_key = self._ns_key(namespace, f"ai:cache:{key}")
                    registry.inc_ai_cache("lookups")
                    try:
                        cached = STORE.read(storage_key)
                        cached = ai_cache_manager.decompress_value(cached)
                    except KeyError:
                        registry.inc_ai_cache("misses")
                        self._json(200, {"hit": False, "key": key, "canonical": canon}, headers=self._with_namespace_headers({}, namespace))
                        self._inc_metrics("POST", route="POST /ai/cache/lookup")
                        return
                    registry.inc_ai_cache("hits")
                    self._json(200, {"hit": True, "key": key, "canonical": canon, "value": cached}, headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("POST", route="POST /ai/cache/lookup")
                    return

                if parts == ["ai", "cache"]:
                    namespace = self._namespace_or_400(query)
                    if namespace is None:
                        self._inc_metrics("POST", route="POST /ai/cache", error=True)
                        return
                    if not self._rate_limit("POST /ai/cache", namespace=namespace):
                        return
                    if not self._require_role(ROLE_WRITER, namespace=namespace):
                        return
                    if settings.REPLICATION_ROLE == "follower":
                        self._reject_readonly(route="POST /ai/cache")
                        return
                    if not self._enforce_disk_write_budget("POST /ai/cache"):
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
                    storage_key = self._ns_key(namespace, f"ai:cache:{key}")
                    if compress:
                        value = ai_cache_manager.compress_value(value)
                    if storage_key in STORE.mget([storage_key]):
                        STORE.update(storage_key, value, ttl_f)
                    else:
                        STORE.create(storage_key, value, ttl_f)
                    registry.inc_ai_cache("stores")
                    self._json(201, {"key": key, "canonical": canon}, headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("POST", route="POST /ai/cache")
                    return

            if len(parts) >= 1 and parts[0] == "vector":
                if parts == ["vector", "search"]:
                    namespace = self._namespace_or_400(query)
                    if namespace is None:
                        self._inc_metrics("POST", route="POST /vector/search", error=True)
                        return
                    if not self._rate_limit("POST /vector/search", namespace=namespace):
                        return
                    if not self._require_role(ROLE_READER, namespace=namespace):
                        return
                    payload = json.loads(self._body() or b"{}")
                    vector = payload.get("vector")
                    k = payload.get("k", 10)
                    ef = payload.get("ef")
                    include_values = bool(payload.get("include_values", False))
                    if not isinstance(vector, list):
                        self._send(400, "vector must be an array")
                        self._inc_metrics("POST", route="POST /vector/search", error=True)
                        return
                    try:
                        k_i = int(k)
                        ef_i = None if ef is None else int(ef)
                        results = STORE.vector_search(vector, k=k_i, ef=ef_i, include_values=include_values)
                    except ValueError as e:
                        self._send(400, str(e))
                        self._inc_metrics("POST", route="POST /vector/search", error=True)
                        return
                    scoped = []
                    ns_prefix = self._ns_prefix(namespace)
                    for item in results:
                        key = item.get("key")
                        if isinstance(key, str) and key.startswith(ns_prefix):
                            entry = dict(item)
                            entry["key"] = self._ns_strip(namespace, key)
                            scoped.append(entry)
                    self._json(200, {"results": scoped}, headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("POST", route="POST /vector/search")
                    return

                if parts == ["vector", "upsert"]:
                    namespace = self._namespace_or_400(query)
                    if namespace is None:
                        self._inc_metrics("POST", route="POST /vector/upsert", error=True)
                        return
                    if not self._rate_limit("POST /vector/upsert", namespace=namespace):
                        return
                    if not self._require_role(ROLE_WRITER, namespace=namespace):
                        return
                    if settings.REPLICATION_ROLE == "follower":
                        self._reject_readonly(route="POST /vector/upsert")
                        return
                    if not self._enforce_disk_write_budget("POST /vector/upsert"):
                        return
                    payload = json.loads(self._body() or b"{}")
                    key = payload.get("key")
                    vector = payload.get("vector")
                    ttl = payload.get("ttl")
                    value_supplied = "value" in payload
                    value = payload.get("value")
                    metadata = payload.get("metadata")
                    if not isinstance(key, str) or not key:
                        self._send(400, "key must be a non-empty string")
                        self._inc_metrics("POST", route="POST /vector/upsert", error=True)
                        return
                    if not isinstance(vector, list):
                        self._send(400, "vector must be an array")
                        self._inc_metrics("POST", route="POST /vector/upsert", error=True)
                        return
                    ttl_f = None
                    if ttl is not None:
                        try:
                            ttl_f = float(ttl)
                        except (TypeError, ValueError):
                            self._send(400, "ttl must be numeric")
                            self._inc_metrics("POST", route="POST /vector/upsert", error=True)
                            return
                    storage_key = self._ns_key(namespace, key)
                    if not value_supplied:
                        value = {"embedding": vector, "metadata": metadata or {}}
                    exists = storage_key in STORE.mget([storage_key])
                    try:
                        info = STORE.vector_upsert(storage_key, vector)
                    except ValueError as e:
                        self._send(400, str(e))
                        self._inc_metrics("POST", route="POST /vector/upsert", error=True)
                        return
                    if exists:
                        STORE.update(storage_key, value, ttl_f)
                        status = 200
                    else:
                        STORE.create(storage_key, value, ttl_f)
                        status = 201
                    self._json(
                        status,
                        {"key": key, "dimension": info["dimension"]},
                        headers=self._with_namespace_headers({}, namespace),
                    )
                    self._inc_metrics("POST", route="POST /vector/upsert")
                    return

            if len(parts) >= 1 and parts[0] == "streams":
                namespace = self._namespace_or_400(query)
                if namespace is None:
                    self._inc_metrics("POST", route="POST /streams", error=True)
                    return
                if parts == ["streams", "read"]:
                    if not self._rate_limit("POST /streams/read", namespace=namespace):
                        return
                    if not self._require_role(ROLE_READER, namespace=namespace):
                        return
                    payload = json.loads(self._body() or b"{}")
                    raw_streams = payload.get("streams", {})
                    if not isinstance(raw_streams, dict):
                        self._send(400, "streams must be object")
                        self._inc_metrics("POST", route="POST /streams/read", error=True)
                        return
                    count = payload.get("count")
                    block_ms = int(payload.get("block_ms", 0) or 0)
                    scoped_streams = {self._ns_key(namespace, k): str(v) for k, v in raw_streams.items()}
                    result = STORE.stream_xread(scoped_streams, count=None if count is None else int(count), block_ms=block_ms)
                    out = {
                        str(self._ns_strip(namespace, key)): entries
                        for key, entries in result.items()
                    }
                    self._json(200, {"streams": out}, headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("POST", route="POST /streams/read")
                    return
                if not self._require_role(ROLE_WRITER, namespace=namespace):
                    return
                if settings.REPLICATION_ROLE == "follower":
                    self._reject_readonly(route="POST /streams")
                    return
                if not self._enforce_disk_write_budget("POST /streams"):
                    return
                payload = json.loads(self._body() or b"{}")

                if len(parts) == 2 and parts[1]:
                    if not self._rate_limit("POST /streams/:key", namespace=namespace):
                        return
                    fields = payload.get("fields", {})
                    entry_id = str(payload.get("id", "*"))
                    maxlen = payload.get("maxlen")
                    if not isinstance(fields, dict) or not fields:
                        self._send(400, "fields must be non-empty object")
                        self._inc_metrics("POST", route="POST /streams/:key", error=True)
                        return
                    try:
                        new_id = STORE.stream_xadd(
                            self._ns_key(namespace, parts[1]),
                            fields,
                            entry_id=entry_id,
                            maxlen=None if maxlen is None else int(maxlen),
                        )
                    except ValueError as e:
                        self._send(400, str(e))
                        self._inc_metrics("POST", route="POST /streams/:key", error=True)
                        return
                    self._json(201, {"stream": parts[1], "id": new_id}, headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("POST", route="POST /streams/:key")
                    return

                if len(parts) == 3 and parts[2] == "groups":
                    if not self._rate_limit("POST /streams/:key/groups", namespace=namespace):
                        return
                    group = str(payload.get("group", ""))
                    entry_id = str(payload.get("id", "$"))
                    mkstream = bool(payload.get("mkstream", False))
                    try:
                        STORE.stream_xgroup_create(
                            self._ns_key(namespace, parts[1]),
                            group,
                            entry_id=entry_id,
                            mkstream=mkstream,
                        )
                    except KeyError as e:
                        self._send(404, str(e))
                        self._inc_metrics("POST", route="POST /streams/:key/groups", error=True)
                        return
                    except ValueError as e:
                        self._send(400, str(e))
                        self._inc_metrics("POST", route="POST /streams/:key/groups", error=True)
                        return
                    self._json(201, {"stream": parts[1], "group": group}, headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("POST", route="POST /streams/:key/groups")
                    return

                if len(parts) == 5 and parts[2] == "groups" and parts[4] == "read":
                    if not self._rate_limit("POST /streams/:key/groups/:group/read", namespace=namespace):
                        return
                    consumer = str(payload.get("consumer", ""))
                    entry_id = str(payload.get("id", ">"))
                    count = payload.get("count")
                    block_ms = int(payload.get("block_ms", 0) or 0)
                    if not consumer:
                        self._send(400, "consumer must be non-empty")
                        self._inc_metrics("POST", route="POST /streams/:key/groups/:group/read", error=True)
                        return
                    try:
                        entries = STORE.stream_xreadgroup(
                            self._ns_key(namespace, parts[1]),
                            parts[3],
                            consumer,
                            entry_id=entry_id,
                            count=None if count is None else int(count),
                            block_ms=block_ms,
                        )
                    except KeyError as e:
                        self._send(404, str(e))
                        self._inc_metrics("POST", route="POST /streams/:key/groups/:group/read", error=True)
                        return
                    self._json(200, {"stream": parts[1], "group": parts[3], "entries": entries}, headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("POST", route="POST /streams/:key/groups/:group/read")
                    return

                if len(parts) == 5 and parts[2] == "groups" and parts[4] == "ack":
                    if not self._rate_limit("POST /streams/:key/groups/:group/ack", namespace=namespace):
                        return
                    ids = payload.get("ids", [])
                    if not isinstance(ids, list):
                        self._send(400, "ids must be array")
                        self._inc_metrics("POST", route="POST /streams/:key/groups/:group/ack", error=True)
                        return
                    count = STORE.stream_xack(self._ns_key(namespace, parts[1]), parts[3], [str(entry_id) for entry_id in ids])
                    self._json(200, {"acked": count}, headers=self._with_namespace_headers({}, namespace))
                    self._inc_metrics("POST", route="POST /streams/:key/groups/:group/ack")
                    return

            if len(parts) >= 3 and parts[0] == "kv" and parts[1] == "incr":
                namespace = self._namespace_or_400(query)
                if namespace is None:
                    self._inc_metrics("POST", route="POST /kv/incr/:key", error=True)
                    return
                if not self._rate_limit("POST /kv/incr/:key", namespace=namespace):
                    return
                if not self._require_role(ROLE_WRITER, namespace=namespace):
                    return
                if settings.REPLICATION_ROLE == "follower":
                    self._reject_readonly(route="POST /kv/incr/:key")
                    return
                if not self._enforce_disk_write_budget("POST /kv/incr/:key"):
                    return
                key = parts[2]
                storage_key = self._ns_key(namespace, key)
                delta = 1.0
                ttl = None
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
                    new_val = STORE.incr(storage_key, delta=delta, ttl=ttl)
                except TypeError as e:
                    self._send(400, str(e))
                    self._inc_metrics("POST", route="POST /kv/incr/:key", error=True)
                    return
                self._json(200, {"key": key, "value": new_val}, headers=self._with_namespace_headers({}, namespace))
                self._inc_metrics("POST", route="POST /kv/incr/:key")
                return
            if parts == ["kv", "batch"]:
                namespace = self._namespace_or_400(query)
                if namespace is None:
                    self._inc_metrics("POST", route="POST /kv/batch", error=True)
                    return
                if not self._rate_limit("POST /kv/batch", namespace=namespace):
                    return
                if not self._require_role(ROLE_WRITER, namespace=namespace):
                    return
                if settings.REPLICATION_ROLE == "follower":
                    self._reject_readonly(route="POST /kv/batch")
                    return
                if not self._enforce_disk_write_budget("POST /kv/batch"):
                    return
                payload = json.loads(self._body() or b"{}")
                items = payload.get("items", {})
                ttl = payload.get("ttl")
                if not isinstance(items, dict):
                    self._send(400, "items must be dict")
                    self._inc_metrics("POST", route="POST /kv/batch", error=True)
                    return
                scoped_items = {self._ns_key(namespace, k): v for k, v in items.items()}
                STORE.mset(scoped_items, ttl)
                self._send(201, headers=self._with_namespace_headers({}, namespace))
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

            if parts == ["admin", "hotkeys", "reset"]:
                if not self._rate_limit("POST /admin/hotkeys/reset"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
                STORE._hotkeys.reset()
                self._json(200, {"status": "ok"})
                self._inc_metrics("POST", route="POST /admin/hotkeys/reset")
                return

            if parts == ["admin", "heavy-hitters", "reset"]:
                if not self._rate_limit("POST /admin/heavy-hitters/reset"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
                STORE._heavy_hitters.reset()
                self._json(200, {"status": "ok"})
                self._inc_metrics("POST", route="POST /admin/heavy-hitters/reset")
                return

            if parts == ["admin", "query-plan-cache", "reset"]:
                if not self._rate_limit("POST /admin/query-plan-cache/reset"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
                query_plan_cache.reset()
                self._json(200, {"status": "ok"})
                self._inc_metrics("POST", route="POST /admin/query-plan-cache/reset")
                return

            if parts == ["admin", "adaptive-ttl", "reset"]:
                if not self._rate_limit("POST /admin/adaptive-ttl/reset"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
                STORE._adaptive_ttl.reset()
                self._json(200, {"status": "ok"})
                self._inc_metrics("POST", route="POST /admin/adaptive-ttl/reset")
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

            if parts == ["admin", "reshard"]:
                if not self._rate_limit("POST /admin/reshard"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
                if not getattr(settings, "RESHARD_ENABLED", True):
                    self._send(403, "Resharding is disabled")
                    self._inc_metrics("POST", route="POST /admin/reshard", error=True)
                    return
                if settings.REPLICATION_ROLE == "follower":
                    self._reject_readonly(route="POST /admin/reshard")
                    return
                if not self._enforce_disk_write_budget("POST /admin/reshard"):
                    return
                payload = json.loads(self._body() or b"{}")
                new_shards = payload.get("shards")
                if not isinstance(new_shards, int) or new_shards < 1:
                    self._send(400, "shards must be an integer >= 1")
                    self._inc_metrics("POST", route="POST /admin/reshard", error=True)
                    return
                result = STORE.reshard(new_shards)
                settings.SHARDS = new_shards
                self._json(200, {"status": "ok", **result})
                self._inc_metrics("POST", route="POST /admin/reshard")
                return

            if parts == ["admin", "pitr", "archives"]:
                if not self._rate_limit("GET /admin/pitr/archives"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
                if not getattr(settings, "PITR_ENABLED", True):
                    self._send(403, "PITR is disabled")
                    self._inc_metrics("GET", route="GET /admin/pitr/archives", error=True)
                    return
                from pxkv.persistence.snapshot import list_snapshot_archives
                archives = list_snapshot_archives(settings.SNAPSHOT_FILE)
                archive_list = []
                for fpath, lsn in archives:
                    try:
                        stat = os.stat(fpath)
                        archive_list.append({
                            "path": fpath,
                            "lsn": lsn,
                            "size_bytes": stat.st_size,
                            "mtime": stat.st_mtime,
                        })
                    except Exception:
                        pass
                self._json(200, {"status": "ok", "archives": archive_list})
                self._inc_metrics("GET", route="GET /admin/pitr/archives")
                return

            if parts == ["admin", "pitr", "restore"]:
                if not self._rate_limit("POST /admin/pitr/restore"):
                    return
                if not self._require_role(ROLE_ADMIN):
                    return
                if not getattr(settings, "PITR_ENABLED", True):
                    self._send(403, "PITR is disabled")
                    self._inc_metrics("POST", route="POST /admin/pitr/restore", error=True)
                    return
                if settings.REPLICATION_ROLE == "follower":
                    self._reject_readonly(route="POST /admin/pitr/restore")
                    return
                if not self._enforce_disk_write_budget("POST /admin/pitr/restore"):
                    return
                payload = json.loads(self._body() or b"{}")
                target_lsn = payload.get("lsn")
                target_ts = payload.get("timestamp")
                if target_lsn is None and target_ts is None:
                    self._send(400, "Either 'lsn' or 'timestamp' must be provided")
                    self._inc_metrics("POST", route="POST /admin/pitr/restore", error=True)
                    return
                from pxkv.persistence.snapshot import recover_to_lsn, recover_to_timestamp
                success = False
                if target_lsn is not None:
                    if not isinstance(target_lsn, int) or target_lsn < 0:
                        self._send(400, "lsn must be a non-negative integer")
                        self._inc_metrics("POST", route="POST /admin/pitr/restore", error=True)
                        return
                    success = recover_to_lsn(STORE, target_lsn, settings.SNAPSHOT_FILE, settings.WAL_FILE)
                else:
                    if not isinstance(target_ts, (int, float)):
                        self._send(400, "timestamp must be a number")
                        self._inc_metrics("POST", route="POST /admin/pitr/restore", error=True)
                        return
                    success = recover_to_timestamp(STORE, float(target_ts), settings.SNAPSHOT_FILE, settings.WAL_FILE)
                if success:
                    self._json(200, {"status": "ok"})
                    self._inc_metrics("POST", route="POST /admin/pitr/restore")
                else:
                    self._send(500, "PITR restore failed")
                    self._inc_metrics("POST", route="POST /admin/pitr/restore", error=True)
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
            disk = disk_throttler.sample(force=True)
            registry.observe_disk_usage(disk)
            self._json(
                200,
                {
                    "status": "ok",
                    "uptime_seconds": time.time() - registry.get_all()["started_at"],
                    "shards": settings.SHARDS,
                    "replication": repl,
                    "disk": disk,
                },
            )
            self._inc_metrics("GET", route="GET /admin/health")
            return
        if parts[0] == "metrics":
            if not self._rate_limit("GET /admin/metrics"):
                return
            registry.observe_hot_keys(STORE._hotkeys.snapshot())
            registry.observe_namespace_hot_keys(self._hot_key_namespace_reports())
            registry.observe_heavy_hitters(STORE._heavy_hitters.snapshot())
            registry.observe_query_plan_cache(query_plan_cache.snapshot())
            fmt = query.get("format", ["json"])[0]
            if fmt == "prometheus":
                prom_data = registry_to_prometheus(registry.get_all())
                self._send(200, prom_data, "text/plain; version=0.0.4; charset=utf-8")
            else:
                self._json(200, registry.get_all())
            self._inc_metrics("GET", route="GET /admin/metrics")
            return
        if parts[0] == "hotkeys":
            if not self._rate_limit("GET /admin/hotkeys"):
                return
            limit_raw = query.get("limit", [None])[0]
            try:
                limit = int(limit_raw) if limit_raw is not None else None
            except (TypeError, ValueError):
                self._send(400, "limit must be int")
                self._inc_metrics("GET", route="GET /admin/hotkeys", error=True)
                return
            if len(parts) > 1 and parts[1] == "namespaces":
                snap = self._hot_key_namespace_reports(limit=limit)
                registry.observe_namespace_hot_keys(snap)
                self._json(200, snap)
                self._inc_metrics("GET", route="GET /admin/hotkeys/namespaces")
                return
            namespace_requested = (
                "namespace" in query
                or "ns" in query
                or bool(self.headers.get(NAMESPACE_HEADER, "") or "")
            )
            if namespace_requested:
                namespace = self._namespace_from_query(query)
                if namespace is None:
                    self._send(400, "Invalid namespace")
                    self._inc_metrics("GET", route="GET /admin/hotkeys", error=True)
                    return
                snap = self._hot_key_report_for_namespace(namespace, limit=limit)
                registry.observe_namespace_hot_keys({"enabled": snap.get("enabled", False), "namespaces": [snap], "namespace_count": 1})
            else:
                snap = STORE._hotkeys.snapshot()
                if limit is not None:
                    snap["top"] = STORE._hotkeys.top_hot_keys(limit=limit)
                registry.observe_hot_keys(snap)
            self._json(200, snap)
            self._inc_metrics("GET", route="GET /admin/hotkeys")
            return
        if parts[0] == "heavy-hitters":
            if not self._rate_limit("GET /admin/heavy-hitters"):
                return
            limit_raw = query.get("limit", [None])[0]
            key_raw = query.get("key", [None])[0]
            try:
                limit = int(limit_raw) if limit_raw is not None else None
            except (TypeError, ValueError):
                self._send(400, "limit must be int")
                self._inc_metrics("GET", route="GET /admin/heavy-hitters", error=True)
                return
            snap = STORE._heavy_hitters.snapshot()
            if limit is not None:
                snap["top"] = STORE._heavy_hitters.top_k(limit=limit)
            if key_raw is not None:
                key = self._ns_key(self._namespace_from_query(query), key_raw)
                snap["query"] = {
                    "key": key_raw,
                    "estimated_count": STORE._heavy_hitters.estimate(key),
                    "hot": STORE._heavy_hitters.is_hot(key),
                }
            registry.observe_heavy_hitters(snap)
            self._json(200, snap)
            self._inc_metrics("GET", route="GET /admin/heavy-hitters")
            return
        if parts[0] == "adaptive-ttl":
            if not self._rate_limit("GET /admin/adaptive-ttl"):
                return
            limit_raw = query.get("limit", [None])[0]
            key_raw = query.get("key", [None])[0]
            try:
                limit = int(limit_raw) if limit_raw is not None else None
            except (TypeError, ValueError):
                self._send(400, "limit must be int")
                self._inc_metrics("GET", route="GET /admin/adaptive-ttl", error=True)
                return
            snap = STORE._adaptive_ttl.snapshot()
            if limit is not None:
                snap["top"] = STORE._adaptive_ttl.top_tracked(limit=limit)
            if key_raw is not None:
                snap["key"] = STORE._adaptive_ttl.key_stats(key_raw)
            self._json(200, snap)
            self._inc_metrics("GET", route="GET /admin/adaptive-ttl")
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
    tracing.init_tracing()
    STORE._replication.start()

    httpd = BaseHTTPServer.ThreadingHTTPServer((settings.HOST, settings.PORT), KVHandler)
    logging.info(
        "Serving on http://%s:%d  shards=%d per_shard_max=%d",
        settings.HOST,
        settings.PORT,
        settings.SHARDS,
        settings.PER_SHARD_MAX,
    )

    httpsd = None
    https_thread = None
    if getattr(settings, "HTTP_TLS_ENABLED", False):
        ctx = _server_ssl_context(getattr(settings, "TLS_CERT_FILE", ""), getattr(settings, "TLS_KEY_FILE", ""))
        if ctx is not None:
            try:
                httpsd = BaseHTTPServer.ThreadingHTTPServer((settings.HOST, settings.HTTPS_PORT), KVHandler)
                httpsd.socket = ctx.wrap_socket(httpsd.socket, server_side=True)
                https_thread = threading.Thread(target=httpsd.serve_forever, daemon=True)
                https_thread.start()
                logging.info("Serving on https://%s:%d", settings.HOST, settings.HTTPS_PORT)
            except Exception as e:
                logging.warning("Failed to start HTTPS listener: %s", e)
                try:
                    if httpsd is not None:
                        httpsd.server_close()
                except Exception:
                    pass
                httpsd = None

    def stop(sig: int, _frame: Any) -> None:
        logging.info("Shutting down (%s)…", sig)
        if httpsd is not None:
            try:
                httpsd.shutdown()
            except Exception:
                pass
            try:
                httpsd.server_close()
            except Exception:
                pass
        httpd.shutdown()
        try:
            httpd.server_close()
        except Exception:
            pass
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
