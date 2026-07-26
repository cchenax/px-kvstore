#!/usr/bin/env python
# -*- coding: utf-8 -*-

import binascii
import heapq
import bisect
import json
import base64
import time
from collections import defaultdict
import threading
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from .lru import LRUKeyValueStore
from .lfu import LFUKeyValueStore
from .hotkey import HotKeyDetector
from .hotkey_mitigation import HotKeyMitigator
from .adaptive_ttl import AdaptiveTTLController
from .cold_eviction import ColdKeyEvictionHints
from .heavy_hitters import TopKHeavyHitters
from .vector import HNSWVectorIndex, normalize_vector
from .streams import StreamStore
from ..persistence.wal import WAL
from ..persistence.replication import ReplicationManager
from ..config.settings import settings
from ..tiering.base import TieringBackend
from ..tiering.file import FileTieringBackend
from ..notifications import notifier

class ShardedKeyValueStore(object):
    """
    Sharding wrapper using consistent hashing with virtual nodes.
    """

    def __init__(
        self,
        shards: int = 4,
        per_shard_max: int = 1000,
        eviction_policy: str = "lru",
        vnodes: int = 100,
        wal_path: str = "",
        tiering_dir: str = "",
        tiering_backend: str = "",
        tiering_http_base_url: str = "",
        tiering_http_timeout: float = 2.0,
        tiering_s3_bucket: str = "",
        tiering_s3_prefix: str = "",
        tiering_s3_region: str = "",
        tiering_s3_endpoint_url: str = "",
        tiering_prefetch_enabled: bool = True,
        tiering_prefetch_workers: int = 4,
        tiering_prefetch_wait_ms: float = 25.0,
        tiering_prefetch_cache_max: int = 4096,
    ):
        if shards < 1:
            raise ValueError("shards must be >= 1")
        self._num = shards
        policy = (eviction_policy or "lru").strip().lower()
        backend_name = (tiering_backend or "").strip().lower()
        tiering: TieringBackend | None = None
        if backend_name in ("", "none"):
            if tiering_dir:
                tiering = FileTieringBackend(tiering_dir)
        elif backend_name == "file":
            if not tiering_dir:
                raise ValueError("tiering_dir is required for file tiering backend")
            tiering = FileTieringBackend(tiering_dir)
        elif backend_name == "http":
            from ..tiering.http import HttpTieringBackend

            tiering = HttpTieringBackend(tiering_http_base_url, timeout=float(tiering_http_timeout))
        elif backend_name == "s3":
            from ..tiering.s3 import S3TieringBackend

            tiering = S3TieringBackend(
                bucket=tiering_s3_bucket,
                prefix=tiering_s3_prefix,
                region=tiering_s3_region,
                endpoint_url=tiering_s3_endpoint_url,
            )
        else:
            raise ValueError(f"unknown tiering_backend: {tiering_backend!r}")

        if tiering is not None and tiering_prefetch_enabled:
            from ..tiering.prefetch import AsyncPrefetchTieringBackend

            tiering = AsyncPrefetchTieringBackend(
                tiering,
                workers=int(tiering_prefetch_workers),
                wait_ms=float(tiering_prefetch_wait_ms),
                cache_max=int(tiering_prefetch_cache_max),
            )
        self._heavy_hitters = TopKHeavyHitters(
            enabled=getattr(settings, "HEAVY_HITTERS_ENABLED", False),
            k=getattr(settings, "HEAVY_HITTERS_K", 10),
            cms_width=getattr(settings, "HEAVY_HITTERS_CMS_WIDTH", 2048),
            cms_depth=getattr(settings, "HEAVY_HITTERS_CMS_DEPTH", 4),
            decay_interval_seconds=getattr(settings, "HEAVY_HITTERS_DECAY_INTERVAL_SECONDS", 60.0),
            decay_factor=getattr(settings, "HEAVY_HITTERS_DECAY_FACTOR", 0.5),
            threshold_count=getattr(settings, "HEAVY_HITTERS_THRESHOLD_COUNT", 0),
        )
        self._cold_eviction_hints = ColdKeyEvictionHints(
            enabled=getattr(settings, "COLD_KEY_HINTS_ENABLED", False),
            window_seconds=getattr(settings, "COLD_KEY_HINTS_WINDOW_SECONDS", 300.0),
            buckets=getattr(settings, "COLD_KEY_HINTS_BUCKETS", 10),
            scan_candidates=getattr(settings, "COLD_KEY_HINTS_SCAN_CANDIDATES", 8),
            cold_threshold_count=getattr(settings, "COLD_KEY_HINTS_COLD_THRESHOLD_COUNT", 1),
            max_tracked_keys=getattr(settings, "COLD_KEY_HINTS_MAX_TRACKED_KEYS", 100000),
        )
        if policy == "lfu":
            factory = lambda: LFUKeyValueStore(per_shard_max, tiering=tiering, eviction_hints=self._cold_eviction_hints)
        elif policy == "lru":
            factory = lambda: LRUKeyValueStore(per_shard_max, tiering=tiering, eviction_hints=self._cold_eviction_hints)
        else:
            raise ValueError(f"unknown eviction_policy: {eviction_policy!r}")
        self._eviction_policy = policy
        self._shards = [factory() for _ in range(shards)]
        self._write_lock = threading.RLock()

        self._ring: List[Tuple[int, int]] = []
        for i in range(shards):
            for v in range(vnodes):
                v_key = f"shard_{i}_v_{v}".encode("utf-8")
                h = binascii.crc32(v_key)
                self._ring.append((h, i))
        self._ring.sort()

        self._wal = WAL(
            wal_path,
            compression_enabled=getattr(settings, "COMPRESSION_ENABLED", False),
            compression_algorithm=getattr(settings, "COMPRESSION_ALGORITHM", "gzip"),
            compression_level=getattr(settings, "COMPRESSION_LEVEL", 6)
        )
        self._vector_index = HNSWVectorIndex(
            metric=getattr(settings, "VECTOR_INDEX_METRIC", "cosine"),
            m=getattr(settings, "VECTOR_INDEX_M", 16),
            ef_construction=getattr(settings, "VECTOR_INDEX_EF_CONSTRUCTION", 64),
            ef_search=getattr(settings, "VECTOR_INDEX_EF_SEARCH", 64),
        )
        self._streams = StreamStore()
        self._replication = ReplicationManager(self)
        self._hotkeys = HotKeyDetector(
            enabled=getattr(settings, "HOT_KEY_DETECTION_ENABLED", False),
            window_seconds=getattr(settings, "HOT_KEY_WINDOW_SECONDS", 60.0),
            buckets=getattr(settings, "HOT_KEY_BUCKETS", 60),
            top_k=getattr(settings, "HOT_KEY_TOP_K", 10),
            threshold_qps=getattr(settings, "HOT_KEY_THRESHOLD_QPS", 0.0),
            sample_rate=getattr(settings, "HOT_KEY_SAMPLE_RATE", 1.0),
        )
        self._hotkey_mitigation = HotKeyMitigator(
            detector=self._hotkeys,
            enabled=getattr(settings, "HOT_KEY_MITIGATION_ENABLED", False),
            cache_ttl_seconds=getattr(settings, "HOT_KEY_MITIGATION_CACHE_TTL_SECONDS", 1.0),
            max_entries=getattr(settings, "HOT_KEY_MITIGATION_MAX_ENTRIES", 1024),
            refresh_interval_seconds=getattr(settings, "HOT_KEY_MITIGATION_REFRESH_INTERVAL_SECONDS", 1.0),
        )
        self._adaptive_ttl = AdaptiveTTLController(
            enabled=getattr(settings, "ADAPTIVE_TTL_ENABLED", False),
            min_ttl_seconds=getattr(settings, "ADAPTIVE_TTL_MIN_SECONDS", 1.0),
            max_ttl_seconds=getattr(settings, "ADAPTIVE_TTL_MAX_SECONDS", 86400.0),
            default_base_ttl_seconds=getattr(settings, "ADAPTIVE_TTL_DEFAULT_BASE_SECONDS", 60.0),
            hit_extend_factor=getattr(settings, "ADAPTIVE_TTL_HIT_EXTEND_FACTOR", 2.0),
            miss_shrink_factor=getattr(settings, "ADAPTIVE_TTL_MISS_SHRINK_FACTOR", 0.5),
            recency_half_life_seconds=getattr(settings, "ADAPTIVE_TTL_RECENCY_HALF_LIFE_SECONDS", 300.0),
            max_tracked_keys=getattr(settings, "ADAPTIVE_TTL_MAX_TRACKED_KEYS", 10000),
        )

    def _hash_key_material(self, key: Any) -> bytes:
        if isinstance(key, bytes):
            raw = key
        elif isinstance(key, str):
            raw = key.encode("utf-8", errors="replace")
        else:
            raw = str(key).encode("utf-8", errors="replace")

        l = raw.find(b"{")
        if l != -1:
            r = raw.find(b"}", l + 1)
            if r != -1 and r > l + 1:
                return raw[l + 1 : r]
        return raw

    def _idx(self, key: Any) -> int:
        material = self._hash_key_material(key)
        h = binascii.crc32(material)

        idx = bisect.bisect_left(self._ring, (h, 0))
        if idx == len(self._ring):
            idx = 0
        return self._ring[idx][1]

    def shard_for_key(self, key: Any) -> int:
        return self._idx(key)

    def _bucket(self, key: Any):
        return self._shards[self._idx(key)]

    def purge_expired(self) -> None:
        with self._write_lock:
            for idx, shard in enumerate(self._shards):
                keys = []
                if hasattr(shard, "purge_expired_keys"):
                    try:
                        keys = shard.purge_expired_keys()
                    except Exception:
                        keys = []
                else:
                    shard.purge_expired()
                for k in keys:
                    notifier.publish("expire", k, lsn=int(getattr(self._wal, "_lsn", 0) or 0), shard=idx)
                    self._vector_index.delete(k)

    def get_xmeta(self, key: Any) -> Optional[Dict[str, Any]]:
        """Get cross-cluster metadata for a key (origin_cluster_id, origin_ts)."""
        return self._bucket(key).get_xmeta(key)

    def set_xmeta(self, key: Any, meta: Dict[str, Any]) -> None:
        """Set cross-cluster metadata for a key."""
        self._bucket(key).set_xmeta(key, meta)

    def resolve_conflict(
        self,
        key: Any,
        new_value: Any,
        new_ttl: Optional[float],
        new_origin_cluster_id: Optional[str] = None,
        new_origin_ts: Optional[float] = None,
        policy: Optional[str] = None,
    ) -> Tuple[bool, Any, Optional[float]]:
        """Resolve cross-cluster conflict for a key."""
        if policy is None:
            policy = getattr(settings, "CROSS_CLUSTER_CONFLICT_POLICY", "last_write_wins")
        return self._bucket(key).resolve_conflict(
            key, new_value, new_ttl, new_origin_cluster_id, new_origin_ts, policy
        )

    def _tune_ttl(self, key: Any, ttl: Optional[float]) -> Optional[float]:
        if not self._adaptive_ttl.is_enabled():
            return ttl
        return self._adaptive_ttl.suggest_ttl(key, ttl)

    def create(
        self,
        key: Any,
        value: Any,
        ttl: Optional[float] = None,
        skip_wal: bool = False,
        skip_replication: bool = False,
        origin_cluster_id: Optional[str] = None,
        origin_ts: Optional[float] = None,
    ) -> None:
        with self._write_lock:
            ttl = self._tune_ttl(key, ttl)
            self._adaptive_ttl.record_set(key, ttl)
            self._bucket(key).create(key, value, ttl)
            meta = {
                "origin_cluster_id": origin_cluster_id or getattr(settings, "CLUSTER_ID", "local"),
                "origin_ts": origin_ts if origin_ts is not None else time.time(),
            }
            self._bucket(key).set_xmeta(key, meta)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("create", key, value, ttl)
            if not skip_replication:
                self._replication.enqueue_change(
                    "create", key, value, ttl, lsn=lsn,
                    origin_cluster_id=meta["origin_cluster_id"], origin_ts=meta["origin_ts"],
                )
            shard = self._idx(key)
            notifier.publish("set", key, lsn=lsn, shard=shard)
            self._hotkey_mitigation.invalidate(key)
            self._cold_eviction_hints.record(key)

    def update(
        self,
        key: Any,
        value: Any,
        ttl: Optional[float] = None,
        skip_wal: bool = False,
        skip_replication: bool = False,
        origin_cluster_id: Optional[str] = None,
        origin_ts: Optional[float] = None,
    ) -> None:
        with self._write_lock:
            ttl = self._tune_ttl(key, ttl)
            self._adaptive_ttl.record_set(key, ttl)
            self._bucket(key).update(key, value, ttl)
            meta = {
                "origin_cluster_id": origin_cluster_id or getattr(settings, "CLUSTER_ID", "local"),
                "origin_ts": origin_ts if origin_ts is not None else time.time(),
            }
            self._bucket(key).set_xmeta(key, meta)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("update", key, value, ttl)
            if not skip_replication:
                self._replication.enqueue_change(
                    "update", key, value, ttl, lsn=lsn,
                    origin_cluster_id=meta["origin_cluster_id"], origin_ts=meta["origin_ts"],
                )
            shard = self._idx(key)
            notifier.publish("set", key, lsn=lsn, shard=shard)
            self._hotkey_mitigation.invalidate(key)
            self._cold_eviction_hints.record(key)

    def mset(
        self,
        items: Dict[Any, Any],
        ttl: Optional[float] = None,
        skip_wal: bool = False,
        skip_replication: bool = False,
        origin_cluster_id: Optional[str] = None,
        origin_ts: Optional[float] = None,
    ) -> None:
        with self._write_lock:
            grouped: Dict[int, Dict[Any, Any]] = defaultdict(dict)
            tuned_per_key: Dict[Any, Optional[float]] = {}
            adaptive_on = self._adaptive_ttl.is_enabled()
            for k, v in items.items():
                k_ttl = self._tune_ttl(k, ttl)
                tuned_per_key[k] = k_ttl
                self._adaptive_ttl.record_set(k, k_ttl)
                grouped[self._idx(k)][k] = v
            for idx, sub in grouped.items():
                shard = self._shards[idx]
                if adaptive_on and any(tuned_per_key.get(k) != ttl for k in sub.keys()):
                    # Group keys that share the same tuned TTL so each subgroup
                    # can be written with a single mset call.
                    by_ttl: Dict[Optional[float], Dict[Any, Any]] = defaultdict(dict)
                    for k, v in sub.items():
                        by_ttl[tuned_per_key.get(k)][k] = v
                    for sub_ttl, sub_items in by_ttl.items():
                        shard.mset(sub_items, sub_ttl)
                else:
                    shard.mset(sub, ttl)
            meta = {
                "origin_cluster_id": origin_cluster_id or getattr(settings, "CLUSTER_ID", "local"),
                "origin_ts": origin_ts if origin_ts is not None else time.time(),
            }
            for k in items.keys():
                self._bucket(k).set_xmeta(k, meta)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("mset", items, ttl=ttl)
            if not skip_replication:
                self._replication.enqueue_change(
                    "mset", items, ttl=ttl, lsn=lsn,
                    origin_cluster_id=meta["origin_cluster_id"], origin_ts=meta["origin_ts"],
                )
            for k in items.keys():
                shard = self._idx(k)
                notifier.publish("set", k, lsn=lsn, shard=shard)
            self._hotkey_mitigation.invalidate_many(items.keys())
            self._cold_eviction_hints.record_many(items.keys())

    def incr(
        self,
        key: Any,
        delta: float = 1,
        ttl: Optional[float] = None,
        skip_wal: bool = False,
        skip_replication: bool = False,
        origin_cluster_id: Optional[str] = None,
        origin_ts: Optional[float] = None,
    ) -> float:
        with self._write_lock:
            ttl = self._tune_ttl(key, ttl)
            self._adaptive_ttl.record_set(key, ttl)
            val = self._bucket(key).incr(key, delta, ttl)
            meta = {
                "origin_cluster_id": origin_cluster_id or getattr(settings, "CLUSTER_ID", "local"),
                "origin_ts": origin_ts if origin_ts is not None else time.time(),
            }
            self._bucket(key).set_xmeta(key, meta)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("incr", key, delta, ttl)
            if not skip_replication:
                self._replication.enqueue_change(
                    "incr", key, delta, ttl, lsn=lsn,
                    origin_cluster_id=meta["origin_cluster_id"], origin_ts=meta["origin_ts"],
                )
            shard = self._idx(key)
            notifier.publish("set", key, lsn=lsn, shard=shard)
            self._hotkey_mitigation.invalidate(key)
            self._cold_eviction_hints.record(key)
            return val

    def read(self, key: Any) -> Any:
        self._hotkeys.record(key)
        self._heavy_hitters.record(key)
        try:
            if self._hotkey_mitigation.is_enabled():
                value, _served = self._hotkey_mitigation.read_through(
                    key, lambda: self._bucket(key).read(key)
                )
            else:
                value = self._bucket(key).read(key)
        except KeyError:
            self._adaptive_ttl.record_miss(key)
            raise
        self._adaptive_ttl.record_hit(key)
        self._cold_eviction_hints.record(key)
        return value

    def read_with_etag(self, key: Any) -> Tuple[Any, str]:
        self._hotkeys.record(key)
        self._heavy_hitters.record(key)
        try:
            if self._hotkey_mitigation.is_enabled():
                value, etag, _served = self._hotkey_mitigation.read_through_with_etag(
                    key, lambda: self._bucket(key).read_with_etag(key)
                )
            else:
                value, etag = self._bucket(key).read_with_etag(key)
        except KeyError:
            self._adaptive_ttl.record_miss(key)
            raise
        self._adaptive_ttl.record_hit(key)
        self._cold_eviction_hints.record(key)
        return value, etag

    def patch(
        self,
        key: Any,
        patches: List[Dict[str, Any]],
        ttl: Optional[float] = None,
        skip_wal: bool = False,
        skip_replication: bool = False,
        origin_cluster_id: Optional[str] = None,
        origin_ts: Optional[float] = None,
    ) -> Tuple[Any, str]:
        """
        Apply JSON Patch to a key.

        Args:
            key: The key to patch
            patches: List of JSON Patch operations
            ttl: Optional new TTL for the key
            skip_wal: Whether to skip writing to the WAL
            skip_replication: Whether to skip replication
            origin_cluster_id: Cross-cluster origin cluster ID
            origin_ts: Cross-cluster origin timestamp

        Returns:
            Tuple of (new_value, new_etag)

        Raises:
            KeyError: If the key doesn't exist
        """
        with self._write_lock:
            ttl = self._tune_ttl(key, ttl)
            self._adaptive_ttl.record_set(key, ttl)
            new_value, etag = self._bucket(key).patch(key, patches, ttl)
            meta = {
                "origin_cluster_id": origin_cluster_id or getattr(settings, "CLUSTER_ID", "local"),
                "origin_ts": origin_ts if origin_ts is not None else time.time(),
            }
            self._bucket(key).set_xmeta(key, meta)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("update", key, new_value, ttl)
            if not skip_replication:
                self._replication.enqueue_change(
                    "update", key, new_value, ttl, lsn=lsn,
                    origin_cluster_id=meta["origin_cluster_id"], origin_ts=meta["origin_ts"],
                )
            shard = self._idx(key)
            notifier.publish("set", key, lsn=lsn, shard=shard)
            self._hotkey_mitigation.invalidate(key)
            self._cold_eviction_hints.record(key)
            return new_value, etag

    def delete(self, key: Any, skip_wal: bool = False, skip_replication: bool = False) -> None:
        with self._write_lock:
            self._bucket(key).delete(key)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("delete", key)
            if not skip_replication:
                self._replication.enqueue_change("delete", key, lsn=lsn)
            shard = self._idx(key)
            notifier.publish("del", key, lsn=lsn, shard=shard)
            self._hotkeys.forget(key)
            self._hotkey_mitigation.invalidate(key)
            self._adaptive_ttl.forget(key)
            self._cold_eviction_hints.forget(key)
            self._heavy_hitters.forget(key)
            self._vector_index.delete(key)

    def mget(self, keys: Iterable[Any]) -> Dict[Any, Any]:
        key_list = list(keys)
        out: Dict[Any, Any] = {}
        if self._hotkey_mitigation.is_enabled():
            cold_keys: list[Any] = []
            for k in key_list:
                if not self._hotkey_mitigation.is_hot(k):
                    cold_keys.append(k)
                    continue
                try:
                    val, _served = self._hotkey_mitigation.read_through(
                        k, lambda kk=k: self._bucket(kk).read(kk)
                    )
                    out[k] = val
                except KeyError:
                    pass
            keys_to_fetch = cold_keys
        else:
            keys_to_fetch = key_list
        grouped: Dict[int, list[Any]] = defaultdict(list)
        for k in keys_to_fetch:
            grouped[self._idx(k)].append(k)
        for idx, sub in grouped.items():
            out.update(self._shards[idx].mget(sub))
        if out:
            self._hotkeys.record_many(out.keys())
            self._heavy_hitters.record_many(out.keys())
            self._cold_eviction_hints.record_many(out.keys())
        if self._adaptive_ttl.is_enabled():
            for k in key_list:
                if k in out:
                    self._adaptive_ttl.record_hit(k)
                else:
                    self._adaptive_ttl.record_miss(k)
        return out

    def keys(self) -> List[Any]:
        all_keys: List[Any] = []
        for shard in self._shards:
            all_keys.extend(shard.keys())
        return all_keys

    def get_ttl(self, key: Any) -> Optional[float]:
        return self._bucket(key).get_ttl(key)

    def persist(self, key: Any, skip_wal: bool = False, skip_replication: bool = False) -> bool:
        with self._write_lock:
            had_ttl = self._bucket(key).persist(key)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("persist", key)
            if not skip_replication:
                self._replication.enqueue_change("persist", key, lsn=lsn)
            shard = self._idx(key)
            notifier.publish("persist", key, lsn=lsn, shard=shard)
            return had_ttl

    def vector_upsert(
        self,
        key: Any,
        vector: Iterable[Any],
        *,
        skip_wal: bool = False,
        skip_replication: bool = False,
        origin_cluster_id: Optional[str] = None,
        origin_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        with self._write_lock:
            vec = normalize_vector(vector)
            self._vector_index.upsert(key, vec)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("vector_upsert", key, list(vec))
            if not skip_replication:
                self._replication.enqueue_change(
                    "vector_upsert",
                    key,
                    list(vec),
                    lsn=lsn,
                    origin_cluster_id=origin_cluster_id or getattr(settings, "CLUSTER_ID", "local"),
                    origin_ts=origin_ts if origin_ts is not None else time.time(),
                )
            return {"key": key, "dimension": len(vec), "lsn": lsn}

    def vector_delete(
        self,
        key: Any,
        *,
        skip_wal: bool = False,
        skip_replication: bool = False,
    ) -> bool:
        with self._write_lock:
            deleted = self._vector_index.delete(key)
            lsn = 0
            if deleted and not skip_wal:
                lsn = self._wal.log("vector_delete", key)
            if deleted and not skip_replication:
                self._replication.enqueue_change("vector_delete", key, lsn=lsn)
            return deleted

    def vector_get(self, key: Any) -> Optional[List[float]]:
        vec = self._vector_index.get(key)
        if vec is None:
            return None
        try:
            self._bucket(key).read(key)
        except KeyError:
            self._vector_index.delete(key)
            return None
        return list(vec)

    def vector_search(
        self,
        vector: Iterable[Any],
        *,
        k: int = 10,
        ef: Optional[int] = None,
        include_values: bool = False,
    ) -> List[Dict[str, Any]]:
        limit = max(0, int(k))
        if limit == 0:
            return []
        raw = self._vector_index.search(vector, k=max(limit * 4, limit), ef=ef)
        out: List[Dict[str, Any]] = []
        stale: List[Any] = []
        for item in raw:
            key = item["key"]
            try:
                value = self._bucket(key).read(key)
            except KeyError:
                stale.append(key)
                continue
            result = {
                "key": key,
                "score": item["score"],
                "distance": item["distance"],
            }
            if include_values:
                result["value"] = value
            out.append(result)
            if len(out) >= limit:
                break
        for key in stale:
            self._vector_index.delete(key)
        return out

    def vector_stats(self) -> Dict[str, Any]:
        return self._vector_index.stats()

    def stream_xadd(
        self,
        key: Any,
        fields: Dict[str, Any],
        *,
        entry_id: str = "*",
        maxlen: Optional[int] = None,
        skip_wal: bool = False,
        skip_replication: bool = False,
    ) -> str:
        with self._write_lock:
            new_id = self._streams.xadd(key, fields, entry_id=entry_id, maxlen=maxlen)
            payload = {"fields": fields, "id": new_id, "maxlen": maxlen}
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("stream_xadd", key, payload)
            if not skip_replication:
                self._replication.enqueue_change("stream_xadd", key, payload, lsn=lsn)
            notifier.publish("stream", key, lsn=lsn, shard=self._idx(key))
            return new_id

    def stream_xrange(
        self,
        key: Any,
        *,
        start: str = "-",
        end: str = "+",
        count: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        return self._streams.xrange(key, start=start, end=end, count=count)

    def stream_xread(
        self,
        streams: Dict[Any, str],
        *,
        count: Optional[int] = None,
        block_ms: int = 0,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        return self._streams.xread(streams, count=count, block_ms=block_ms)

    def stream_xgroup_create(
        self,
        key: Any,
        group: str,
        *,
        entry_id: str = "$",
        mkstream: bool = False,
        skip_wal: bool = False,
        skip_replication: bool = False,
    ) -> bool:
        with self._write_lock:
            ok = self._streams.xgroup_create(key, group, entry_id=entry_id, mkstream=mkstream)
            payload = {"group": group, "id": entry_id, "mkstream": mkstream}
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("stream_xgroup_create", key, payload)
            if not skip_replication:
                self._replication.enqueue_change("stream_xgroup_create", key, payload, lsn=lsn)
            return ok

    def stream_xreadgroup(
        self,
        key: Any,
        group: str,
        consumer: str,
        *,
        entry_id: str = ">",
        count: Optional[int] = None,
        block_ms: int = 0,
        skip_wal: bool = False,
        skip_replication: bool = False,
    ) -> List[Dict[str, Any]]:
        entries = self._streams.xreadgroup(key, group, consumer, entry_id=entry_id, count=count, block_ms=block_ms)
        delivered_ids = [entry["id"] for entry in entries]
        if entry_id == ">" and delivered_ids:
            payload = {"group": group, "consumer": consumer, "ids": delivered_ids}
            lsn = 0
            with self._write_lock:
                if not skip_wal:
                    lsn = self._wal.log("stream_xdeliver", key, payload)
                if not skip_replication:
                    self._replication.enqueue_change("stream_xdeliver", key, payload, lsn=lsn)
            notifier.publish("stream", key, lsn=lsn, shard=self._idx(key))
        return entries

    def stream_xdeliver(
        self,
        key: Any,
        group: str,
        consumer: str,
        ids: Iterable[str],
        *,
        skip_wal: bool = False,
        skip_replication: bool = False,
    ) -> int:
        with self._write_lock:
            id_list = [str(entry_id) for entry_id in ids]
            count = self._streams.mark_delivered(key, group, consumer, id_list)
            if count > 0:
                payload = {"group": group, "consumer": consumer, "ids": id_list}
                lsn = 0
                if not skip_wal:
                    lsn = self._wal.log("stream_xdeliver", key, payload)
                if not skip_replication:
                    self._replication.enqueue_change("stream_xdeliver", key, payload, lsn=lsn)
                notifier.publish("stream", key, lsn=lsn, shard=self._idx(key))
            return count

    def stream_xack(
        self,
        key: Any,
        group: str,
        ids: Iterable[str],
        *,
        skip_wal: bool = False,
        skip_replication: bool = False,
    ) -> int:
        with self._write_lock:
            id_list = [str(entry_id) for entry_id in ids]
            count = self._streams.xack(key, group, id_list)
            if count > 0:
                payload = {"group": group, "ids": id_list}
                lsn = 0
                if not skip_wal:
                    lsn = self._wal.log("stream_xack", key, payload)
                if not skip_replication:
                    self._replication.enqueue_change("stream_xack", key, payload, lsn=lsn)
            return count

    def stream_xpending(self, key: Any, group: str) -> Dict[str, Any]:
        return self._streams.xpending(key, group)

    def stream_delete(
        self,
        key: Any,
        *,
        skip_wal: bool = False,
        skip_replication: bool = False,
    ) -> bool:
        with self._write_lock:
            deleted = self._streams.delete(key)
            if deleted:
                lsn = 0
                if not skip_wal:
                    lsn = self._wal.log("stream_delete", key)
                if not skip_replication:
                    self._replication.enqueue_change("stream_delete", key, lsn=lsn)
            return deleted

    def stream_stats(self) -> Dict[str, Any]:
        return self._streams.stats()

    def scan(
        self,
        prefix: Optional[str] = None,
        limit: int = 100,
        start_after: Optional[str] = None,
    ) -> List[str]:
        lim = max(0, int(limit))
        if lim == 0:
            return []

        iters: List[Iterator[str]] = [
            shard.iter_string_keys_sorted(prefix=prefix, start_after=start_after) for shard in self._shards
        ]
        heap: List[Tuple[str, int]] = []
        for i, it in enumerate(iters):
            try:
                first = next(it)
            except StopIteration:
                continue
            heapq.heappush(heap, (first, i))

        out: List[str] = []
        while heap and len(out) < lim:
            k, i = heapq.heappop(heap)
            out.append(k)
            try:
                nxt = next(iters[i])
            except StopIteration:
                continue
            heapq.heappush(heap, (nxt, i))
        return out

    def _encode_cursor(self, state: Dict[str, Any]) -> str:
        """Encode cursor state as a base64-encoded JSON string."""
        return base64.urlsafe_b64encode(json.dumps(state, ensure_ascii=False).encode("utf-8")).decode("ascii")

    def _decode_cursor(self, cursor: Optional[str]) -> Dict[str, Any]:
        """Decode cursor from base64-encoded JSON; empty/default state for None/empty."""
        if not cursor:
            return {
                "shard_positions": [None] * len(self._shards),
                "done": False,
            }
        try:
            decoded = json.loads(base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8"))
            if "shard_positions" not in decoded:
                decoded["shard_positions"] = [None] * len(self._shards)
            if len(decoded["shard_positions"]) != len(self._shards):
                decoded["shard_positions"] = [None] * len(self._shards)
            return decoded
        except Exception:
            return {
                "shard_positions": [None] * len(self._shards),
                "done": False,
            }

    def scan_with_cursor(
        self,
        cursor: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 100,
    ) -> Tuple[str, List[str]]:
        """
        True cursor-based scan.
        Returns (next_cursor, keys). When next_cursor is "0", scan is complete.
        """
        state = self._decode_cursor(cursor)
        if state.get("done", False):
            return "0", []

        lim = max(0, int(limit))
        if lim == 0:
            return cursor or "0", []

        out: List[str] = []
        shard_positions = list(state["shard_positions"])

        iters: List[Tuple[int, Iterator[str]]] = []
        heap: List[Tuple[str, int, int]] = []  # (key, shard_idx, iter_idx)

        iter_idx = 0
        for shard_idx in range(len(self._shards)):
            start_after = shard_positions[shard_idx]
            it = self._shards[shard_idx].iter_string_keys_sorted(prefix=prefix, start_after=start_after)
            iters.append((shard_idx, it))
            try:
                first = next(it)
                heapq.heappush(heap, (first, shard_idx, iter_idx))
            except StopIteration:
                pass
            iter_idx += 1

        while heap and len(out) < lim:
            k, shard_idx, _ = heapq.heappop(heap)
            out.append(k)
            # Update the shard's position to this key (start_after is exclusive)
            shard_positions[shard_idx] = k
            # Try to get next key from this shard's iterator
            shard_idx_in_iters, it = None, None
            for idx, (s_idx, i) in enumerate(iters):
                if s_idx == shard_idx:
                    shard_idx_in_iters, it = idx, i
                    break
            if it is not None:
                try:
                    nxt = next(it)
                    heapq.heappush(heap, (nxt, shard_idx, shard_idx_in_iters))
                except StopIteration:
                    pass

        done = not heap
        new_state = {
            "shard_positions": shard_positions,
            "done": done,
        }
        next_cursor = "0" if done else self._encode_cursor(new_state)
        return next_cursor, out

    def dump(self) -> Dict[str, Dict[str, Any]]:
        with self._write_lock:
            data = {str(i): shard.dump_state() for i, shard in enumerate(self._shards)}
            data["_vectors"] = self._vector_index.dump()
            data["_streams"] = self._streams.dump()
            return data

    def dump_with_lsn(self) -> tuple[int, Dict[str, Dict[str, Any]]]:
        with self._write_lock:
            lsn = int(getattr(self._wal, "_lsn", 0) or 0)
            data = {str(i): shard.dump_state() for i, shard in enumerate(self._shards)}
            data["_vectors"] = self._vector_index.dump()
            data["_streams"] = self._streams.dump()
            return lsn, data

    def load(self, data: Dict[str, Dict[str, Any]]) -> None:
        vectors = data.get("_vectors") if isinstance(data, dict) else None
        streams = data.get("_streams") if isinstance(data, dict) else None
        for idx_str, shard_data in data.items():
            try:
                idx = int(idx_str)
            except ValueError:
                continue
            if 0 <= idx < len(self._shards):
                self._shards[idx].load_state(shard_data)
        self._vector_index.clear()
        if isinstance(vectors, dict):
            try:
                self._vector_index.load(vectors)
            except Exception:
                self._vector_index.clear()
        self._streams.load(streams if isinstance(streams, dict) else {})

    def reshard(self, new_shards: int) -> dict:
        """
        Reshard the store to use new_shards shards, migrating keys as needed.
        Returns a summary dict with old_shards, new_shards, keys_migrated.
        """
        if new_shards < 1:
            raise ValueError("new_shards must be >= 1")

        with self._write_lock:
            old_shards = self._num
            if old_shards == new_shards:
                return {"old_shards": old_shards, "new_shards": new_shards, "keys_migrated": 0}

            # Collect all key-value-ttl-xmeta from all old shards
            all_data = []
            for shard in self._shards:
                state = shard.dump_state()
                for k, rec in state.items():
                    v = rec.get("value")
                    ttl = rec.get("ttl")
                    xmeta = rec.get("xmeta")
                    all_data.append((k, v, ttl, xmeta))

            # Create new shards
            self._num = new_shards
            self._vnodes = [f"vn-{i}-{j}" for j in range(self._vnodes_per_shard) for i in range(self._num)]
            self._ring = []
            for n in self._vnodes:
                hash_val = binascii.crc32(n.encode("utf-8")) & 0xffffffff
                self._ring.append((hash_val, n, int(n.split("-")[1])))
            self._ring.sort()

            new_shard_list = []
            for i in range(self._num):
                if self._eviction_policy == "lru":
                    shard = LRUKeyValueStore(max_size=self._per_shard_max, eviction_hints=self._cold_eviction_hints)
                else:
                    shard = LFUKeyValueStore(max_size=self._per_shard_max, eviction_hints=self._cold_eviction_hints)
                new_shard_list.append(shard)
            self._shards = new_shard_list

            # Re-insert all data into new shards
            keys_migrated = 0
            for k, v, ttl, xmeta in all_data:
                try:
                    self._bucket(k).create(k, v, ttl)
                    if xmeta:
                        self._bucket(k).set_xmeta(k, xmeta)
                    keys_migrated += 1
                except KeyError:
                    self._bucket(k).update(k, v, ttl)
                    if xmeta:
                        self._bucket(k).set_xmeta(k, xmeta)
                    keys_migrated += 1

            return {
                "old_shards": old_shards,
                "new_shards": new_shards,
                "keys_migrated": keys_migrated,
            }
