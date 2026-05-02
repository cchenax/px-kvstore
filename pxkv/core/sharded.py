#!/usr/bin/env python
# -*- coding: utf-8 -*-

import binascii
import heapq
import bisect
from collections import defaultdict
import threading
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from .lru import LRUKeyValueStore
from .lfu import LFUKeyValueStore
from ..persistence.wal import WAL
from ..persistence.replication import ReplicationManager
from ..tiering.base import TieringBackend
from ..tiering.file import FileTieringBackend

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
        if policy == "lfu":
            factory = lambda: LFUKeyValueStore(per_shard_max, tiering=tiering)
        elif policy == "lru":
            factory = lambda: LRUKeyValueStore(per_shard_max, tiering=tiering)
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

        self._wal = WAL(wal_path)
        self._replication = ReplicationManager(self)

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
            for shard in self._shards:
                shard.purge_expired()

    def create(self, key: Any, value: Any, ttl: Optional[float] = None, skip_wal: bool = False, skip_replication: bool = False) -> None:
        with self._write_lock:
            self._bucket(key).create(key, value, ttl)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("create", key, value, ttl)
            if not skip_replication:
                self._replication.enqueue_change("create", key, value, ttl, lsn=lsn)

    def read(self, key: Any) -> Any:
        return self._bucket(key).read(key)

    def update(self, key: Any, value: Any, ttl: Optional[float] = None, skip_wal: bool = False, skip_replication: bool = False) -> None:
        with self._write_lock:
            self._bucket(key).update(key, value, ttl)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("update", key, value, ttl)
            if not skip_replication:
                self._replication.enqueue_change("update", key, value, ttl, lsn=lsn)

    def delete(self, key: Any, skip_wal: bool = False, skip_replication: bool = False) -> None:
        with self._write_lock:
            self._bucket(key).delete(key)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("delete", key)
            if not skip_replication:
                self._replication.enqueue_change("delete", key, lsn=lsn)

    def mset(self, items: Dict[Any, Any], ttl: Optional[float] = None, skip_wal: bool = False, skip_replication: bool = False) -> None:
        with self._write_lock:
            grouped: Dict[int, Dict[Any, Any]] = defaultdict(dict)
            for k, v in items.items():
                grouped[self._idx(k)][k] = v
            for idx, sub in grouped.items():
                self._shards[idx].mset(sub, ttl)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("mset", items, ttl=ttl)
            if not skip_replication:
                self._replication.enqueue_change("mset", items, ttl=ttl, lsn=lsn)

    def mget(self, keys: Iterable[Any]) -> Dict[Any, Any]:
        grouped: Dict[int, list[Any]] = defaultdict(list)
        for k in keys:
            grouped[self._idx(k)].append(k)
        out: Dict[Any, Any] = {}
        for idx, sub in grouped.items():
            out.update(self._shards[idx].mget(sub))
        return out

    def incr(self, key: Any, delta: float = 1, ttl: Optional[float] = None, skip_wal: bool = False, skip_replication: bool = False) -> float:
        with self._write_lock:
            val = self._bucket(key).incr(key, delta, ttl)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("incr", key, delta, ttl)
            if not skip_replication:
                self._replication.enqueue_change("incr", key, delta, ttl, lsn=lsn)
            return val

    def keys(self) -> List[Any]:
        all_keys: List[Any] = []
        for shard in self._shards:
            all_keys.extend(shard.keys())
        return all_keys

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

    def dump(self) -> Dict[str, Dict[str, Any]]:
        with self._write_lock:
            return {str(i): shard.dump_state() for i, shard in enumerate(self._shards)}

    def dump_with_lsn(self) -> tuple[int, Dict[str, Dict[str, Any]]]:
        with self._write_lock:
            lsn = int(getattr(self._wal, "_lsn", 0) or 0)
            data = {str(i): shard.dump_state() for i, shard in enumerate(self._shards)}
            return lsn, data

    def load(self, data: Dict[str, Dict[str, Any]]) -> None:
        for idx_str, shard_data in data.items():
            try:
                idx = int(idx_str)
            except ValueError:
                continue
            if 0 <= idx < len(self._shards):
                self._shards[idx].load_state(shard_data)
