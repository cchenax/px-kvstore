#!/usr/bin/env python
# -*- coding: utf-8 -*-

import binascii
import heapq
import bisect
from collections import defaultdict
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from .lru import LRUKeyValueStore
from .lfu import LFUKeyValueStore
from ..persistence.wal import WAL
from ..persistence.replication import ReplicationManager
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
    ):
        if shards < 1:
            raise ValueError("shards must be >= 1")
        self._num = shards
        policy = (eviction_policy or "lru").strip().lower()
        tiering = FileTieringBackend(tiering_dir) if tiering_dir else None
        if policy == "lfu":
            factory = lambda: LFUKeyValueStore(per_shard_max, tiering=tiering)
        elif policy == "lru":
            factory = lambda: LRUKeyValueStore(per_shard_max, tiering=tiering)
        else:
            raise ValueError(f"unknown eviction_policy: {eviction_policy!r}")
        self._eviction_policy = policy
        self._shards = [factory() for _ in range(shards)]

        self._ring: List[Tuple[int, int]] = []
        for i in range(shards):
            for v in range(vnodes):
                v_key = f"shard_{i}_v_{v}".encode("utf-8")
                h = binascii.crc32(v_key)
                self._ring.append((h, i))
        self._ring.sort()

        self._wal = WAL(wal_path)
        self._replication = ReplicationManager(self)

    def _idx(self, key: Any) -> int:
        if isinstance(key, str):
            key = key.encode("utf-8")
        h = binascii.crc32(key)

        idx = bisect.bisect_left(self._ring, (h, 0))
        if idx == len(self._ring):
            idx = 0
        return self._ring[idx][1]

    def _bucket(self, key: Any):
        return self._shards[self._idx(key)]

    def purge_expired(self) -> None:
        for shard in self._shards:
            shard.purge_expired()

    def create(self, key: Any, value: Any, ttl: Optional[float] = None, skip_wal: bool = False, skip_replication: bool = False) -> None:
        self._bucket(key).create(key, value, ttl)
        lsn = 0
        if not skip_wal:
            lsn = self._wal.log("create", key, value, ttl)
        if not skip_replication:
            self._replication.enqueue_change("create", key, value, ttl, lsn=lsn)

    def read(self, key: Any) -> Any:
        return self._bucket(key).read(key)

    def update(self, key: Any, value: Any, ttl: Optional[float] = None, skip_wal: bool = False, skip_replication: bool = False) -> None:
        self._bucket(key).update(key, value, ttl)
        lsn = 0
        if not skip_wal:
            lsn = self._wal.log("update", key, value, ttl)
        if not skip_replication:
            self._replication.enqueue_change("update", key, value, ttl, lsn=lsn)

    def delete(self, key: Any, skip_wal: bool = False, skip_replication: bool = False) -> None:
        self._bucket(key).delete(key)
        lsn = 0
        if not skip_wal:
            lsn = self._wal.log("delete", key)
        if not skip_replication:
            self._replication.enqueue_change("delete", key, lsn=lsn)

    def mset(self, items: Dict[Any, Any], ttl: Optional[float] = None, skip_wal: bool = False, skip_replication: bool = False) -> None:
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
        return {str(i): shard.dump_state() for i, shard in enumerate(self._shards)}

    def load(self, data: Dict[str, Dict[str, Any]]) -> None:
        for idx_str, shard_data in data.items():
            try:
                idx = int(idx_str)
            except ValueError:
                continue
            if 0 <= idx < len(self._shards):
                self._shards[idx].load_state(shard_data)
