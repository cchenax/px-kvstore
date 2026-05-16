#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import binascii
import fnmatch
import heapq
import bisect
import json
from collections import defaultdict
import threading
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from .lru import LRUKeyValueStore
from .lfu import LFUKeyValueStore
from ..persistence.wal import WAL
from ..persistence.replication import ReplicationManager
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

    def create(self, key: Any, value: Any, ttl: Optional[float] = None, skip_wal: bool = False, skip_replication: bool = False) -> None:
        with self._write_lock:
            self._bucket(key).create(key, value, ttl)
            lsn = 0
            if not skip_wal:
                lsn = self._wal.log("create", key, value, ttl)
            if not skip_replication:
                self._replication.enqueue_change("create", key, value, ttl, lsn=lsn)
            shard = self._idx(key)
            notifier.publish("set", key, lsn=lsn, shard=shard)

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
            shard = self._idx(key)
            notifier.publish("set", key, lsn=lsn, shard=shard)

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
            for k in items.keys():
                shard = self._idx(k)
                notifier.publish("set", k, lsn=lsn, shard=shard)

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
            shard = self._idx(key)
            notifier.publish("set", key, lsn=lsn, shard=shard)
            return val

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

    _CURSOR_VERSION = 1

    def _encode_cursor(self, shard_idx: int, start_after: Optional[str]) -> str:
        payload: Dict[str, Any] = {"v": self._CURSOR_VERSION, "s": int(shard_idx)}
        if start_after is not None:
            payload["k"] = start_after
        raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    def _decode_cursor(self, cursor: Optional[str]) -> Tuple[int, Optional[str]]:
        if cursor is None or cursor == "" or cursor == "0":
            return 0, None
        try:
            pad = "=" * (-len(cursor) % 4)
            raw = base64.urlsafe_b64decode((cursor + pad).encode("ascii"))
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            return 0, None
        if not isinstance(payload, dict):
            return 0, None
        if int(payload.get("v", 0)) != self._CURSOR_VERSION:
            return 0, None
        try:
            s = int(payload.get("s", 0))
        except (TypeError, ValueError):
            return 0, None
        if s < 0 or s > len(self._shards):
            return 0, None
        k = payload.get("k")
        if k is not None and not isinstance(k, str):
            k = None
        return s, k

    def _scan_step(
        self,
        shard_idx: int,
        start_after: Optional[str],
        *,
        match: Optional[str] = None,
        count: int = 100,
        prefix: Optional[str] = None,
    ) -> Tuple[Optional[int], Optional[str], List[str]]:
        """Advance one SCAN step from (shard_idx, start_after).

        Returns (next_shard_idx, next_start_after, keys). next_shard_idx is
        None when iteration has finished — callers should treat that as the
        terminal state.
        """
        try:
            examined_limit = int(count)
        except (TypeError, ValueError):
            examined_limit = 100
        if examined_limit < 1:
            examined_limit = 1

        if shard_idx < 0:
            shard_idx = 0
            start_after = None

        out: List[str] = []
        examined = 0

        while shard_idx < len(self._shards) and examined < examined_limit:
            shard = self._shards[shard_idx]
            it = shard.iter_string_keys_sorted(prefix=prefix, start_after=start_after)
            last_key = start_after
            shard_done = True
            for k in it:
                examined += 1
                last_key = k
                if match is None or fnmatch.fnmatchcase(k, match):
                    out.append(k)
                if examined >= examined_limit:
                    shard_done = False
                    break

            if shard_done:
                shard_idx += 1
                start_after = None
            else:
                start_after = last_key
                break

        if shard_idx >= len(self._shards):
            return None, None, out
        return shard_idx, start_after, out

    def scan_cursor(
        self,
        cursor: str = "0",
        *,
        match: Optional[str] = None,
        count: int = 100,
        prefix: Optional[str] = None,
    ) -> Tuple[str, List[str]]:
        """Cursor-based SCAN over the global keyspace.

        Returns (next_cursor, keys). A returned cursor of "0" indicates
        iteration has finished. Cursors are opaque tokens — callers must
        treat them as bytes and only pass them back unchanged.

        - cursor: pass "0" (or empty) to start; otherwise the value returned
          by the previous call.
        - match: optional Redis-style glob applied to keys (post-filter).
        - count: hint on how many keys to examine per call; the number of
          returned keys may be lower if MATCH filters some out, or higher
          if a shard yields extras to make progress.
        - prefix: optional prefix filter; uses the sorted index for O(log n)
          positioning rather than a full scan.
        """
        shard_idx, start_after = self._decode_cursor(cursor)
        next_idx, next_after, keys = self._scan_step(
            shard_idx, start_after, match=match, count=count, prefix=prefix
        )
        if next_idx is None:
            return "0", keys
        return self._encode_cursor(next_idx, next_after), keys

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
