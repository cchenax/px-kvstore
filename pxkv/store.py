#!/usr/bin/env python
# -*- coding: utf-8 -*-

import binascii
import bisect
import heapq
import time
from collections import defaultdict
from threading import Lock
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple


class _Node(object):
    def __init__(self, key: Any, value: Any):
        self.key = key
        self.value = value
        self.prev: Optional["_Node"] = None
        self.next: Optional["_Node"] = None


class _SortedStringKeyIndex:
    """
    Maintain a sorted list of string keys for faster scan/pagination.
    """

    def __init__(self) -> None:
        self._keys: List[str] = []

    def clear(self) -> None:
        self._keys.clear()

    def add(self, key: Any) -> None:
        if not isinstance(key, str):
            return
        i = bisect.bisect_left(self._keys, key)
        if i < len(self._keys) and self._keys[i] == key:
            return
        self._keys.insert(i, key)

    def discard(self, key: Any) -> None:
        if not isinstance(key, str):
            return
        i = bisect.bisect_left(self._keys, key)
        if i < len(self._keys) and self._keys[i] == key:
            self._keys.pop(i)

    def iter_from(
        self, *, prefix: Optional[str] = None, start_after: Optional[str] = None
    ) -> Iterator[str]:
        if prefix is None and start_after is None:
            idx = 0
        else:
            if prefix is not None:
                idx = bisect.bisect_left(self._keys, prefix)
            else:
                idx = 0
            if start_after is not None:
                idx = max(idx, bisect.bisect_right(self._keys, start_after))

        while idx < len(self._keys):
            k = self._keys[idx]
            if prefix is not None and not k.startswith(prefix):
                break
            yield k
            idx += 1


class LRUKeyValueStore(object):
    """
    Thread-safe in-memory LRU key-value store with optional TTL per key.
    """

    def __init__(self, max_size: Optional[int] = None):
        self._lock = Lock()
        self._max = max_size
        self._map: Dict[Any, _Node] = {}
        self._ttl: Dict[Any, Optional[float]] = {}
        self._skeys = _SortedStringKeyIndex()

        self._head = _Node(None, None)
        self._tail = _Node(None, None)
        self._head.next = self._tail
        self._tail.prev = self._head

    def _append(self, node: _Node) -> None:
        last = self._tail.prev
        if last is None:
            return
        last.next = node
        node.prev = last
        node.next = self._tail
        self._tail.prev = node

    def _remove(self, node: _Node) -> None:
        if node.prev is not None:
            node.prev.next = node.next
        if node.next is not None:
            node.next.prev = node.prev
        node.prev = node.next = None

    def _move_to_tail(self, node: _Node) -> None:
        self._remove(node)
        self._append(node)

    def _purge_expired(self) -> None:
        now = time.time()

        expired = []
        for k, ts in self._ttl.items():
            if ts is not None and ts <= now:
                expired.append(k)

        for k in expired:
            self._delete(k)

    def _evict_if_needed(self) -> None:
        while self._max and len(self._map) > self._max:
            lru = self._head.next
            if lru is None:
                break
            self._delete(lru.key)

    def _delete(self, key: Any) -> None:
        node = self._map.pop(key, None)
        if node:
            self._remove(node)
        self._ttl.pop(key, None)
        self._skeys.discard(key)

    def create(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            if key in self._map:
                raise KeyError(key)
            node = _Node(key, value)
            self._map[key] = node
            self._append(node)
            self._skeys.add(key)
            if ttl is not None:
                expire_ts = time.time() + ttl
            else:
                expire_ts = None
            self._ttl[key] = expire_ts

            self._evict_if_needed()

    def read(self, key: Any) -> Any:
        with self._lock:
            self._purge_expired()
            node = self._map.get(key)
            if node is None:
                raise KeyError(key)
            self._move_to_tail(node)
            return node.value

    def update(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            node = self._map.get(key)
            if node is None:
                raise KeyError(key)
            node.value = value
            self._move_to_tail(node)
            if ttl is not None:
                self._ttl[key] = time.time() + ttl
            self._evict_if_needed()

    def delete(self, key: Any) -> None:
        with self._lock:
            if key not in self._map:
                raise KeyError(key)
            self._delete(key)

    def mset(self, items: Dict[Any, Any], ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            for k, v in items.items():
                node = self._map.get(k)
                if node:
                    node.value = v
                    self._move_to_tail(node)
                else:
                    node = _Node(k, v)
                    self._map[k] = node
                    self._append(node)
                    self._skeys.add(k)
                if ttl is not None:
                    expire_ts = time.time() + ttl
                else:
                    expire_ts = None
                self._ttl[k] = expire_ts
            self._evict_if_needed()

    def mget(self, keys: Iterable[Any]) -> Dict[Any, Any]:
        with self._lock:
            self._purge_expired()
            out: Dict[Any, Any] = {}
            for k in keys:
                node = self._map.get(k)
                if node:
                    self._move_to_tail(node)
                    out[k] = node.value
            return out

    def incr(self, key: Any, delta: float = 1, ttl: Optional[float] = None) -> float:
        """
        Atomically increment a numeric value stored at `key` by `delta`.

        If the key does not exist, it is treated as 0.
        """
        with self._lock:
            self._purge_expired()
            node = self._map.get(key)
            if node is None:
                current = 0.0
                node = _Node(key, current)
                self._map[key] = node
                self._append(node)
                self._skeys.add(key)
            else:
                current = node.value
                if not isinstance(current, (int, float)):
                    raise TypeError("INCR target must be numeric")
            new_val = float(current) + float(delta)
            node.value = new_val
            self._move_to_tail(node)
            if ttl is not None:
                self._ttl[key] = time.time() + ttl
            self._evict_if_needed()
            return new_val

    def keys(self) -> List[Any]:
        """
        Return current (non-expired) keys in this shard.
        """
        with self._lock:
            self._purge_expired()
            return list(self._map.keys())

    def iter_string_keys_sorted(
        self, *, prefix: Optional[str] = None, start_after: Optional[str] = None
    ) -> Iterator[str]:
        with self._lock:
            self._purge_expired()
            yield from self._skeys.iter_from(prefix=prefix, start_after=start_after)

    def dump_state(self) -> Dict[str, Any]:
        """
        Produce a JSON-serializable snapshot of this shard's data.
        """
        with self._lock:
            self._purge_expired()
            now = time.time()
            data: Dict[str, Any] = {}
            for k, node in self._map.items():
                ts = self._ttl.get(k)
                if ts is not None and ts <= now:
                    continue
                if ts is None:
                    ttl_remaining = None
                else:
                    ttl_remaining = max(0.0, ts - now)
                data[k] = {"value": node.value, "ttl": ttl_remaining}
            return data

    def load_state(self, data: Dict[str, Dict[str, Any]]) -> None:
        """
        Restore this shard's data from a snapshot previously produced by dump_state.
        """
        with self._lock:
            self._map.clear()
            self._ttl.clear()
            self._skeys.clear()
            now = time.time()
            for k, entry in data.items():
                ttl_remaining = entry.get("ttl")
                value = entry.get("value")
                node = _Node(k, value)
                self._map[k] = node
                self._append(node)
                self._skeys.add(k)
                if ttl_remaining is None:
                    expire_ts = None
                else:
                    expire_ts = now + float(ttl_remaining)
                self._ttl[k] = expire_ts


class LFUKeyValueStore(object):
    """
    Thread-safe in-memory LFU store with optional TTL per key.

    Eviction: remove the lowest-frequency key; tie-breaker by oldest access.
    This is intentionally simple (O(n) eviction) to keep code small and predictable.
    """

    def __init__(self, max_size: Optional[int] = None):
        self._lock = Lock()
        self._max = max_size
        self._map: Dict[Any, Any] = {}
        self._ttl: Dict[Any, Optional[float]] = {}
        self._freq: Dict[Any, int] = {}
        self._seq: int = 0
        self._last: Dict[Any, int] = {}
        self._skeys = _SortedStringKeyIndex()

    def _touch(self, key: Any) -> None:
        self._seq += 1
        self._freq[key] = self._freq.get(key, 0) + 1
        self._last[key] = self._seq

    def _purge_expired(self) -> None:
        now = time.time()
        expired: List[Any] = []
        for k, ts in self._ttl.items():
            if ts is not None and ts <= now:
                expired.append(k)
        for k in expired:
            self._delete(k)

    def _delete(self, key: Any) -> None:
        self._map.pop(key, None)
        self._ttl.pop(key, None)
        self._freq.pop(key, None)
        self._last.pop(key, None)
        self._skeys.discard(key)

    def _evict_if_needed(self) -> None:
        while self._max and len(self._map) > self._max:
            victim = None
            best = None
            for k in self._map.keys():
                score = (self._freq.get(k, 0), self._last.get(k, 0))
                if best is None or score < best:
                    best = score
                    victim = k
            if victim is None:
                break
            self._delete(victim)

    def create(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            if key in self._map:
                raise KeyError(key)
            self._map[key] = value
            self._skeys.add(key)
            if ttl is not None:
                self._ttl[key] = time.time() + ttl
            else:
                self._ttl[key] = None
            self._touch(key)
            self._evict_if_needed()

    def read(self, key: Any) -> Any:
        with self._lock:
            self._purge_expired()
            if key not in self._map:
                raise KeyError(key)
            self._touch(key)
            return self._map[key]

    def update(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            if key not in self._map:
                raise KeyError(key)
            self._map[key] = value
            if ttl is not None:
                self._ttl[key] = time.time() + ttl
            self._touch(key)
            self._evict_if_needed()

    def delete(self, key: Any) -> None:
        with self._lock:
            if key not in self._map:
                raise KeyError(key)
            self._delete(key)

    def mset(self, items: Dict[Any, Any], ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            for k, v in items.items():
                if k in self._map:
                    self._map[k] = v
                else:
                    self._map[k] = v
                    self._skeys.add(k)
                if ttl is not None:
                    self._ttl[k] = time.time() + ttl
                else:
                    self._ttl.setdefault(k, None)
                self._touch(k)
            self._evict_if_needed()

    def mget(self, keys: Iterable[Any]) -> Dict[Any, Any]:
        with self._lock:
            self._purge_expired()
            out: Dict[Any, Any] = {}
            for k in keys:
                if k in self._map:
                    self._touch(k)
                    out[k] = self._map[k]
            return out

    def incr(self, key: Any, delta: float = 1, ttl: Optional[float] = None) -> float:
        with self._lock:
            self._purge_expired()
            if key not in self._map:
                current = 0.0
            else:
                current = self._map[key]
                if not isinstance(current, (int, float)):
                    raise TypeError("INCR target must be numeric")
            new_val = float(current) + float(delta)
            self._map[key] = new_val
            self._skeys.add(key)
            if ttl is not None:
                self._ttl[key] = time.time() + ttl
            self._touch(key)
            self._evict_if_needed()
            return new_val

    def keys(self) -> List[Any]:
        with self._lock:
            self._purge_expired()
            return list(self._map.keys())

    def iter_string_keys_sorted(
        self, *, prefix: Optional[str] = None, start_after: Optional[str] = None
    ) -> Iterator[str]:
        with self._lock:
            self._purge_expired()
            yield from self._skeys.iter_from(prefix=prefix, start_after=start_after)

    def dump_state(self) -> Dict[str, Any]:
        with self._lock:
            self._purge_expired()
            now = time.time()
            data: Dict[str, Any] = {}
            for k, v in self._map.items():
                ts = self._ttl.get(k)
                if ts is not None and ts <= now:
                    continue
                ttl_remaining = None if ts is None else max(0.0, ts - now)
                data[k] = {"value": v, "ttl": ttl_remaining}
            return data

    def load_state(self, data: Dict[str, Dict[str, Any]]) -> None:
        with self._lock:
            self._map.clear()
            self._ttl.clear()
            self._freq.clear()
            self._last.clear()
            self._seq = 0
            self._skeys.clear()
            now = time.time()
            for k, entry in data.items():
                ttl_remaining = entry.get("ttl")
                value = entry.get("value")
                self._map[k] = value
                self._skeys.add(k)
                self._freq[k] = 1
                self._seq += 1
                self._last[k] = self._seq
                self._ttl[k] = None if ttl_remaining is None else now + float(ttl_remaining)


class ShardedKeyValueStore(object):
    """
    Simple sharding wrapper around multiple in-memory stores.
    """

    def __init__(self, shards: int = 4, per_shard_max: int = 1000, eviction_policy: str = "lru"):
        if shards < 1:
            raise ValueError("shards must be >= 1")
        self._num = shards
        policy = (eviction_policy or "lru").strip().lower()
        if policy == "lfu":
            factory = lambda: LFUKeyValueStore(per_shard_max)
        elif policy == "lru":
            factory = lambda: LRUKeyValueStore(per_shard_max)
        else:
            raise ValueError(f"unknown eviction_policy: {eviction_policy!r}")
        self._eviction_policy = policy
        self._shards = [factory() for _ in range(shards)]

    def _idx(self, key: Any) -> int:
        if isinstance(key, str):
            key = key.encode("utf-8")
        return binascii.crc32(key) % self._num

    def _bucket(self, key: Any):
        return self._shards[self._idx(key)]

    def create(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        self._bucket(key).create(key, value, ttl)

    def read(self, key: Any) -> Any:
        return self._bucket(key).read(key)

    def update(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        self._bucket(key).update(key, value, ttl)

    def delete(self, key: Any) -> None:
        self._bucket(key).delete(key)

    def mset(self, items: Dict[Any, Any], ttl: Optional[float] = None) -> None:
        grouped: Dict[int, Dict[Any, Any]] = defaultdict(dict)
        for k, v in items.items():
            grouped[self._idx(k)][k] = v
        for idx, sub in grouped.items():
            self._shards[idx].mset(sub, ttl)

    def mget(self, keys: Iterable[Any]) -> Dict[Any, Any]:
        grouped: Dict[int, list[Any]] = defaultdict(list)
        for k in keys:
            grouped[self._idx(k)].append(k)
        out: Dict[Any, Any] = {}
        for idx, sub in grouped.items():
            out.update(self._shards[idx].mget(sub))
        return out

    def incr(self, key: Any, delta: float = 1, ttl: Optional[float] = None) -> float:
        """
        Atomically increment a numeric value stored at `key` by `delta`.

        If the key does not exist, it is treated as 0.
        """
        return self._bucket(key).incr(key, delta, ttl)

    def keys(self) -> List[Any]:
        """
        Return keys across all shards.
        """
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
        """
        Return up to `limit` string keys, optionally filtered by prefix and
        lexicographically after `start_after`.
        """
        lim = max(0, int(limit))
        if lim == 0:
            return []

        # K-way merge across shards using each shard's sorted string key index.
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
        """
        Dump full sharded state for persistence.
        """
        return {str(i): shard.dump_state() for i, shard in enumerate(self._shards)}

    def load(self, data: Dict[str, Dict[str, Any]]) -> None:
        """
        Restore sharded state from data previously produced by `dump`.
        """
        for idx_str, shard_data in data.items():
            try:
                idx = int(idx_str)
            except ValueError:
                continue
            if 0 <= idx < len(self._shards):
                self._shards[idx].load_state(shard_data)

