#!/usr/bin/env python
# -*- coding: utf-8 -*-

import binascii
import time
from collections import defaultdict
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Tuple


class _Node(object):
    def __init__(self, key: Any, value: Any):
        self.key = key
        self.value = value
        self.prev: Optional["_Node"] = None
        self.next: Optional["_Node"] = None


class LRUKeyValueStore(object):
    """
    Thread-safe in-memory LRU key-value store with optional TTL per key.
    """

    def __init__(self, max_size: Optional[int] = None):
        self._lock = Lock()
        self._max = max_size
        self._map: Dict[Any, _Node] = {}
        self._ttl: Dict[Any, Optional[float]] = {}

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

    def create(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            if key in self._map:
                raise KeyError(key)
            node = _Node(key, value)
            self._map[key] = node
            self._append(node)
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
            now = time.time()
            for k, entry in data.items():
                ttl_remaining = entry.get("ttl")
                value = entry.get("value")
                node = _Node(k, value)
                self._map[k] = node
                self._append(node)
                if ttl_remaining is None:
                    expire_ts = None
                else:
                    expire_ts = now + float(ttl_remaining)
                self._ttl[k] = expire_ts


class ShardedKeyValueStore(object):
    """
    Simple sharding wrapper around multiple LRUKeyValueStore instances.
    """

    def __init__(self, shards: int = 4, per_shard_max: int = 1000):
        if shards < 1:
            raise ValueError("shards must be >= 1")
        self._num = shards
        self._shards = [LRUKeyValueStore(per_shard_max) for _ in range(shards)]

    def _idx(self, key: Any) -> int:
        if isinstance(key, str):
            key = key.encode("utf-8")
        return binascii.crc32(key) % self._num

    def _bucket(self, key: Any) -> LRUKeyValueStore:
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
        keys = [k for k in self.keys() if isinstance(k, str)]
        keys.sort()
        if prefix is not None:
            keys = [k for k in keys if k.startswith(prefix)]
        if start_after is not None:
            keys = [k for k in keys if k > start_after]
        return keys[: max(0, int(limit))]

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

