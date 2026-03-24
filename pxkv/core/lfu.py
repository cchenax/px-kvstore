#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from typing import Any, Dict, Iterable, Iterator, List, Optional
from threading import Lock
from .base import _SortedStringKeyIndex

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

    def purge_expired(self) -> None:
        with self._lock:
            self._purge_expired()

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
