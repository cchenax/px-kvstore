#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from typing import Any, Dict, Iterable, Iterator, List, Optional
from threading import Lock
from .base import _SortedStringKeyIndex
from ..tiering.base import TieringBackend

class LFUKeyValueStore(object):
    """
    Thread-safe in-memory LFU store with optional TTL per key.

    Eviction: remove the lowest-frequency key; tie-breaker by oldest access.
    This is intentionally simple (O(n) eviction) to keep code small and predictable.
    """

    def __init__(self, max_size: Optional[int] = None, tiering: Optional[TieringBackend] = None):
        self._lock = Lock()
        self._max = max_size
        self._map: Dict[Any, Any] = {}
        self._ttl: Dict[Any, Optional[float]] = {}
        self._freq: Dict[Any, int] = {}
        self._seq: int = 0
        self._last: Dict[Any, int] = {}
        self._skeys = _SortedStringKeyIndex()
        self._tiering = tiering

    def _touch(self, key: Any) -> None:
        self._seq += 1
        self._freq[key] = self._freq.get(key, 0) + 1
        self._last[key] = self._seq

    def _purge_expired_keys(self) -> List[Any]:
        now = time.time()
        expired: List[Any] = []
        for k, ts in self._ttl.items():
            if ts is not None and ts <= now:
                expired.append(k)
        for k in expired:
            self._delete(k)
        return expired

    def _purge_expired(self) -> None:
        _ = self._purge_expired_keys()

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
            if self._tiering is not None:
                now = time.time()
                ts = self._ttl.get(victim)
                ttl_remaining = None if ts is None else max(0.0, ts - now)
                if ttl_remaining is None or ttl_remaining > 0:
                    try:
                        self._tiering.put(victim, self._map.get(victim), ttl_remaining)
                    except Exception:
                        pass
            self._delete(victim)

    def purge_expired(self) -> None:
        with self._lock:
            self._purge_expired()

    def purge_expired_keys(self) -> List[Any]:
        with self._lock:
            return self._purge_expired_keys()

    def create(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            if key in self._map:
                raise KeyError(key)
            if self._tiering is not None:
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass
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
                if self._tiering is None:
                    raise KeyError(key)
                try:
                    hit = self._tiering.get(key)
                except Exception:
                    hit = None
                if hit is None:
                    raise KeyError(key)
                self._map[key] = hit.value
                self._skeys.add(key)
                if hit.ttl_remaining is None:
                    self._ttl[key] = None
                else:
                    self._ttl[key] = time.time() + float(hit.ttl_remaining)
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass
                self._touch(key)
                self._evict_if_needed()
            self._touch(key)
            return self._map[key]

    def update(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            if key not in self._map:
                raise KeyError(key)
            if self._tiering is not None:
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass
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
            if self._tiering is not None:
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass

    def mset(self, items: Dict[Any, Any], ttl: Optional[float] = None) -> None:
        with self._lock:
            self._purge_expired()
            for k, v in items.items():
                if self._tiering is not None:
                    try:
                        self._tiering.delete(k)
                    except Exception:
                        pass
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
            if self._tiering is not None and hasattr(self._tiering, "prefetch"):
                missing: List[Any] = []
                for k in keys:
                    if k not in self._map:
                        missing.append(k)
                if missing:
                    try:
                        self._tiering.prefetch(missing)  # type: ignore[attr-defined]
                    except Exception:
                        pass
            for k in keys:
                if k not in self._map and self._tiering is not None:
                    try:
                        hit = self._tiering.get(k)
                    except Exception:
                        hit = None
                    if hit is not None:
                        self._map[k] = hit.value
                        self._skeys.add(k)
                        if hit.ttl_remaining is None:
                            self._ttl[k] = None
                        else:
                            self._ttl[k] = time.time() + float(hit.ttl_remaining)
                        try:
                            self._tiering.delete(k)
                        except Exception:
                            pass
                        self._touch(k)
                        self._evict_if_needed()
                if k in self._map:
                    self._touch(k)
                    out[k] = self._map[k]
            return out

    def incr(self, key: Any, delta: float = 1, ttl: Optional[float] = None) -> float:
        with self._lock:
            self._purge_expired()
            if self._tiering is not None:
                try:
                    self._tiering.delete(key)
                except Exception:
                    pass
            if key not in self._map:
                current = 0.0
            else:
                current = self._map[key]
                if not isinstance(current, (int, float)):
                    try:
                        current = float(current)
                    except (ValueError, TypeError):
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
