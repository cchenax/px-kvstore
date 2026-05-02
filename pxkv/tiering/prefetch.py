from __future__ import annotations

import threading
import time
from collections import OrderedDict
from queue import Queue
from typing import Any, Iterable, Optional

from .base import TieringBackend, TieringResult


class AsyncPrefetchTieringBackend(TieringBackend):
    def __init__(
        self,
        backend: TieringBackend,
        workers: int = 4,
        wait_ms: float = 25.0,
        cache_max: int = 4096,
    ):
        self._backend = backend
        self._wait_s = max(0.0, float(wait_ms) / 1000.0)
        self._cache_max = max(0, int(cache_max))

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._queue: Queue[Any] = Queue()
        self._inflight: dict[Any, threading.Event] = {}
        self._cache: OrderedDict[Any, tuple[float, TieringResult]] = OrderedDict()

        n = max(1, int(workers))
        self._threads = [threading.Thread(target=self._worker, daemon=True) for _ in range(n)]
        for t in self._threads:
            t.start()

    def _worker(self) -> None:
        while not self._stop.is_set():
            key = self._queue.get()
            if key is None:
                return
            try:
                hit = self._backend.get(key)
            except Exception:
                hit = None
            now = time.time()
            with self._lock:
                ev = self._inflight.pop(key, None)
                if hit is not None:
                    self._cache[key] = (now, hit)
                    self._cache.move_to_end(key, last=True)
                    if self._cache_max and len(self._cache) > self._cache_max:
                        self._cache.popitem(last=False)
                if ev is not None:
                    ev.set()

    def close(self) -> None:
        self._stop.set()
        for _ in self._threads:
            self._queue.put(None)
        for t in self._threads:
            try:
                t.join(timeout=0.2)
            except Exception:
                pass

    def prefetch(self, keys: Iterable[Any]) -> None:
        with self._lock:
            for key in keys:
                if key in self._cache or key in self._inflight:
                    continue
                ev = threading.Event()
                self._inflight[key] = ev
                self._queue.put(key)

    def _pop_cached(self, key: Any) -> Optional[TieringResult]:
        now = time.time()
        with self._lock:
            item = self._cache.pop(key, None)
        if item is None:
            return None
        fetched_at, hit = item
        if hit.ttl_remaining is None:
            remaining = None
        else:
            remaining = float(hit.ttl_remaining) - max(0.0, now - float(fetched_at))
            if remaining <= 0:
                return None
        return TieringResult(value=hit.value, ttl_remaining=remaining)

    def put(self, key: Any, value: Any, ttl_remaining: Optional[float]) -> None:
        with self._lock:
            self._cache.pop(key, None)
            ev = self._inflight.pop(key, None)
            if ev is not None:
                ev.set()
        self._backend.put(key, value, ttl_remaining)

    def get(self, key: Any) -> Optional[TieringResult]:
        cached = self._pop_cached(key)
        if cached is not None:
            return cached

        with self._lock:
            ev = self._inflight.get(key)
        if ev is not None and self._wait_s > 0:
            ev.wait(timeout=self._wait_s)
            cached = self._pop_cached(key)
            if cached is not None:
                return cached

        return self._backend.get(key)

    def delete(self, key: Any) -> None:
        with self._lock:
            self._cache.pop(key, None)
            ev = self._inflight.pop(key, None)
            if ev is not None:
                ev.set()
        self._backend.delete(key)

