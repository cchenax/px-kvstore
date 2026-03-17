#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import random
import threading
import time
from typing import Any

from .store import ShardedKeyValueStore


def _json_default(v: Any) -> Any:
    """
    Helper to make snapshot data JSON-serializable.
    """
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    raise TypeError(f"Object of type {type(v)!r} is not JSON serializable")


def load_snapshot(store: ShardedKeyValueStore, path: str) -> None:
    """
    Load snapshot data from `path` into `store` if the file exists.
    """
    if not path or not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            store.load(data)
    except Exception:
        # Best-effort: if load fails, just start with empty store.
        return


class SnapshotManager(object):
    """
    Periodically persist the contents of a ShardedKeyValueStore to disk.
    """

    def __init__(self, store: ShardedKeyValueStore, path: str, interval_seconds: float):
        self._store = store
        self._path = path
        self._interval = float(interval_seconds)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="pxkv-snapshot", daemon=True)
        self._fail_p = float(os.getenv("PXKV_FAULT_SNAPSHOT_FAIL_P", "0") or "0")

    def start(self) -> None:
        if self._interval <= 0:
            return
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def snapshot_once(self) -> None:
        if self._fail_p > 0 and random.random() < self._fail_p:
            raise RuntimeError("fault injection: snapshot failure")
        data = self._store.dump()
        tmp_path = f"{self._path}.tmp"
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, default=_json_default, ensure_ascii=False)
        os.replace(tmp_path, self._path)

    def _loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(self._interval)
            try:
                self.snapshot_once()
            except Exception:
                # Best-effort persistence: ignore snapshot failures.
                continue

