#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import threading
import time

from ..config.settings import settings

def load_snapshot(store, path: str) -> bool:
    if not path or not os.path.exists(path):
        return False
    try:
        with open(path, "r") as f:
            data = json.load(f)
        if isinstance(data, dict) and "_lsn" in data:
            try:
                store._wal._lsn = max(int(store._wal._lsn), int(data.get("_lsn", 0) or 0))
            except Exception:
                pass
            try:
                data = dict(data)
                data.pop("_lsn", None)
            except Exception:
                pass
        store.load(data)
        logging.info("Restored state from %s", path)
        return True
    except Exception as e:
        logging.error("Failed to load snapshot from %s: %s", path, e)
        return False

class SnapshotManager(threading.Thread):
    def __init__(self, store, path: str, interval: float):
        super().__init__(daemon=True)
        self.store = store
        self.path = path
        self.interval = interval
        self._stop_event = threading.Event()

    def snapshot_once(self) -> None:
        if not self.path:
            return
        tmp = f"{self.path}.tmp"
        try:
            lsn, data = self.store.dump_with_lsn()
            payload = dict(data)
            payload["_lsn"] = int(lsn)
            with open(tmp, "w") as f:
                json.dump(payload, f)
            os.replace(tmp, self.path)
            logging.info("Saved snapshot to %s", self.path)
            if settings.WAL_ROTATE_ENABLED and getattr(self.store, "_wal", None) is not None:
                try:
                    self.store._wal.rotate_after_snapshot(int(lsn), keep=int(settings.WAL_ROTATE_KEEP))
                    logging.info("Rotated WAL after snapshot (lsn=%d)", int(lsn))
                except Exception as e:
                    logging.warning("WAL rotation failed: %s", e)
        except Exception as e:
            logging.error("Failed to save snapshot: %s", e)
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except:
                    pass

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logging.info("Snapshot manager started (interval=%.1fs)", settings.SNAPSHOT_INTERVAL)
        while not self._stop_event.is_set():
            interval = settings.SNAPSHOT_INTERVAL
            if interval <= 0:
                time.sleep(1.0)
                continue
            
            time.sleep(interval)
            if self._stop_event.is_set():
                break
            self.snapshot_once()
