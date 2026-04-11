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
            data = self.store.dump()
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, self.path)
            logging.info("Saved snapshot to %s", self.path)
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
                time.sleep(1.0) # Check again later if it was enabled
                continue
            
            time.sleep(interval)
            if self._stop_event.is_set():
                break
            self.snapshot_once()
