#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
from threading import Lock

class Settings:
    def __init__(self):
        self._lock = Lock()
        self.reload()

    def reload(self):
        """Reload settings from environment variables."""
        with self._lock:
            self.HOST = os.getenv("PXKV_HOST", "0.0.0.0")
            self.PORT = int(os.getenv("PXKV_PORT", "8000"))
            self.SHARDS = int(os.getenv("PXKV_SHARD_COUNT", os.getenv("SHARD_COUNT", "4")))
            self.PER_SHARD_MAX = int(os.getenv("PXKV_PER_SHARD_MAX", "1000"))
            self.EVICTION_POLICY = os.getenv("PXKV_EVICTION_POLICY", "lru")

            self.FAULT_LATENCY_MS = float(os.getenv("PXKV_FAULT_LATENCY_MS", "0") or "0")
            self.FAULT_LATENCY_JITTER_MS = float(os.getenv("PXKV_FAULT_LATENCY_JITTER_MS", "0") or "0")

            self.SNAPSHOT_FILE = os.getenv("PXKV_SNAPSHOT_FILE", "")
            self.SNAPSHOT_INTERVAL = float(os.getenv("PXKV_SNAPSHOT_INTERVAL", "0"))
            self.WAL_FILE = os.getenv("PXKV_WAL_FILE", "")

            self.REDIS_HOST = os.getenv("PXKV_REDIS_HOST", "0.0.0.0")
            self.REDIS_PORT = int(os.getenv("PXKV_REDIS_PORT", "6379"))
            self.REDIS_ENABLED = os.getenv("PXKV_REDIS_ENABLED", "true").lower() == "true"

            self.REPLICATION_ROLE = os.getenv("PXKV_REPLICATION_ROLE", "leader").lower() # leader or follower
            self.REPLICATION_LEADER_ADDR = os.getenv("PXKV_REPLICATION_LEADER_ADDR", "127.0.0.1:8000")
            self.REPLICATION_FOLLOWERS = [f for f in os.getenv("PXKV_REPLICATION_FOLLOWERS", "").split(",") if f]
            self.REPLICATION_SYNC_INTERVAL = float(os.getenv("PXKV_REPLICATION_SYNC_INTERVAL", "1.0"))
            logging.info("Settings reloaded from environment.")

    def update(self, new_settings: dict):
        """Update specific settings dynamically."""
        with self._lock:
            # Only allow updating a subset of settings that make sense at runtime
            updatable = {
                "FAULT_LATENCY_MS": float,
                "FAULT_LATENCY_JITTER_MS": float,
                "SNAPSHOT_INTERVAL": float,
                "REPLICATION_SYNC_INTERVAL": float,
            }
            for key, val in new_settings.items():
                if key in updatable:
                    try:
                        typed_val = updatable[key](val)
                        setattr(self, key, typed_val)
                        logging.info("Setting %s updated to %s", key, typed_val)
                    except (ValueError, TypeError) as e:
                        logging.warning("Failed to update setting %s: %s", key, e)

    def to_dict(self):
        """Return current settings as a dictionary."""
        with self._lock:
            return {k: v for k, v in self.__dict__.items() if k.isupper()}

settings = Settings()
