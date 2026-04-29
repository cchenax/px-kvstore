#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import json
from threading import RLock

class Settings:
    def __init__(self):
        self._lock = RLock()
        self.reload()

    def reload(self):
        """Reload settings from environment variables."""
        with self._lock:
            self.CONFIG_FILE = os.getenv("PXKV_CONFIG_FILE", "")

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

            self.TIERING_DIR = os.getenv("PXKV_TIERING_DIR", "")

            self.AUTH_ADMIN_TOKEN = os.getenv("PXKV_AUTH_ADMIN_TOKEN", "")
            self.AUTH_WRITER_TOKEN = os.getenv("PXKV_AUTH_WRITER_TOKEN", "")
            self.AUTH_READER_TOKEN = os.getenv("PXKV_AUTH_READER_TOKEN", "")
            self.AUTH_ADMIN_PASSWORD = os.getenv("PXKV_AUTH_ADMIN_PASSWORD", "")
            self.AUTH_WRITER_PASSWORD = os.getenv("PXKV_AUTH_WRITER_PASSWORD", "")
            self.AUTH_READER_PASSWORD = os.getenv("PXKV_AUTH_READER_PASSWORD", "")

            self.FOLLOWER_READ_ENABLED = os.getenv("PXKV_FOLLOWER_READ_ENABLED", "false").lower() == "true"
            self.FOLLOWER_READ_MAX_LAG_LSN = int(os.getenv("PXKV_FOLLOWER_READ_MAX_LAG_LSN", "0") or "0")
            self.FOLLOWER_READ_MAX_AGE_MS = float(os.getenv("PXKV_FOLLOWER_READ_MAX_AGE_MS", "0") or "0")
            self.FOLLOWER_READ_STRATEGY = os.getenv("PXKV_FOLLOWER_READ_STRATEGY", "least_lag").lower()

            self.REPLICATION_ROLE = os.getenv("PXKV_REPLICATION_ROLE", "leader").lower()
            self.REPLICATION_LEADER_ADDR = os.getenv("PXKV_REPLICATION_LEADER_ADDR", "127.0.0.1:8000")
            self.REPLICATION_FOLLOWERS = [f for f in os.getenv("PXKV_REPLICATION_FOLLOWERS", "").split(",") if f]
            self.REPLICATION_SYNC_INTERVAL = float(os.getenv("PXKV_REPLICATION_SYNC_INTERVAL", "1.0"))
            self.REPLICATION_QUEUE_MAX = int(os.getenv("PXKV_REPLICATION_QUEUE_MAX", "10000") or "10000")
            self.REPLICATION_SHED_POLICY = os.getenv("PXKV_REPLICATION_SHED_POLICY", "drop_newest").lower()

            if self.CONFIG_FILE:
                try:
                    if os.path.exists(self.CONFIG_FILE):
                        with open(self.CONFIG_FILE, "r") as f:
                            data = json.load(f) or {}
                        self.update(data)
                except Exception as e:
                    logging.warning("Failed to load config file %s: %s", self.CONFIG_FILE, e)
            logging.info("Settings reloaded from environment.")

    def update(self, new_settings: dict):
        """Update specific settings dynamically."""
        with self._lock:
            updatable = {
                "FAULT_LATENCY_MS": float,
                "FAULT_LATENCY_JITTER_MS": float,
                "SNAPSHOT_INTERVAL": float,
                "REPLICATION_SYNC_INTERVAL": float,
                "REDIS_ENABLED": lambda v: str(v).lower() == "true",
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
