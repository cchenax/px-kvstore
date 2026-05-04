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
            self.WAL_ROTATE_ENABLED = os.getenv("PXKV_WAL_ROTATE_ENABLED", "false").lower() == "true"
            self.WAL_ROTATE_KEEP = int(os.getenv("PXKV_WAL_ROTATE_KEEP", "0") or "0")

            self.REDIS_HOST = os.getenv("PXKV_REDIS_HOST", "0.0.0.0")
            self.REDIS_PORT = int(os.getenv("PXKV_REDIS_PORT", "6379"))
            self.REDIS_ENABLED = os.getenv("PXKV_REDIS_ENABLED", "true").lower() == "true"

            self.TIERING_DIR = os.getenv("PXKV_TIERING_DIR", "")
            self.TIERING_BACKEND = os.getenv("PXKV_TIERING_BACKEND", "file" if self.TIERING_DIR else "").lower()
            self.TIERING_HTTP_BASE_URL = os.getenv("PXKV_TIERING_HTTP_BASE_URL", "")
            self.TIERING_HTTP_TIMEOUT = float(os.getenv("PXKV_TIERING_HTTP_TIMEOUT", "2.0") or "2.0")
            self.TIERING_S3_BUCKET = os.getenv("PXKV_TIERING_S3_BUCKET", "")
            self.TIERING_S3_PREFIX = os.getenv("PXKV_TIERING_S3_PREFIX", "")
            self.TIERING_S3_REGION = os.getenv("PXKV_TIERING_S3_REGION", "")
            self.TIERING_S3_ENDPOINT_URL = os.getenv("PXKV_TIERING_S3_ENDPOINT_URL", "")
            self.TIERING_PREFETCH_ENABLED = os.getenv("PXKV_TIERING_PREFETCH_ENABLED", "true").lower() == "true"
            self.TIERING_PREFETCH_WORKERS = int(os.getenv("PXKV_TIERING_PREFETCH_WORKERS", "4") or "4")
            self.TIERING_PREFETCH_WAIT_MS = float(os.getenv("PXKV_TIERING_PREFETCH_WAIT_MS", "25") or "25")
            self.TIERING_PREFETCH_CACHE_MAX = int(os.getenv("PXKV_TIERING_PREFETCH_CACHE_MAX", "4096") or "4096")

            self.AUTH_ADMIN_TOKEN = os.getenv("PXKV_AUTH_ADMIN_TOKEN", "")
            self.AUTH_WRITER_TOKEN = os.getenv("PXKV_AUTH_WRITER_TOKEN", "")
            self.AUTH_READER_TOKEN = os.getenv("PXKV_AUTH_READER_TOKEN", "")
            self.AUTH_ADMIN_PASSWORD = os.getenv("PXKV_AUTH_ADMIN_PASSWORD", "")
            self.AUTH_WRITER_PASSWORD = os.getenv("PXKV_AUTH_WRITER_PASSWORD", "")
            self.AUTH_READER_PASSWORD = os.getenv("PXKV_AUTH_READER_PASSWORD", "")

            self.RATE_LIMIT_ENABLED = os.getenv("PXKV_RATE_LIMIT_ENABLED", "false").lower() == "true"
            self.RATE_LIMIT_DEFAULT = {
                "rps": float(os.getenv("PXKV_RATE_LIMIT_DEFAULT_RPS", "0") or "0"),
                "burst": int(os.getenv("PXKV_RATE_LIMIT_DEFAULT_BURST", "0") or "0"),
                "per_ip": os.getenv("PXKV_RATE_LIMIT_DEFAULT_PER_IP", "true").lower() == "true",
            }
            self.RATE_LIMIT_ROUTES = {}
            routes_json = os.getenv("PXKV_RATE_LIMIT_ROUTES", "") or ""
            if routes_json.strip():
                try:
                    parsed = json.loads(routes_json)
                    if isinstance(parsed, dict):
                        self.RATE_LIMIT_ROUTES = parsed
                except Exception as e:
                    logging.warning("Failed to parse PXKV_RATE_LIMIT_ROUTES: %s", e)

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
            def _to_bool(v):
                if isinstance(v, bool):
                    return v
                return str(v).lower() == "true"

            def _policy_merge(base: dict, patch: dict) -> dict:
                out = dict(base or {})
                if not isinstance(patch, dict):
                    raise TypeError("policy must be an object")
                if "rps" in patch:
                    out["rps"] = float(patch["rps"])
                if "burst" in patch:
                    out["burst"] = int(patch["burst"])
                if "per_ip" in patch:
                    out["per_ip"] = _to_bool(patch["per_ip"])
                return out

            updatable = {
                "FAULT_LATENCY_MS": float,
                "FAULT_LATENCY_JITTER_MS": float,
                "SNAPSHOT_INTERVAL": float,
                "REPLICATION_SYNC_INTERVAL": float,
                "REDIS_ENABLED": _to_bool,
                "RATE_LIMIT_ENABLED": _to_bool,
            }
            for key, val in new_settings.items():
                if key in updatable:
                    try:
                        typed_val = updatable[key](val)
                        setattr(self, key, typed_val)
                        logging.info("Setting %s updated to %s", key, typed_val)
                    except (ValueError, TypeError) as e:
                        logging.warning("Failed to update setting %s: %s", key, e)
                elif key == "RATE_LIMIT_DEFAULT":
                    try:
                        if isinstance(val, str):
                            val = json.loads(val)
                        self.RATE_LIMIT_DEFAULT = _policy_merge(self.RATE_LIMIT_DEFAULT, val)
                        logging.info("Setting RATE_LIMIT_DEFAULT updated to %s", self.RATE_LIMIT_DEFAULT)
                    except Exception as e:
                        logging.warning("Failed to update setting RATE_LIMIT_DEFAULT: %s", e)
                elif key == "RATE_LIMIT_ROUTES":
                    try:
                        if isinstance(val, str):
                            val = json.loads(val)
                        if not isinstance(val, dict):
                            raise TypeError("RATE_LIMIT_ROUTES must be an object")
                        merged = dict(self.RATE_LIMIT_ROUTES or {})
                        for route, pol in val.items():
                            if pol is None:
                                merged.pop(route, None)
                                continue
                            merged[route] = _policy_merge(self.RATE_LIMIT_DEFAULT, pol)
                        self.RATE_LIMIT_ROUTES = merged
                        logging.info("Setting RATE_LIMIT_ROUTES updated (%d routes)", len(self.RATE_LIMIT_ROUTES))
                    except Exception as e:
                        logging.warning("Failed to update setting RATE_LIMIT_ROUTES: %s", e)

    def to_dict(self):
        """Return current settings as a dictionary."""
        with self._lock:
            return {k: v for k, v in self.__dict__.items() if k.isupper()}

settings = Settings()
