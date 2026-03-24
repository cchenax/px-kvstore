#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

class Settings:
    HOST = os.getenv("PXKV_HOST", "0.0.0.0")
    PORT = int(os.getenv("PXKV_PORT", "8000"))
    SHARDS = int(os.getenv("PXKV_SHARD_COUNT", os.getenv("SHARD_COUNT", "4")))
    PER_SHARD_MAX = int(os.getenv("PXKV_PER_SHARD_MAX", "1000"))
    EVICTION_POLICY = os.getenv("PXKV_EVICTION_POLICY", "lru")

    FAULT_LATENCY_MS = float(os.getenv("PXKV_FAULT_LATENCY_MS", "0") or "0")
    FAULT_LATENCY_JITTER_MS = float(os.getenv("PXKV_FAULT_LATENCY_JITTER_MS", "0") or "0")

    SNAPSHOT_FILE = os.getenv("PXKV_SNAPSHOT_FILE", "")
    SNAPSHOT_INTERVAL = float(os.getenv("PXKV_SNAPSHOT_INTERVAL", "0"))
    WAL_FILE = os.getenv("PXKV_WAL_FILE", "")

settings = Settings()
