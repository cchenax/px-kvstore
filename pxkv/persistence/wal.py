#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import logging
from typing import Any, Dict, Optional

class WAL:
    """
    Simple Write-Ahead Log for persistent recovery.
    Appends operations to a file.
    """
    def __init__(self, path: str):
        self.path = path
        self._file = None
        if path:
            self._file = open(path, "a")

    def log(self, op: str, key: Any, value: Any = None, ttl: Optional[float] = None):
        if not self._file:
            return
        entry = {
            "op": op,
            "key": key,
            "value": value,
            "ttl": ttl
        }
        self._file.write(json.dumps(entry) + "\n")
        self._file.flush() # Ensure it's on disk

    def close(self):
        if self._file:
            self._file.close()

def recover_from_wal(store, path: str):
    if not path or not os.path.exists(path):
        return
    logging.info("Recovering from WAL: %s", path)
    try:
        with open(path, "r") as f:
            for line in f:
                if not line.strip():
                    continue
                entry = json.loads(line)
                op = entry["op"]
                key = entry["key"]
                val = entry.get("value")
                ttl = entry.get("ttl")
                
                if op == "create":
                    try:
                        store.create(key, val, ttl)
                    except KeyError:
                        store.update(key, val, ttl)
                elif op == "update":
                    store.update(key, val, ttl)
                elif op == "delete":
                    try:
                        store.delete(key)
                    except KeyError:
                        pass
                elif op == "mset":
                    store.mset(key, ttl) # key is a dict here
                elif op == "incr":
                    store.incr(key, val, ttl) # val is delta
    except Exception as e:
        logging.error("WAL recovery failed: %s", e)
