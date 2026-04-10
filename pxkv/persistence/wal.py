#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import logging
from typing import Any, Dict, List, Optional

class WAL:
    """
    Simple Write-Ahead Log for persistent recovery.
    Appends operations to a file.
    """
    def __init__(self, path: str):
        self.path = path
        self._file = None
        self._lsn = 0
        if path:
            # If path exists, find the last LSN
            if os.path.exists(path):
                try:
                    with open(path, "r") as f:
                        for line in f:
                            if not line.strip():
                                continue
                            try:
                                entry = json.loads(line)
                                if "lsn" in entry:
                                    self._lsn = max(self._lsn, entry["lsn"])
                            except:
                                pass
                except Exception as e:
                    logging.warning("Could not pre-scan WAL for LSN: %s", e)
            self._file = open(path, "a")

    def log(self, op: str, key: Any, value: Any = None, ttl: Optional[float] = None) -> int:
        if not self._file:
            self._lsn += 1
            return self._lsn
        
        self._lsn += 1
        
        def _serialize(obj):
            if isinstance(obj, (bytes, bytearray)):
                return obj.decode("utf-8", errors="replace")
            return obj

        entry = {
            "lsn": self._lsn,
            "op": op,
            "key": key,
            "value": _serialize(value) if value is not None else None,
            "ttl": ttl
        }
        # For mset, key is a dict. We need to serialize its values too.
        if op == "mset" and isinstance(key, dict):
            entry["key"] = {k: _serialize(v) for k, v in key.items()}
            
        self._file.write(json.dumps(entry) + "\n")
        self._file.flush() # Ensure it's on disk
        return self._lsn

    def get_entries(self, start_lsn: int) -> List[Dict[str, Any]]:
        """Read entries from the WAL file starting after start_lsn."""
        if not self.path or not os.path.exists(self.path):
            return []
        
        entries = []
        try:
            with open(self.path, "r") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                        if entry.get("lsn", 0) > start_lsn:
                            entries.append(entry)
                    except:
                        continue
        except Exception as e:
            logging.error("Failed to read WAL entries: %s", e)
        return entries

    def close(self):
        if self._file:
            self._file.close()

def recover_from_wal(store, wal: WAL):
    if not wal.path or not os.path.exists(wal.path):
        return
    logging.info("Recovering from WAL: %s", wal.path)
    max_lsn = 0
    try:
        with open(wal.path, "r") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                    lsn = entry.get("lsn", 0)
                    max_lsn = max(max_lsn, lsn)
                    op = entry["op"]
                    key = entry["key"]
                    val = entry.get("value")
                    ttl = entry.get("ttl")
                    
                    if op == "create":
                        try:
                            store.create(key, val, ttl, skip_wal=True)
                        except KeyError:
                            store.update(key, val, ttl, skip_wal=True)
                    elif op == "update":
                        store.update(key, val, ttl, skip_wal=True)
                    elif op == "delete":
                        try:
                            store.delete(key, skip_wal=True)
                        except KeyError:
                            pass
                    elif op == "mset":
                        store.mset(key, ttl, skip_wal=True) # key is a dict here
                    elif op == "incr":
                        store.incr(key, val, ttl, skip_wal=True) # val is delta
                except Exception as e:
                    logging.warning("Failed to recover entry: %s", e)
        wal._lsn = max_lsn
    except Exception as e:
        logging.error("WAL recovery failed: %s", e)
