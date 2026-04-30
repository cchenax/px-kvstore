#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import logging
import glob
import time
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
        self._base_lsn = 1
        if path:
            self._load_or_init()
            self._file = open(path, "a")

    def _load_or_init(self) -> None:
        if not self.path:
            return
        if not os.path.exists(self.path) or os.path.getsize(self.path) == 0:
            self._write_new_file(base_lsn=self._base_lsn)
            return

        min_lsn = None
        try:
            with open(self.path, "r") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                    except Exception:
                        continue
                    if entry.get("type") == "meta":
                        try:
                            self._base_lsn = int(entry.get("base_lsn", self._base_lsn) or self._base_lsn)
                        except Exception:
                            pass
                        continue
                    if "lsn" in entry:
                        try:
                            lsn = int(entry["lsn"])
                        except Exception:
                            continue
                        self._lsn = max(self._lsn, lsn)
                        if min_lsn is None or lsn < min_lsn:
                            min_lsn = lsn
        except Exception as e:
            logging.warning("Could not pre-scan WAL for LSN: %s", e)

        if min_lsn is not None and self._base_lsn == 1:
            self._base_lsn = int(min_lsn)

    def _write_new_file(self, base_lsn: int) -> None:
        if not self.path:
            return
        tmp = f"{self.path}.tmp"
        with open(tmp, "w") as f:
            f.write(json.dumps({"type": "meta", "base_lsn": int(base_lsn)}) + "\n")
        os.replace(tmp, self.path)

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
        if op == "mset" and isinstance(key, dict):
            entry["key"] = {k: _serialize(v) for k, v in key.items()}
            
        self._file.write(json.dumps(entry) + "\n")
        self._file.flush()
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
                        if entry.get("type") == "meta":
                            continue
                        if entry.get("lsn", 0) > start_lsn:
                            entries.append(entry)
                    except:
                        continue
        except Exception as e:
            logging.error("Failed to read WAL entries: %s", e)
        return entries

    def get_oldest_lsn(self) -> int:
        if not self.path:
            return 0
        return int(self._base_lsn)

    def rotate_after_snapshot(self, snapshot_lsn: int, keep: int = 0) -> None:
        if not self.path:
            return
        base_lsn = int(snapshot_lsn) + 1
        if base_lsn < 1:
            base_lsn = 1
        self._base_lsn = base_lsn

        try:
            if self._file:
                self._file.close()
        finally:
            self._file = None

        src = self.path
        if os.path.exists(src) and keep > 0:
            ts = int(time.time())
            dst = f"{src}.{snapshot_lsn}.{ts}.rot"
            try:
                os.replace(src, dst)
            except Exception:
                try:
                    os.remove(src)
                except Exception:
                    pass
        else:
            try:
                if os.path.exists(src):
                    os.remove(src)
            except Exception:
                pass

        self._write_new_file(base_lsn=self._base_lsn)
        self._file = open(self.path, "a")

        if keep > 0:
            pattern = f"{self.path}.*.rot"
            files = sorted(glob.glob(pattern))
            if len(files) > keep:
                for p in files[: max(0, len(files) - keep)]:
                    try:
                        os.remove(p)
                    except Exception:
                        pass

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
                    if entry.get("type") == "meta":
                        continue
                    lsn = entry.get("lsn", 0)
                    max_lsn = max(max_lsn, lsn)
                    op = entry["op"]
                    key = entry["key"]
                    val = entry.get("value")
                    ttl = entry.get("ttl")
                    
                    if op == "create":
                        try:
                            store.create(key, val, ttl, skip_wal=True, skip_replication=True)
                        except KeyError:
                            store.update(key, val, ttl, skip_wal=True, skip_replication=True)
                    elif op == "update":
                        store.update(key, val, ttl, skip_wal=True, skip_replication=True)
                    elif op == "delete":
                        try:
                            store.delete(key, skip_wal=True, skip_replication=True)
                        except KeyError:
                            pass
                    elif op == "mset":
                        store.mset(key, ttl, skip_wal=True, skip_replication=True)
                    elif op == "incr":
                        store.incr(key, val, ttl, skip_wal=True, skip_replication=True)
                except Exception as e:
                    logging.warning("Failed to recover entry: %s", e)
        wal._lsn = max_lsn
    except Exception as e:
        logging.error("WAL recovery failed: %s", e)
