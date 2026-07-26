#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import logging
import glob
import time
import gzip
from typing import Any, Dict, List, Optional

class WAL:
    """
    Simple Write-Ahead Log for persistent recovery.
    Appends operations to a file.
    """
    def __init__(self, path: str, compression_enabled: bool = False, compression_algorithm: str = "gzip", compression_level: int = 6):
        self.path = path
        self._file = None
        self._lsn = 0
        self._base_lsn = 1
        self._compression_enabled = compression_enabled
        self._compression_algorithm = compression_algorithm
        self._compression_level = compression_level
        if path:
            self._load_or_init()
            self._file = self._open_file(path, "a")

    def _open_file(self, path: str, mode: str):
        """Open file with optional compression."""
        if self._compression_enabled and self._compression_algorithm == "gzip":
            if not path.endswith(".gz"):
                path = path + ".gz"
            if mode == "a":
                if not os.path.exists(path):
                    return gzip.open(path, "wt", compresslevel=self._compression_level)
                else:
                    # For append with gzip, we need to read all content first, then write back
                    return open(path, "a")  # Just use regular append for simplicity
            elif mode == "r":
                if os.path.exists(path):
                    return gzip.open(path, "rt")
                # If gzip file doesn't exist, check non-gzip
                if os.path.exists(path.replace(".gz", "")):
                    return open(path.replace(".gz", ""), "r")
                return gzip.open(path, "rt")
            elif mode == "w":
                return gzip.open(path, "wt", compresslevel=self._compression_level)
            else:
                return open(path, mode)
        else:
            if path.endswith(".gz") and os.path.exists(path):
                return gzip.open(path, "rt")
            return open(path, mode)

    def _load_or_init(self) -> None:
        if not self.path:
            return
        file_to_read = self.path
        if self._compression_enabled and self._compression_algorithm == "gzip":
            if not file_to_read.endswith(".gz"):
                file_to_read = file_to_read + ".gz"
            if not os.path.exists(file_to_read):
                file_to_read = self.path
        if not os.path.exists(file_to_read) or os.path.getsize(file_to_read) == 0:
            self._write_new_file(base_lsn=self._base_lsn)
            return

        min_lsn = None
        try:
            with self._open_file(file_to_read, "r") as f:
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
        if self._compression_enabled and self._compression_algorithm == "gzip":
            tmp_gz = f"{self.path}.tmp.gz"
            with gzip.open(tmp_gz, "wt", compresslevel=self._compression_level) as f:
                f.write(json.dumps({"type": "meta", "base_lsn": int(base_lsn)}) + "\n")
            os.replace(tmp_gz, self.path + ".gz")
        else:
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
            "ts": time.time(),
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
        file_to_read = self.path
        if self._compression_enabled and self._compression_algorithm == "gzip":
            if not file_to_read.endswith(".gz"):
                file_to_read = file_to_read + ".gz"
            if not os.path.exists(file_to_read):
                file_to_read = self.path
        
        if not file_to_read or not os.path.exists(file_to_read):
            return []
        
        entries = []
        try:
            with self._open_file(file_to_read, "r") as f:
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
        if self._compression_enabled and self._compression_algorithm == "gzip":
            src = src + ".gz"
        if os.path.exists(src) and keep > 0:
            ts = int(time.time())
            dst = f"{self.path}.{snapshot_lsn}.{ts}.rot"
            if self._compression_enabled and self._compression_algorithm == "gzip":
                dst = dst + ".gz"
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
        self._file = self._open_file(self.path, "a")

        if keep > 0:
            pattern = f"{self.path}.*.rot*"
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
    file_to_read = wal.path
    if wal._compression_enabled and wal._compression_algorithm == "gzip":
        if not file_to_read.endswith(".gz"):
            file_to_read = file_to_read + ".gz"
        if not os.path.exists(file_to_read):
            file_to_read = wal.path
    if not file_to_read or not os.path.exists(file_to_read):
        return
    logging.info("Recovering from WAL: %s", file_to_read)
    max_lsn = 0
    try:
        with wal._open_file(file_to_read, "r") as f:
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
                    elif op == "persist":
                        try:
                            store.persist(key, skip_wal=True, skip_replication=True)
                        except KeyError:
                            pass
                    elif op == "vector_upsert":
                        store.vector_upsert(key, val, skip_wal=True, skip_replication=True)
                    elif op == "vector_delete":
                        store.vector_delete(key, skip_wal=True, skip_replication=True)
                    elif op == "stream_xadd":
                        payload = val if isinstance(val, dict) else {}
                        store.stream_xadd(
                            key,
                            payload.get("fields", {}),
                            entry_id=str(payload.get("id", "*")),
                            maxlen=payload.get("maxlen"),
                            skip_wal=True,
                            skip_replication=True,
                        )
                    elif op == "stream_xgroup_create":
                        payload = val if isinstance(val, dict) else {}
                        try:
                            store.stream_xgroup_create(
                                key,
                                str(payload.get("group", "")),
                                entry_id=str(payload.get("id", "$")),
                                mkstream=bool(payload.get("mkstream", False)),
                                skip_wal=True,
                                skip_replication=True,
                            )
                        except KeyError:
                            pass
                    elif op == "stream_xack":
                        payload = val if isinstance(val, dict) else {}
                        store.stream_xack(
                            key,
                            str(payload.get("group", "")),
                            payload.get("ids", []) or [],
                            skip_wal=True,
                            skip_replication=True,
                        )
                    elif op == "stream_xdeliver":
                        payload = val if isinstance(val, dict) else {}
                        try:
                            store.stream_xdeliver(
                                key,
                                str(payload.get("group", "")),
                                str(payload.get("consumer", "")),
                                payload.get("ids", []) or [],
                                skip_wal=True,
                                skip_replication=True,
                            )
                        except KeyError:
                            pass
                    elif op == "stream_delete":
                        store.stream_delete(key, skip_wal=True, skip_replication=True)
                except Exception as e:
                    logging.warning("Failed to recover entry: %s", e)
        wal._lsn = max_lsn
    except Exception as e:
        logging.error("WAL recovery failed: %s", e)
