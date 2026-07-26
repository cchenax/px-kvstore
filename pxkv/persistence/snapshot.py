#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import threading
import time
import glob
import gzip
import hashlib
import re
import binascii
from typing import Any, Dict, Optional

from ..config.settings import settings

_DIFF_FORMAT = "pxkv-page-diff-v1"


def _json_default(v: Any):
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    raise TypeError


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, default=_json_default, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _page_hash(page: Dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(page).encode("utf-8")).hexdigest()


def _read_json(path: str) -> Dict[str, Any]:
    if path.endswith(".gz"):
        with gzip.open(path, "rt") as f:
            return json.load(f)
    with open(path, "r") as f:
        return json.load(f)


def _write_json_atomic(path: str, payload: Dict[str, Any]) -> None:
    if path.endswith(".gz"):
        tmp = f"{path}.tmp"
        with gzip.open(tmp, "wt", compresslevel=int(getattr(settings, "COMPRESSION_LEVEL", 6))) as f:
            json.dump(payload, f, default=_json_default)
        os.replace(tmp, path)
        return
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, default=_json_default)
    os.replace(tmp, path)


def _archive_lsn(path: str) -> Optional[int]:
    m = re.search(r"\.(\d+)\.\d+\.archive(?:\.diff)?(?:\.gz)?$", path)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _base_path_from_archive(path: str) -> str:
    return re.sub(r"\.\d+\.\d+\.archive(?:\.diff)?(?:\.gz)?$", "", path)


def _archive_path(base_path: str, lsn: int, ts: int, *, diff: bool = False) -> str:
    path = f"{base_path}.{lsn}.{ts}.archive"
    if diff:
        path += ".diff"
    if getattr(settings, "COMPRESSION_ENABLED", False) and getattr(settings, "COMPRESSION_ALGORITHM", "gzip") == "gzip":
        path += ".gz"
    return path


def _page_bucket(key: Any, buckets: int) -> int:
    raw = key.encode("utf-8", errors="replace") if isinstance(key, str) else str(key).encode("utf-8", errors="replace")
    return binascii.crc32(raw) % max(1, int(buckets))


def _payload_to_pages(payload: Dict[str, Any], buckets: int) -> Dict[str, Dict[str, Any]]:
    pages: Dict[str, Dict[str, Any]] = {}
    for top_key, value in payload.items():
        if top_key in ("_lsn", "_ts", "_snapshot_format", "_snapshot_diff"):
            continue
        if str(top_key).isdigit() and isinstance(value, dict):
            grouped: Dict[int, Dict[str, Any]] = {}
            for key, rec in value.items():
                bucket = _page_bucket(key, buckets)
                grouped.setdefault(bucket, {})[key] = rec
            for bucket, items in grouped.items():
                page_id = f"shard:{top_key}:bucket:{bucket}"
                pages[page_id] = {"kind": "shard", "shard": str(top_key), "items": items}
        elif top_key.startswith("_"):
            page_id = f"meta:{top_key}"
            pages[page_id] = {"kind": "meta", "key": top_key, "value": value}
    return pages


def _pages_to_payload(pages: Dict[str, Dict[str, Any]], *, lsn: int, ts: float) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"_lsn": int(lsn), "_ts": ts}
    for page in pages.values():
        if not isinstance(page, dict):
            continue
        if page.get("kind") == "shard":
            shard = str(page.get("shard", ""))
            if not shard.isdigit():
                continue
            payload.setdefault(shard, {}).update(page.get("items", {}) or {})
        elif page.get("kind") == "meta":
            key = page.get("key")
            if isinstance(key, str) and key.startswith("_"):
                payload[key] = page.get("value")
    return payload


def _manifest_for_pages(pages: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    return {page_id: _page_hash(page) for page_id, page in pages.items()}


def _find_archive_by_lsn(base_path: str, target_lsn: int) -> Optional[str]:
    for fpath, lsn in list_snapshot_archives(base_path):
        if int(lsn) == int(target_lsn):
            return fpath
    return None


def _diff_base_lsn(path: str) -> Optional[int]:
    try:
        data = _read_json(path)
    except Exception:
        return None
    if data.get("_snapshot_format") != _DIFF_FORMAT:
        return None
    try:
        return int(data.get("base_lsn", 0) or 0)
    except Exception:
        return None


def _load_snapshot_payload(path: str) -> Dict[str, Any]:
    data = _read_json(path)
    if data.get("_snapshot_format") != _DIFF_FORMAT:
        return data

    base_lsn = int(data.get("base_lsn", 0) or 0)
    base_path = _base_path_from_archive(path)
    base_archive = _find_archive_by_lsn(base_path, base_lsn)
    if not base_archive:
        raise ValueError(f"base snapshot archive for LSN {base_lsn} not found")

    base_payload = _load_snapshot_payload(base_archive)
    buckets = int(data.get("page_buckets", getattr(settings, "SNAPSHOT_DIFF_PAGE_BUCKETS", 256)) or 256)
    pages = _payload_to_pages(base_payload, buckets)
    for page_id in data.get("deleted_pages", []) or []:
        pages.pop(page_id, None)
    for page_id, page in (data.get("pages", {}) or {}).items():
        pages[page_id] = page

    expected = data.get("manifest", {}) or {}
    actual = _manifest_for_pages(pages)
    if expected and expected != actual:
        raise ValueError("snapshot diff manifest verification failed")
    return _pages_to_payload(pages, lsn=int(data.get("_lsn", 0) or 0), ts=float(data.get("_ts", 0.0) or 0.0))


def _build_diff_payload(payload: Dict[str, Any], base_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    buckets = int(getattr(settings, "SNAPSHOT_DIFF_PAGE_BUCKETS", 256) or 256)
    current_pages = _payload_to_pages(payload, buckets)
    base_pages = _payload_to_pages(base_payload, buckets)
    current_manifest = _manifest_for_pages(current_pages)
    base_manifest = _manifest_for_pages(base_pages)

    changed_pages = {
        page_id: current_pages[page_id]
        for page_id, digest in current_manifest.items()
        if base_manifest.get(page_id) != digest
    }
    deleted_pages = sorted(page_id for page_id in base_manifest.keys() if page_id not in current_manifest)

    if not changed_pages and not deleted_pages:
        return None

    return {
        "_snapshot_format": _DIFF_FORMAT,
        "_lsn": int(payload.get("_lsn", 0) or 0),
        "_ts": float(payload.get("_ts", 0.0) or 0.0),
        "base_lsn": int(base_payload.get("_lsn", 0) or 0),
        "page_buckets": buckets,
        "manifest": current_manifest,
        "pages": changed_pages,
        "deleted_pages": deleted_pages,
        "stats": {
            "changed_pages": len(changed_pages),
            "deleted_pages": len(deleted_pages),
            "total_pages": len(current_pages),
            "base_pages": len(base_pages),
        },
    }


def load_snapshot(store, path: str) -> bool:
    if not path:
        return False
    file_to_read = path
    if getattr(settings, "COMPRESSION_ENABLED", False) and getattr(settings, "COMPRESSION_ALGORITHM", "gzip") == "gzip":
        if not file_to_read.endswith(".gz"):
            file_to_read = file_to_read + ".gz"
        if not os.path.exists(file_to_read):
            file_to_read = path
    if not os.path.exists(file_to_read):
        return False
    try:
        data = _load_snapshot_payload(file_to_read)
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
        logging.info("Restored state from %s", file_to_read)
        return True
    except Exception as e:
        logging.error("Failed to load snapshot from %s: %s", file_to_read, e)
        return False


def list_snapshot_archives(base_path: str) -> list:
    """List all snapshot archives, sorted by LSN descending."""
    archives = []
    if not base_path:
        return archives
    pattern = f"{base_path}.*.archive*"
    files = glob.glob(pattern)
    for fpath in files:
        try:
            lsn = _archive_lsn(fpath)
            if lsn is not None:
                archives.append((fpath, lsn))
        except Exception:
            pass
    archives.sort(key=lambda x: x[1], reverse=True)
    return archives


def prune_snapshot_archives(base_path: str, keep: int) -> None:
    """Prune snapshot archives to keep only the N most recent ones."""
    if keep <= 0 or not base_path:
        return
    archives = list_snapshot_archives(base_path)
    if len(archives) > keep:
        protected = {fpath for fpath, _ in archives[:keep]}

        # Diff archives are only useful with their base chain. Preserve those
        # dependencies even when doing so slightly exceeds the configured keep.
        changed = True
        while changed:
            changed = False
            for fpath, _ in archives:
                if fpath not in protected:
                    continue
                base_lsn = _diff_base_lsn(fpath)
                if base_lsn is None:
                    continue
                base_path_for_diff = _find_archive_by_lsn(base_path, base_lsn)
                if base_path_for_diff and base_path_for_diff not in protected:
                    protected.add(base_path_for_diff)
                    changed = True

        for fpath, _ in archives:
            if fpath in protected:
                continue
            try:
                os.remove(fpath)
                logging.info("Pruned old snapshot archive: %s", fpath)
            except Exception as e:
                logging.warning("Failed to prune snapshot archive %s: %s", fpath, e)


def find_snapshot_for_lsn(base_path: str, target_lsn: int) -> Optional[str]:
    """Find the most recent snapshot whose LSN <= target_lsn."""
    archives = list_snapshot_archives(base_path)
    for fpath, lsn in archives:
        if lsn <= target_lsn:
            return fpath
    # If no archive matches, check the main snapshot
    file_to_check = base_path
    if getattr(settings, "COMPRESSION_ENABLED", False) and getattr(settings, "COMPRESSION_ALGORITHM", "gzip") == "gzip":
        if not file_to_check.endswith(".gz"):
            file_to_check = file_to_check + ".gz"
        if not os.path.exists(file_to_check):
            file_to_check = base_path
    if os.path.exists(file_to_check):
        try:
            data = _load_snapshot_payload(file_to_check)
            lsn = int(data.get("_lsn", 0) or 0)
            if lsn <= target_lsn:
                return file_to_check
        except Exception:
            pass
    return None


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
            payload["_ts"] = time.time()
            
            if getattr(settings, "COMPRESSION_ENABLED", False) and getattr(settings, "COMPRESSION_ALGORITHM", "gzip") == "gzip":
                _write_json_atomic(self.path + ".gz", payload)
                logging.info("Saved compressed snapshot to %s.gz", self.path)
            else:
                _write_json_atomic(self.path, payload)
                logging.info("Saved snapshot to %s", self.path)
            
            # Also save an archived version if PITR is enabled
            if getattr(settings, "PITR_ENABLED", True):
                try:
                    ts = int(time.time())
                    archive_payload = payload
                    archive_path = _archive_path(self.path, int(lsn), ts, diff=False)
                    if getattr(settings, "SNAPSHOT_DIFF_ENABLED", False):
                        previous = None
                        for fpath, prev_lsn in list_snapshot_archives(self.path):
                            if int(prev_lsn) < int(lsn):
                                previous = fpath
                                break
                        if previous:
                            try:
                                base_payload = _load_snapshot_payload(previous)
                                diff_payload = _build_diff_payload(payload, base_payload)
                                if diff_payload is not None:
                                    archive_payload = diff_payload
                                    archive_path = _archive_path(self.path, int(lsn), ts, diff=True)
                            except Exception as e:
                                logging.warning("Failed to build snapshot diff archive, falling back to full archive: %s", e)
                    _write_json_atomic(archive_path, archive_payload)
                    if archive_payload.get("_snapshot_format") == _DIFF_FORMAT:
                        stats = archive_payload.get("stats", {})
                        logging.info(
                            "Saved snapshot diff archive to %s (changed_pages=%s total_pages=%s)",
                            archive_path,
                            stats.get("changed_pages"),
                            stats.get("total_pages"),
                        )
                    else:
                        logging.info("Saved snapshot archive to %s", archive_path)
                    keep = int(getattr(settings, "PITR_SNAPSHOT_KEEP", 5))
                    prune_snapshot_archives(self.path, keep)
                except Exception as e:
                    logging.warning("Failed to save snapshot archive: %s", e)
            
            if settings.WAL_ROTATE_ENABLED and getattr(self.store, "_wal", None) is not None:
                try:
                    self.store._wal.rotate_after_snapshot(int(lsn), keep=int(settings.WAL_ROTATE_KEEP))
                    logging.info("Rotated WAL after snapshot (lsn=%d)", int(lsn))
                except Exception as e:
                    logging.warning("WAL rotation failed: %s", e)
        except Exception as e:
            logging.error("Failed to save snapshot: %s", e)
            for tmp_file in [tmp, f"{self.path}.tmp.gz", f"{self.path}.gz.tmp"]:
                if os.path.exists(tmp_file):
                    try:
                        os.remove(tmp_file)
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


def recover_to_lsn(store, target_lsn: int, snapshot_path: str, wal_path: str) -> bool:
    """
    Recover the store to a specific LSN using snapshots and WAL.
    Returns True on success.
    """
    from .wal import WAL

    logging.info("Starting PITR recovery to LSN %d", target_lsn)

    # Find and load the appropriate snapshot
    snapshot_file = find_snapshot_for_lsn(snapshot_path, target_lsn)
    if snapshot_file:
        logging.info("Loading snapshot for PITR: %s", snapshot_file)
        if not load_snapshot(store, snapshot_file):
            return False
    else:
        logging.info("No suitable snapshot found, starting from empty")
        try:
            store.load({})
        except Exception:
            pass

    # Now replay WAL up to target_lsn
    if wal_path and os.path.exists(wal_path):
        wal = WAL(wal_path)
        try:
            entries = []
            with open(wal_path, "r") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                        if entry.get("type") == "meta":
                            continue
                        entry_lsn = entry.get("lsn", 0)
                        if entry_lsn > target_lsn:
                            break
                        entries.append(entry)
                    except Exception:
                        continue

            # Apply the entries
            max_applied_lsn = 0
            for entry in entries:
                lsn = entry.get("lsn", 0)
                if lsn <= max_applied_lsn:
                    continue
                op = entry.get("op")
                key = entry.get("key")
                val = entry.get("value")
                ttl = entry.get("ttl")
                try:
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
                    elif op == "stream_xack":
                        payload = val if isinstance(val, dict) else {}
                        store.stream_xack(
                            key,
                            str(payload.get("group", "")),
                            payload.get("ids", []) or [],
                            skip_wal=True,
                            skip_replication=True,
                        )
                    elif op == "stream_delete":
                        store.stream_delete(key, skip_wal=True, skip_replication=True)
                    max_applied_lsn = max(max_applied_lsn, lsn)
                except Exception as e:
                    logging.warning("PITR failed to apply entry LSN %d: %s", lsn, e)
            store._wal._lsn = max_applied_lsn
            logging.info("PITR recovery completed, applied up to LSN %d", max_applied_lsn)
            return True
        except Exception as e:
            logging.error("PITR recovery failed: %s", e)
            return False
    return True


def recover_to_timestamp(store, target_ts: float, snapshot_path: str, wal_path: str) -> bool:
    """
    Recover the store to a specific timestamp (seconds since epoch) using snapshots and WAL.
    Returns True on success.
    """
    from .wal import WAL

    logging.info("Starting PITR recovery to timestamp %f", target_ts)

    # First pass: find max LSN with ts <= target_ts from WAL
    target_lsn = None
    if wal_path and os.path.exists(wal_path):
        try:
            with open(wal_path, "r") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                        if entry.get("type") == "meta":
                            continue
                        entry_ts = entry.get("ts", 0.0)
                        entry_lsn = entry.get("lsn", 0)
                        if entry_ts <= target_ts:
                            target_lsn = entry_lsn
                        else:
                            break
                    except Exception:
                        continue
        except Exception:
            pass

    # If we found a target LSN, use recover_to_lsn
    if target_lsn is not None:
        return recover_to_lsn(store, target_lsn, snapshot_path, wal_path)
    else:
        # If no WAL, try to find the best snapshot
        if snapshot_path and os.path.exists(snapshot_path):
            try:
                data = _load_snapshot_payload(snapshot_path)
                snapshot_ts = data.get("_ts", 0.0)
                if snapshot_ts <= target_ts:
                    load_snapshot(store, snapshot_path)
                    return True
            except Exception:
                pass
        # List archives and find the best one
        archives = list_snapshot_archives(snapshot_path)
        for fpath, _ in archives:
            try:
                data = _load_snapshot_payload(fpath)
                snapshot_ts = data.get("_ts", 0.0)
                if snapshot_ts <= target_ts:
                    load_snapshot(store, fpath)
                    return True
            except Exception:
                continue
        return False
