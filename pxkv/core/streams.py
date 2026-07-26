#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import threading
from typing import Any, Dict, Iterable, List, Optional, Tuple


MIN_ID = "0-0"
MAX_ID = "18446744073709551615-18446744073709551615"


def parse_stream_id(value: Any, *, allow_special: bool = True) -> Tuple[int, int]:
    raw = str(value)
    if allow_special and raw == "-":
        return 0, 0
    if allow_special and raw == "+":
        return 18446744073709551615, 18446744073709551615
    if raw == "$":
        return 18446744073709551615, 18446744073709551615
    if "-" not in raw:
        return int(raw), 0
    ms, seq = raw.split("-", 1)
    return int(ms), int(seq)


def format_stream_id(value: Tuple[int, int]) -> str:
    return f"{int(value[0])}-{int(value[1])}"


def compare_stream_ids(left: Any, right: Any) -> int:
    a = parse_stream_id(left)
    b = parse_stream_id(right)
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


class StreamStore:
    """
    In-memory Redis Streams-style store.

    Entries are append-only per stream and ordered by `ms-seq` IDs. Consumer
    group metadata is stored beside the stream so snapshots can restore queue
    progress and pending acknowledgements.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._streams: Dict[Any, Dict[str, Any]] = {}

    def _stream(self, key: Any, *, create: bool = False) -> Optional[Dict[str, Any]]:
        stream = self._streams.get(key)
        if stream is None and create:
            stream = {"last_id": "0-0", "entries": [], "groups": {}}
            self._streams[key] = stream
        return stream

    def _next_id(self, stream: Dict[str, Any], requested: str) -> str:
        now_ms = int(time.time() * 1000)
        last = parse_stream_id(stream.get("last_id", "0-0"))
        requested = str(requested or "*")
        if requested == "*":
            if now_ms > last[0]:
                candidate = (now_ms, 0)
            else:
                candidate = (last[0], last[1] + 1)
        elif requested.endswith("-*"):
            ms = int(requested[:-2])
            candidate = (ms, last[1] + 1 if ms == last[0] else 0)
        else:
            candidate = parse_stream_id(requested, allow_special=False)
        if candidate <= last:
            raise ValueError("stream ID must be greater than the last entry ID")
        return format_stream_id(candidate)

    def xadd(
        self,
        key: Any,
        fields: Dict[str, Any],
        *,
        entry_id: str = "*",
        maxlen: Optional[int] = None,
    ) -> str:
        if not isinstance(fields, dict) or not fields:
            raise ValueError("fields must be a non-empty object")
        with self._cv:
            stream = self._stream(key, create=True)
            assert stream is not None
            new_id = self._next_id(stream, entry_id)
            entry = {"id": new_id, "fields": dict(fields)}
            stream["entries"].append(entry)
            stream["last_id"] = new_id
            if maxlen is not None and int(maxlen) >= 0:
                self._trim_locked(stream, int(maxlen))
            self._cv.notify_all()
            return new_id

    def _trim_locked(self, stream: Dict[str, Any], maxlen: int) -> int:
        entries = stream.get("entries", [])
        if maxlen <= 0:
            removed = list(entries)
            stream["entries"] = []
        elif len(entries) > maxlen:
            cut = len(entries) - maxlen
            removed = entries[:cut]
            stream["entries"] = entries[cut:]
        else:
            return 0

        removed_ids = {entry["id"] for entry in removed}
        for group in stream.get("groups", {}).values():
            pending = group.get("pending", {})
            for entry_id in list(removed_ids):
                pending.pop(entry_id, None)
            for consumer in group.get("consumers", {}).values():
                c_pending = consumer.get("pending", [])
                consumer["pending"] = [entry_id for entry_id in c_pending if entry_id not in removed_ids]
        return len(removed)

    def xrange(self, key: Any, start: str = "-", end: str = "+", count: Optional[int] = None) -> List[Dict[str, Any]]:
        with self._lock:
            stream = self._stream(key)
            if stream is None:
                return []
            start_id = parse_stream_id(start)
            end_id = parse_stream_id(end)
            out: List[Dict[str, Any]] = []
            for entry in stream.get("entries", []):
                eid = parse_stream_id(entry["id"])
                if start_id <= eid <= end_id:
                    out.append({"id": entry["id"], "fields": dict(entry.get("fields", {}))})
                    if count is not None and len(out) >= int(count):
                        break
            return out

    def xread(
        self,
        streams: Dict[Any, str],
        *,
        count: Optional[int] = None,
        block_ms: int = 0,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        deadline = time.time() + (max(0, int(block_ms)) / 1000.0)
        with self._cv:
            resolved_streams: Dict[Any, str] = {}
            for key, last_id in streams.items():
                if str(last_id) == "$":
                    stream = self._stream(key)
                    resolved_streams[key] = stream.get("last_id", "0-0") if stream is not None else "0-0"
                else:
                    resolved_streams[key] = str(last_id)
            while True:
                out = self._xread_locked(resolved_streams, count=count)
                if out or block_ms <= 0:
                    return out
                remaining = deadline - time.time()
                if remaining <= 0:
                    return {}
                self._cv.wait(timeout=remaining)

    def _xread_locked(self, streams: Dict[Any, str], *, count: Optional[int]) -> Dict[Any, List[Dict[str, Any]]]:
        out: Dict[Any, List[Dict[str, Any]]] = {}
        for key, last_id in streams.items():
            stream = self._stream(key)
            if stream is None:
                continue
            min_id = parse_stream_id(last_id)
            items: List[Dict[str, Any]] = []
            for entry in stream.get("entries", []):
                if parse_stream_id(entry["id"]) > min_id:
                    items.append({"id": entry["id"], "fields": dict(entry.get("fields", {}))})
                    if count is not None and len(items) >= int(count):
                        break
            if items:
                out[key] = items
        return out

    def xgroup_create(self, key: Any, group: str, *, entry_id: str = "$", mkstream: bool = False) -> bool:
        if not group:
            raise ValueError("group must be non-empty")
        with self._lock:
            stream = self._stream(key, create=mkstream)
            if stream is None:
                raise KeyError(key)
            groups = stream.setdefault("groups", {})
            if group in groups:
                raise KeyError(group)
            if entry_id == "$":
                last_id = stream.get("last_id", "0-0")
            else:
                last_id = format_stream_id(parse_stream_id(entry_id))
            groups[group] = {"last_delivered_id": last_id, "pending": {}, "consumers": {}}
            return True

    def xreadgroup(
        self,
        key: Any,
        group: str,
        consumer: str,
        *,
        entry_id: str = ">",
        count: Optional[int] = None,
        block_ms: int = 0,
    ) -> List[Dict[str, Any]]:
        deadline = time.time() + (max(0, int(block_ms)) / 1000.0)
        with self._cv:
            while True:
                out = self._xreadgroup_locked(key, group, consumer, entry_id=entry_id, count=count)
                if out or block_ms <= 0:
                    return out
                remaining = deadline - time.time()
                if remaining <= 0:
                    return []
                self._cv.wait(timeout=remaining)

    def _xreadgroup_locked(
        self,
        key: Any,
        group: str,
        consumer: str,
        *,
        entry_id: str,
        count: Optional[int],
    ) -> List[Dict[str, Any]]:
        stream = self._stream(key)
        if stream is None:
            raise KeyError(key)
        group_state = stream.get("groups", {}).get(group)
        if group_state is None:
            raise KeyError(group)
        consumer_state = group_state.setdefault("consumers", {}).setdefault(consumer, {"pending": [], "seen": 0.0})
        consumer_state["seen"] = time.time()

        out: List[Dict[str, Any]] = []
        if entry_id == ">":
            min_id = parse_stream_id(group_state.get("last_delivered_id", "0-0"))
            for entry in stream.get("entries", []):
                if parse_stream_id(entry["id"]) <= min_id:
                    continue
                out.append({"id": entry["id"], "fields": dict(entry.get("fields", {}))})
                group_state["last_delivered_id"] = entry["id"]
                group_state.setdefault("pending", {})[entry["id"]] = {
                    "consumer": consumer,
                    "delivered_at": time.time(),
                    "deliveries": int(group_state.get("pending", {}).get(entry["id"], {}).get("deliveries", 0)) + 1,
                }
                if entry["id"] not in consumer_state["pending"]:
                    consumer_state["pending"].append(entry["id"])
                if count is not None and len(out) >= int(count):
                    break
            return out

        min_id = parse_stream_id(entry_id)
        pending_for_consumer = list(consumer_state.get("pending", []))
        entry_by_id = {entry["id"]: entry for entry in stream.get("entries", [])}
        for pending_id in pending_for_consumer:
            if parse_stream_id(pending_id) < min_id:
                continue
            entry = entry_by_id.get(pending_id)
            if entry is None:
                continue
            out.append({"id": entry["id"], "fields": dict(entry.get("fields", {}))})
            if count is not None and len(out) >= int(count):
                break
        return out

    def mark_delivered(self, key: Any, group: str, consumer: str, ids: Iterable[str]) -> int:
        with self._lock:
            stream = self._stream(key)
            if stream is None:
                raise KeyError(key)
            group_state = stream.get("groups", {}).get(group)
            if group_state is None:
                raise KeyError(group)
            consumer_state = group_state.setdefault("consumers", {}).setdefault(consumer, {"pending": [], "seen": 0.0})
            consumer_state["seen"] = time.time()
            entry_ids = {entry["id"] for entry in stream.get("entries", [])}
            marked = 0
            for entry_id in ids:
                entry_id = str(entry_id)
                if entry_id not in entry_ids:
                    continue
                previous = group_state.setdefault("pending", {}).get(entry_id, {})
                group_state["pending"][entry_id] = {
                    "consumer": consumer,
                    "delivered_at": time.time(),
                    "deliveries": int(previous.get("deliveries", 0)) + 1,
                }
                if entry_id not in consumer_state["pending"]:
                    consumer_state["pending"].append(entry_id)
                if parse_stream_id(entry_id) > parse_stream_id(group_state.get("last_delivered_id", "0-0")):
                    group_state["last_delivered_id"] = entry_id
                marked += 1
            return marked

    def xack(self, key: Any, group: str, ids: Iterable[str]) -> int:
        with self._lock:
            stream = self._stream(key)
            if stream is None:
                return 0
            group_state = stream.get("groups", {}).get(group)
            if group_state is None:
                return 0
            count = 0
            for entry_id in ids:
                meta = group_state.get("pending", {}).pop(str(entry_id), None)
                if meta is None:
                    continue
                consumer = meta.get("consumer")
                c_state = group_state.get("consumers", {}).get(consumer)
                if c_state:
                    c_state["pending"] = [eid for eid in c_state.get("pending", []) if eid != str(entry_id)]
                count += 1
            return count

    def xpending(self, key: Any, group: str) -> Dict[str, Any]:
        with self._lock:
            stream = self._stream(key)
            if stream is None:
                raise KeyError(key)
            group_state = stream.get("groups", {}).get(group)
            if group_state is None:
                raise KeyError(group)
            pending = group_state.get("pending", {})
            ids = sorted(pending.keys(), key=parse_stream_id)
            consumers: Dict[str, int] = {}
            for meta in pending.values():
                consumer = str(meta.get("consumer", ""))
                consumers[consumer] = consumers.get(consumer, 0) + 1
            return {
                "count": len(ids),
                "min": ids[0] if ids else None,
                "max": ids[-1] if ids else None,
                "consumers": consumers,
            }

    def delete(self, key: Any) -> bool:
        with self._lock:
            return self._streams.pop(key, None) is not None

    def dump(self) -> Dict[str, Any]:
        with self._lock:
            return {
                str(key): {
                    "last_id": stream.get("last_id", "0-0"),
                    "entries": [
                        {"id": entry["id"], "fields": dict(entry.get("fields", {}))}
                        for entry in stream.get("entries", [])
                    ],
                    "groups": stream.get("groups", {}),
                }
                for key, stream in self._streams.items()
            }

    def load(self, data: Dict[str, Any]) -> None:
        with self._cv:
            self._streams.clear()
            if not isinstance(data, dict):
                return
            for key, raw in data.items():
                if not isinstance(raw, dict):
                    continue
                entries = [
                    {"id": str(entry.get("id")), "fields": dict(entry.get("fields", {}) or {})}
                    for entry in raw.get("entries", []) or []
                    if isinstance(entry, dict) and entry.get("id") is not None
                ]
                entries.sort(key=lambda item: parse_stream_id(item["id"]))
                last_id = str(raw.get("last_id") or (entries[-1]["id"] if entries else "0-0"))
                self._streams[key] = {
                    "last_id": last_id,
                    "entries": entries,
                    "groups": raw.get("groups", {}) if isinstance(raw.get("groups", {}), dict) else {},
                }
            self._cv.notify_all()

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            entries = 0
            groups = 0
            pending = 0
            for stream in self._streams.values():
                entries += len(stream.get("entries", []))
                groups += len(stream.get("groups", {}))
                for group in stream.get("groups", {}).values():
                    pending += len(group.get("pending", {}))
            return {"streams": len(self._streams), "entries": entries, "groups": groups, "pending": pending}
