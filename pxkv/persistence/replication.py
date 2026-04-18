#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import threading
import time
import queue
import json
import gzip
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional, Tuple

from ..config.settings import settings
from ..metrics.registry import registry

def _http_get_json(url: str, timeout: float) -> Tuple[int, Any, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read()
            text = raw.decode("utf-8", errors="replace")
            if not text:
                return resp.status, None, ""
            return resp.status, json.loads(text), text
    except urllib.error.HTTPError as e:
        try:
            text = e.read().decode("utf-8", errors="replace")
        except Exception:
            text = ""
        return int(getattr(e, "code", 500)), None, text
    except Exception as e:
        return 0, None, str(e)


def _http_post_json(url: str, payload: Dict[str, Any], timeout: float) -> Tuple[int, str]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, raw.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        try:
            text = e.read().decode("utf-8", errors="replace")
        except Exception:
            text = ""
        return int(getattr(e, "code", 500)), text
    except Exception as e:
        return 0, str(e)


class ReplicationManager:
    """
    Manages asynchronous replication between Leader and Followers.
    """
    def __init__(self, store):
        self.store = store
        self.role = settings.REPLICATION_ROLE
        self._stop_event = threading.Event()
        
        self.replication_queue = queue.Queue()
        self.followers = [f for f in settings.REPLICATION_FOLLOWERS if f]
        self._follower_ack_lsn: Dict[str, int] = {f: 0 for f in self.followers}
        
        self.leader_addr = settings.REPLICATION_LEADER_ADDR
        self._last_applied_lsn = 0
        self._last_applied_at = 0.0
        self._known_leader_lsn = 0

    def start(self):
        if self.role == "leader":
            if self.followers:
                logging.info("Starting replication as LEADER. Followers: %s", self.followers)
                threading.Thread(target=self._leader_replication_loop, daemon=True).start()
        else:
            logging.info("Starting replication as FOLLOWER. Leader: %s", self.leader_addr)
            threading.Thread(target=self._follower_replication_loop, daemon=True).start()

    def _initial_full_sync(self):
        logging.info("Performing initial full sync from leader: %s", self.leader_addr)
        max_retries = 5
        for i in range(max_retries):
            try:
                url = f"http://{self.leader_addr}/replication/snapshot?format=ndjson&compress=gzip"
                req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"}, method="GET")
                with urllib.request.urlopen(req, timeout=10.0) as resp:
                    if int(getattr(resp, "status", 0) or 0) != 200:
                        raise RuntimeError(f"snapshot status={getattr(resp, 'status', 0)}")
                    stream: Any = resp
                    if (resp.headers.get("Content-Encoding", "") or "").lower() == "gzip":
                        stream = gzip.GzipFile(fileobj=resp, mode="rb")
                    first = stream.readline()
                    if not first:
                        raise RuntimeError("empty snapshot stream")
                    meta = json.loads(first.decode("utf-8", errors="replace"))
                    lsn = int(meta.get("_lsn", 0) or 0)
                    while True:
                        line = stream.readline()
                        if not line:
                            break
                        rec = json.loads(line.decode("utf-8", errors="replace"))
                        shard_idx = rec.get("shard")
                        state = rec.get("state")
                        if shard_idx is None or state is None:
                            continue
                        idx = int(shard_idx)
                        if 0 <= idx < len(self.store._shards):
                            self.store._shards[idx].load_state(state)
                    self._last_applied_lsn = lsn
                    self._last_applied_at = time.time()
                    self._known_leader_lsn = max(self._known_leader_lsn, lsn)
                    logging.info("Initial full sync completed successfully. LSN: %d", lsn)
                    return
            except Exception as e:
                try:
                    url = f"http://{self.leader_addr}/replication/snapshot"
                    status, data, text = _http_get_json(url, timeout=5.0)
                    if status == 200 and isinstance(data, dict):
                        lsn = int(data.pop("_lsn", 0) or 0)
                        self.store.load(data)
                        self._last_applied_lsn = lsn
                        self._last_applied_at = time.time()
                        self._known_leader_lsn = max(self._known_leader_lsn, lsn)
                        logging.info("Initial full sync completed successfully. LSN: %d", lsn)
                        return
                    logging.warning("Initial full sync attempt %d failed: %s %s", i + 1, status, text)
                except Exception:
                    logging.warning("Initial full sync attempt %d failed: %s", i + 1, e)
            
            if i < max_retries - 1:
                time.sleep(2.0)
        
        logging.error("Initial full sync failed after %d retries", max_retries)

    def stop(self):
        self._stop_event.set()

    def set_known_leader_lsn(self, lsn: int) -> None:
        self._known_leader_lsn = max(int(lsn or 0), self._known_leader_lsn)

    def get_staleness(self) -> Dict[str, Any]:
        now = time.time()
        lag_lsn = max(0, int(self._known_leader_lsn) - int(self._last_applied_lsn))
        age_ms = 0.0
        if self._last_applied_at > 0:
            age_ms = max(0.0, (now - float(self._last_applied_at)) * 1000.0)
        return {
            "role": self.role,
            "last_applied_lsn": int(self._last_applied_lsn),
            "known_leader_lsn": int(self._known_leader_lsn),
            "lag_lsn": int(lag_lsn),
            "last_applied_age_ms": float(age_ms),
        }

    def enqueue_change(self, op: str, key: Any, value: Any = None, ttl: Optional[float] = None, lsn: int = 0):
        """Called by store when a change happens (Leader only)"""
        if self.role == "leader" and self.followers:
            registry.set_replication_leader_lsn(lsn)
            
            def _serialize(obj):
                if isinstance(obj, (bytes, bytearray)):
                    return obj.decode("utf-8", errors="replace")
                return obj

            serialized_val = _serialize(value) if value is not None else None
            serialized_key = key
            if op == "mset" and isinstance(key, dict):
                serialized_key = {k: _serialize(v) for k, v in key.items()}
            
            self.replication_queue.put({
                "lsn": lsn,
                "op": op,
                "key": serialized_key,
                "value": serialized_val,
                "ttl": ttl,
                "ts": time.time()
            })

    def _leader_replication_loop(self):
        while not self._stop_event.is_set():
            try:
                changes = []
                try:
                    changes.append(self.replication_queue.get(timeout=1.0))
                    while len(changes) < 100:
                        changes.append(self.replication_queue.get_nowait())
                except queue.Empty:
                    if not changes:
                        continue

                for follower in self.followers:
                    url = f"http://{follower}/replication/sync"
                    leader_lsn = int(getattr(self.store._wal, "_lsn", 0) or 0)
                    status, text = _http_post_json(url, {"changes": changes, "leader_lsn": leader_lsn}, timeout=2.0)
                    ack_lsn = self._follower_ack_lsn.get(follower, 0)
                    if status == 200:
                        try:
                            payload = json.loads(text) if text else {}
                        except ValueError:
                            payload = {}
                        ack_lsn = int(payload.get("last_applied_lsn", ack_lsn) or ack_lsn)
                        self._follower_ack_lsn[follower] = ack_lsn
                        registry.observe_replication_ack(
                            follower=follower,
                            leader_lsn=leader_lsn,
                            ack_lsn=ack_lsn,
                            ok=True,
                        )
                        continue
                    if status not in (200, 0):
                        logging.warning("Follower sync returned %s for %s", status, follower)
                    registry.observe_replication_ack(
                        follower=follower,
                        leader_lsn=leader_lsn,
                        ack_lsn=ack_lsn,
                        ok=False,
                        error=f"status={status} detail={text[:120]}",
                    )
            except Exception as e:
                logging.error("Leader replication error: %s", e)

    def _follower_replication_loop(self):
        self._initial_full_sync()
        
        while not self._stop_event.is_set():
            try:
                url = f"http://{self.leader_addr}/replication/wal?start_lsn={self._last_applied_lsn}"
                status, data, _text = _http_get_json(url, timeout=2.0)
                if status == 200 and isinstance(data, dict):
                    self.set_known_leader_lsn(int(data.get("leader_lsn", 0) or 0))
                    changes = data.get("changes", [])
                    if isinstance(changes, list) and changes:
                        self.apply_changes(changes)
                elif status == 410:
                    self._initial_full_sync()
            except Exception as e:
                logging.debug("Follower catch-up error: %s", e)
            
            time.sleep(settings.REPLICATION_SYNC_INTERVAL)

    def apply_changes(self, changes: List[Dict[str, Any]]):
        if self.role != "follower":
            return
            
        changes.sort(key=lambda x: x.get("lsn", 0))
        
        last_applied = False
        for change in changes:
            lsn = change.get("lsn", 0)
            if lsn <= self._last_applied_lsn:
                continue
                
            op = change["op"]
            key = change["key"]
            val = change.get("value")
            ttl = change.get("ttl")
            
            try:
                if op == "create":
                    try:
                        self.store.create(key, val, ttl, skip_replication=True)
                    except KeyError:
                        self.store.update(key, val, ttl, skip_replication=True)
                elif op == "update":
                    self.store.update(key, val, ttl, skip_replication=True)
                elif op == "delete":
                    try:
                        self.store.delete(key, skip_replication=True)
                    except KeyError:
                        pass
                elif op == "mset":
                    self.store.mset(key, ttl, skip_replication=True)
                elif op == "incr":
                    self.store.incr(key, val, ttl, skip_replication=True)
                
                self._last_applied_lsn = lsn
                last_applied = True
            except Exception as e:
                logging.error("Follower failed to apply change LSN %d: %s", lsn, e)
        if last_applied:
            self._last_applied_at = time.time()
