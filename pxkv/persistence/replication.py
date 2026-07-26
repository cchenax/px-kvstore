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
from .. import tracing
from .gossip import get_gossip_membership

def _http_get_json(url: str, timeout: float) -> Tuple[int, Any, str]:
    headers: Dict[str, str] = {}
    tracing.inject_headers(headers)
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
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
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    tracing.inject_headers(headers)
    req = urllib.request.Request(
        url,
        data=data,
        headers=headers,
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

        max_size = int(getattr(settings, "REPLICATION_QUEUE_MAX", 0) or 0)
        if max_size > 0:
            self.replication_queue = queue.Queue(maxsize=max_size)
        else:
            self.replication_queue = queue.Queue()
        self.followers = [f for f in settings.REPLICATION_FOLLOWERS if f]
        self._follower_ack_lsn: Dict[str, int] = {f: 0 for f in self.followers}
        self._queue_max = max(0, max_size)
        self._shed_policy = str(getattr(settings, "REPLICATION_SHED_POLICY", "drop_newest") or "drop_newest").lower()
        registry.observe_replication_queue(depth=self.replication_queue.qsize(), max_size=self._queue_max)
        
        self.leader_addr = settings.REPLICATION_LEADER_ADDR
        self._last_applied_lsn = 0
        self._last_applied_at = 0.0
        self._known_leader_lsn = 0
        
        # Gossip integration
        self._initialize_gossip()

    def _initialize_gossip(self) -> None:
        if not getattr(settings, "GOSSIP_ENABLED", False):
            return

        gossip = get_gossip_membership()
        if gossip:
            gossip.on_membership_change(self._on_membership_change)
            if self.role == "leader":
                # For leader: all alive peers except itself are potential followers
                alive = gossip.get_alive_peers()
                # Filter out ourself from followers list
                if gossip.self_addr in alive:
                    alive.remove(gossip.self_addr)
                # Update followers
                self._update_followers(alive)
            elif self.role == "follower":
                # For follower: find potential leaders
                pass

    def _on_membership_change(self, alive_peers: List[str]) -> None:
        if self.role == "leader":
            # Update followers list from gossip
            # All alive peers except ourselves
            gossip = get_gossip_membership()
            if gossip:
                filtered = [p for p in alive_peers if p != gossip.self_addr]
                self._update_followers(filtered)
        elif self.role == "follower":
            # For follower: update followers list from gossip for follower reads
            gossip = get_gossip_membership()
            if gossip:
                filtered = [p for p in alive_peers if p != gossip.self_addr]
                self.followers = filtered

    def _update_followers(self, new_followers: List[str]) -> None:
        existing = set(self.followers)
        new = set(new_followers)
        
        added = new - existing
        removed = existing - new
        
        for peer in added:
            if peer not in self._follower_ack_lsn:
                self._follower_ack_lsn[peer] = 0
            logging.info(f"New follower discovered via gossip: {peer}")
        
        for peer in removed:
            if peer in self._follower_ack_lsn:
                del self._follower_ack_lsn[peer]
            logging.info(f"Follower removed: {peer} removed from follower list")
        
        self.followers = list(new_followers)

    def start(self):
        if self.role == "leader":
            if self.followers:
                logging.info("Starting replication as LEADER. Followers: %s", self.followers)
                threading.Thread(target=self._leader_replication_loop, daemon=True).start()
        else:
            logging.info("Starting replication as FOLLOWER. Leader: %s", self.leader_addr)
            threading.Thread(target=self._follower_replication_loop, daemon=True).start()
            if getattr(settings, "ANTI_ENTROPY_ENABLED", True):
                threading.Thread(target=self._anti_entropy_loop, daemon=True).start()

    def _anti_entropy_loop(self):
        interval = float(getattr(settings, "ANTI_ENTROPY_INTERVAL", 60.0))
        max_lag_lsn = int(getattr(settings, "ANTI_ENTROPY_MAX_LAG_LSN", 100000))
        max_age_ms = float(getattr(settings, "ANTI_ENTROPY_MAX_AGE_MS", 300000.0))
        logging.info(
            "Anti-entropy (read repair / divergence repair) enabled (interval=%.1fs, max_lag=%d, max_age=%.1fms)",
            interval, max_lag_lsn, max_age_ms
        )
        while not self._stop_event.is_set():
            time.sleep(interval)
            if self._stop_event.is_set():
                break
            try:
                with tracing.start_span(
                    "replication.follower.anti_entropy",
                    attributes={
                        "pxkv.replication.leader": self.leader_addr,
                    },
                    kind="client",
                ) as ae_span:
                    # Check if we need full sync repair
                    need_repair = False
                    reason = ""

                    now = time.time()
                    lag_lsn = max(0, int(self._known_leader_lsn) - int(self._last_applied_lsn))
                    age_ms = 0.0
                    if self._last_applied_at > 0:
                        age_ms = max(0.0, (now - float(self._last_applied_at)) * 1000.0)

                    if lag_lsn > max_lag_lsn:
                        need_repair = True
                        reason = f"lag_lsn={lag_lsn} > max_lag={max_lag_lsn}"
                    elif age_ms > max_age_ms:
                        need_repair = True
                        reason = f"last_applied_age_ms={age_ms:.0f} > max_age={max_age_ms:.0f}"

                    tracing.set_attribute(ae_span, "pxkv.anti_entropy.lag_lsn", lag_lsn)
                    tracing.set_attribute(ae_span, "pxkv.anti_entropy.last_applied_age_ms", age_ms)
                    tracing.set_attribute(ae_span, "pxkv.anti_entropy.need_repair", need_repair)
                    if need_repair:
                        tracing.set_attribute(ae_span, "pxkv.anti_entropy.reason", reason)
                        logging.info("Anti-entropy triggering full sync repair: %s", reason)
                        self._initial_full_sync()
                        logging.info("Anti-entropy full sync repair completed. last_applied_lsn=%d", self._last_applied_lsn)
            except Exception as e:
                logging.error("Anti-entropy error: %s", e)

    def _initial_full_sync(self):
        logging.info("Performing initial full sync from leader: %s", self.leader_addr)
        max_retries = 5
        for i in range(max_retries):
            with tracing.start_span(
                "replication.follower.initial_full_sync",
                attributes={
                    "pxkv.replication.leader": self.leader_addr,
                    "pxkv.replication.attempt": i + 1,
                },
                kind="client",
            ) as sync_span:
                try:
                    url = f"http://{self.leader_addr}/replication/snapshot?format=ndjson&compress=gzip"
                    req_headers = {"Accept-Encoding": "gzip"}
                    tracing.inject_headers(req_headers)
                    req = urllib.request.Request(url, headers=req_headers, method="GET")
                    with urllib.request.urlopen(req, timeout=10.0) as resp:
                        status = int(getattr(resp, "status", 0) or 0)
                        tracing.set_attribute(sync_span, "http.status_code", status)
                        if status != 200:
                            raise RuntimeError(f"snapshot status={status}")
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
                            if "vectors" in rec and isinstance(rec.get("vectors"), dict):
                                try:
                                    self.store._vector_index.load(rec["vectors"])
                                except Exception:
                                    logging.warning("Failed to load vector index from snapshot")
                                continue
                            if "streams" in rec and isinstance(rec.get("streams"), dict):
                                try:
                                    self.store._streams.load(rec["streams"])
                                except Exception:
                                    logging.warning("Failed to load streams from snapshot")
                                continue
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
                        tracing.set_attribute(sync_span, "pxkv.replication.last_applied_lsn", lsn)
                        logging.info("Initial full sync completed successfully. LSN: %d", lsn)
                        return
                except Exception as e:
                    tracing.set_status_error(sync_span, str(e))
                    try:
                        url = f"http://{self.leader_addr}/replication/snapshot"
                        status, data, text = _http_get_json(url, timeout=5.0)
                        if status == 200 and isinstance(data, dict):
                            lsn = int(data.pop("_lsn", 0) or 0)
                            self.store.load(data)
                            self._last_applied_lsn = lsn
                            self._last_applied_at = time.time()
                            self._known_leader_lsn = max(self._known_leader_lsn, lsn)
                            tracing.set_attribute(sync_span, "pxkv.replication.last_applied_lsn", lsn)
                            logging.info("Initial full sync completed successfully (json fallback). LSN: %d", lsn)
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

    def enqueue_change(
        self,
        op: str,
        key: Any,
        value: Any = None,
        ttl: Optional[float] = None,
        lsn: int = 0,
        origin_cluster_id: Optional[str] = None,
        origin_ts: Optional[float] = None,
    ):
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

            item = {
                "lsn": lsn,
                "op": op,
                "key": serialized_key,
                "value": serialized_val,
                "ttl": ttl,
                "ts": time.time(),
                "origin_cluster_id": origin_cluster_id or getattr(settings, "CLUSTER_ID", "local"),
                "origin_ts": origin_ts if origin_ts is not None else time.time(),
            }

            try:
                self.replication_queue.put_nowait(item)
            except queue.Full:
                policy = self._shed_policy
                if policy == "drop_oldest":
                    try:
                        _ = self.replication_queue.get_nowait()
                    except Exception:
                        registry.inc_replication_drop(policy=policy, reason="queue_full_drop_oldest_failed")
                        registry.observe_replication_queue(depth=self.replication_queue.qsize(), max_size=self._queue_max)
                        return
                    try:
                        self.replication_queue.put_nowait(item)
                    except Exception:
                        registry.inc_replication_drop(policy=policy, reason="queue_full_drop_oldest_put_failed")
                        registry.observe_replication_queue(depth=self.replication_queue.qsize(), max_size=self._queue_max)
                        return
                    registry.inc_replication_drop(policy=policy, reason="queue_full_drop_oldest")
                    registry.observe_replication_queue(depth=self.replication_queue.qsize(), max_size=self._queue_max)
                    return

                registry.inc_replication_drop(policy="drop_newest", reason="queue_full_drop_newest")
                registry.observe_replication_queue(depth=self.replication_queue.qsize(), max_size=self._queue_max)
                return

            registry.observe_replication_queue(depth=self.replication_queue.qsize(), max_size=self._queue_max)

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
                registry.observe_replication_queue(depth=self.replication_queue.qsize(), max_size=self._queue_max)

                leader_lsn = int(getattr(self.store._wal, "_lsn", 0) or 0)
                with tracing.start_span(
                    "replication.leader.dispatch_batch",
                    attributes={
                        "pxkv.replication.batch_size": len(changes),
                        "pxkv.replication.followers": len(self.followers),
                        "pxkv.replication.leader_lsn": leader_lsn,
                    },
                    kind="producer",
                ):
                    for follower in self.followers:
                        with tracing.start_span(
                            "replication.leader.sync_follower",
                            attributes={
                                "pxkv.replication.follower": follower,
                                "pxkv.replication.batch_size": len(changes),
                                "pxkv.replication.leader_lsn": leader_lsn,
                            },
                            kind="client",
                        ) as fspan:
                            url = f"http://{follower}/replication/sync"
                            status, text = _http_post_json(url, {"changes": changes, "leader_lsn": leader_lsn}, timeout=2.0)
                            tracing.set_attribute(fspan, "http.status_code", status)
                            ack_lsn = self._follower_ack_lsn.get(follower, 0)
                            if status == 200:
                                try:
                                    payload = json.loads(text) if text else {}
                                except ValueError:
                                    payload = {}
                                ack_lsn = int(payload.get("last_applied_lsn", ack_lsn) or ack_lsn)
                                self._follower_ack_lsn[follower] = ack_lsn
                                tracing.set_attribute(fspan, "pxkv.replication.ack_lsn", ack_lsn)
                                registry.observe_replication_ack(
                                    follower=follower,
                                    leader_lsn=leader_lsn,
                                    ack_lsn=ack_lsn,
                                    ok=True,
                                )
                                continue
                            if status not in (200, 0):
                                logging.warning("Follower sync returned %s for %s", status, follower)
                            tracing.set_status_error(fspan, f"sync failed status={status}")
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
            with tracing.start_span(
                "replication.follower.pull_wal",
                attributes={
                    "pxkv.replication.leader": self.leader_addr,
                    "pxkv.replication.start_lsn": int(self._last_applied_lsn),
                },
                kind="client",
            ) as pspan:
                try:
                    url = f"http://{self.leader_addr}/replication/wal?start_lsn={self._last_applied_lsn}"
                    status, data, _text = _http_get_json(url, timeout=2.0)
                    tracing.set_attribute(pspan, "http.status_code", status)
                    if status == 200 and isinstance(data, dict):
                        self.set_known_leader_lsn(int(data.get("leader_lsn", 0) or 0))
                        changes = data.get("changes", [])
                        if isinstance(changes, list) and changes:
                            tracing.set_attribute(pspan, "pxkv.replication.batch_size", len(changes))
                            self.apply_changes(changes)
                    elif status == 410:
                        tracing.set_attribute(pspan, "pxkv.replication.wal_truncated", True)
                        self._initial_full_sync()
                except Exception as e:
                    tracing.set_status_error(pspan, str(e))
                    logging.debug("Follower catch-up error: %s", e)

            time.sleep(settings.REPLICATION_SYNC_INTERVAL)

    def apply_changes(self, changes: List[Dict[str, Any]]):
        if self.role != "follower":
            return

        with tracing.start_span(
            "replication.follower.apply_changes",
            attributes={
                "pxkv.replication.batch_size": len(changes),
                "pxkv.replication.last_applied_lsn": int(self._last_applied_lsn),
            },
            kind="consumer",
        ):
            self._apply_changes_impl(changes)

    def _apply_changes_impl(self, changes: List[Dict[str, Any]]) -> None:
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
            origin_cluster_id = change.get("origin_cluster_id")
            origin_ts = change.get("origin_ts")
            
            try:
                if op in ("create", "update"):
                    should_apply, resolved_val, resolved_ttl = self.store.resolve_conflict(
                        key, val, ttl,
                        new_origin_cluster_id=origin_cluster_id,
                        new_origin_ts=origin_ts,
                    )
                    if should_apply:
                        try:
                            self.store.create(
                                key, resolved_val, resolved_ttl,
                                skip_replication=True,
                                origin_cluster_id=origin_cluster_id,
                                origin_ts=origin_ts,
                            )
                        except KeyError:
                            self.store.update(
                                key, resolved_val, resolved_ttl,
                                skip_replication=True,
                                origin_cluster_id=origin_cluster_id,
                                origin_ts=origin_ts,
                            )
                elif op == "delete":
                    try:
                        self.store.delete(key, skip_replication=True)
                    except KeyError:
                        pass
                elif op == "mset":
                    if isinstance(key, dict):
                        should_apply_all = True
                        for k, v in key.items():
                            sa, _, _ = self.store.resolve_conflict(
                                k, v, ttl,
                                new_origin_cluster_id=origin_cluster_id,
                                new_origin_ts=origin_ts,
                            )
                            if not sa:
                                should_apply_all = False
                                break
                        if should_apply_all:
                            self.store.mset(
                                key, ttl,
                                skip_replication=True,
                                origin_cluster_id=origin_cluster_id,
                                origin_ts=origin_ts,
                            )
                elif op == "incr":
                    self.store.incr(key, val, ttl, skip_replication=True)
                elif op == "persist":
                    try:
                        self.store.persist(key, skip_replication=True)
                    except KeyError:
                        pass
                elif op == "vector_upsert":
                    self.store.vector_upsert(
                        key,
                        val,
                        skip_replication=True,
                        origin_cluster_id=origin_cluster_id,
                        origin_ts=origin_ts,
                    )
                elif op == "vector_delete":
                    self.store.vector_delete(key, skip_replication=True)
                elif op == "stream_xadd":
                    payload = val if isinstance(val, dict) else {}
                    self.store.stream_xadd(
                        key,
                        payload.get("fields", {}),
                        entry_id=str(payload.get("id", "*")),
                        maxlen=payload.get("maxlen"),
                        skip_replication=True,
                    )
                elif op == "stream_xgroup_create":
                    payload = val if isinstance(val, dict) else {}
                    try:
                        self.store.stream_xgroup_create(
                            key,
                            str(payload.get("group", "")),
                            entry_id=str(payload.get("id", "$")),
                            mkstream=bool(payload.get("mkstream", False)),
                            skip_replication=True,
                        )
                    except KeyError:
                        pass
                elif op == "stream_xack":
                    payload = val if isinstance(val, dict) else {}
                    self.store.stream_xack(
                        key,
                        str(payload.get("group", "")),
                        payload.get("ids", []) or [],
                        skip_replication=True,
                    )
                elif op == "stream_xdeliver":
                    payload = val if isinstance(val, dict) else {}
                    try:
                        self.store.stream_xdeliver(
                            key,
                            str(payload.get("group", "")),
                            str(payload.get("consumer", "")),
                            payload.get("ids", []) or [],
                            skip_replication=True,
                        )
                    except KeyError:
                        pass
                elif op == "stream_delete":
                    self.store.stream_delete(key, skip_replication=True)

                self._last_applied_lsn = lsn
                last_applied = True
            except Exception as e:
                logging.error("Follower failed to apply change LSN %d: %s", lsn, e)
        if last_applied:
            self._last_applied_at = time.time()
