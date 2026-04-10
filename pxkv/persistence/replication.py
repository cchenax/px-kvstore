#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import threading
import time
import requests
import queue
from typing import Any, Dict, List, Optional

from ..config.settings import settings

class ReplicationManager:
    """
    Manages asynchronous replication between Leader and Followers.
    """
    def __init__(self, store):
        self.store = store
        self.role = settings.REPLICATION_ROLE
        self._stop_event = threading.Event()
        
        # Leader specific
        self.replication_queue = queue.Queue()
        self.followers = [f for f in settings.REPLICATION_FOLLOWERS if f]
        
        # Follower specific
        self.leader_addr = settings.REPLICATION_LEADER_ADDR
        self._last_applied_lsn = 0

    def start(self):
        if self.role == "leader":
            if self.followers:
                logging.info("Starting replication as LEADER. Followers: %s", self.followers)
                threading.Thread(target=self._leader_replication_loop, daemon=True).start()
        else:
            logging.info("Starting replication as FOLLOWER. Leader: %s", self.leader_addr)
            # Initial Full Sync in background to not block the server startup
            threading.Thread(target=self._follower_replication_loop, daemon=True).start()

    def _initial_full_sync(self):
        """Follower pulls full snapshot from leader on startup with retries"""
        logging.info("Performing initial full sync from leader: %s", self.leader_addr)
        max_retries = 5
        for i in range(max_retries):
            try:
                url = f"http://{self.leader_addr}/replication/snapshot"
                resp = requests.get(url, timeout=5.0)
                if resp.status_code == 200:
                    data = resp.json()
                    lsn = data.pop("_lsn", 0)
                    self.store.load(data)
                    self._last_applied_lsn = lsn
                    logging.info("Initial full sync completed successfully. LSN: %d", lsn)
                    return
                else:
                    logging.error("Failed to pull snapshot from leader: %s", resp.text)
            except Exception as e:
                logging.warning("Initial full sync attempt %d failed: %s", i+1, e)
            
            if i < max_retries - 1:
                time.sleep(2.0)
        
        logging.error("Initial full sync failed after %d retries", max_retries)

    def stop(self):
        self._stop_event.set()

    def enqueue_change(self, op: str, key: Any, value: Any = None, ttl: Optional[float] = None, lsn: int = 0):
        """Called by store when a change happens (Leader only)"""
        if self.role == "leader" and self.followers:
            
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
        """Leader pushes changes to all followers asynchronously"""
        while not self._stop_event.is_set():
            try:
                # Get batch of changes
                changes = []
                try:
                    # Wait for at least one change
                    changes.append(self.replication_queue.get(timeout=1.0))
                    # Collect more if available immediately
                    while len(changes) < 100:
                        changes.append(self.replication_queue.get_nowait())
                except queue.Empty:
                    if not changes:
                        continue

                # Broadcast to all followers
                for follower in self.followers:
                    try:
                        url = f"http://{follower}/replication/sync"
                        requests.post(url, json={"changes": changes}, timeout=2.0)
                    except Exception as e:
                        logging.warning("Failed to sync with follower %s: %s", follower, e)
            except Exception as e:
                logging.error("Leader replication error: %s", e)

    def _follower_replication_loop(self):
        """Follower handles catch-up and monitoring"""
        # 1. Try initial full sync
        self._initial_full_sync()
        
        # 2. Periodically check if we are behind and pull from WAL
        while not self._stop_event.is_set():
            try:
                # Check if we need to pull missing WAL entries
                # This is a backup mechanism in case push failed
                url = f"http://{self.leader_addr}/replication/wal?start_lsn={self._last_applied_lsn}"
                resp = requests.get(url, timeout=2.0)
                if resp.status_code == 200:
                    changes = resp.json().get("changes", [])
                    if changes:
                        logging.info("Pulled %d missed changes from WAL", len(changes))
                        self.apply_changes(changes)
                elif resp.status_code == 410:
                    # Leader says WAL is too old, need full sync
                    logging.warning("Follower too far behind. Triggering full sync.")
                    self._initial_full_sync()
            except Exception as e:
                logging.debug("Follower catch-up error: %s", e)
            
            time.sleep(settings.REPLICATION_SYNC_INTERVAL)

    def apply_changes(self, changes: List[Dict[str, Any]]):
        """Applied by Follower when receiving changes from Leader"""
        if self.role != "follower":
            return
            
        # Sort by LSN just in case
        changes.sort(key=lambda x: x.get("lsn", 0))
        
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
            except Exception as e:
                logging.error("Follower failed to apply change LSN %d: %s", lsn, e)
