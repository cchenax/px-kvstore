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
        self._last_wal_offset = 0

    def start(self):
        if self.role == "leader":
            if self.followers:
                logging.info("Starting replication as LEADER. Followers: %s", self.followers)
                threading.Thread(target=self._leader_replication_loop, daemon=True).start()
        else:
            logging.info("Starting replication as FOLLOWER. Leader: %s", self.leader_addr)
            threading.Thread(target=self._follower_replication_loop, daemon=True).start()

    def stop(self):
        self._stop_event.set()

    def enqueue_change(self, op: str, key: Any, value: Any = None, ttl: Optional[float] = None):
        """Called by store when a change happens (Leader only)"""
        if self.role == "leader" and self.followers:
            self.replication_queue.put({
                "op": op,
                "key": key,
                "value": value,
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
        """Follower can optionally pull if push is missed, or handle heartbeats"""
        # For now, we rely on Leader pushing via /replication/sync
        # But we could implement a pull-based recovery here if needed
        while not self._stop_event.is_set():
            time.sleep(10.0) # Heartbeat/Health check interval
            # Add health check logic to leader if needed

    def apply_changes(self, changes: List[Dict[str, Any]]):
        """Applied by Follower when receiving changes from Leader"""
        if self.role != "follower":
            return
            
        for change in changes:
            op = change["op"]
            key = change["key"]
            val = change.get("value")
            ttl = change.get("ttl")
            
            # Apply to store bypassing WAL to avoid loops (or handle appropriately)
            # Here we use store methods directly
            try:
                if op == "create":
                    try:
                        self.store.create(key, val, ttl)
                    except KeyError:
                        self.store.update(key, val, ttl)
                elif op == "update":
                    self.store.update(key, val, ttl)
                elif op == "delete":
                    try:
                        self.store.delete(key)
                    except KeyError:
                        pass
                elif op == "mset":
                    self.store.mset(key, ttl)
                elif op == "incr":
                    self.store.incr(key, val, ttl)
            except Exception as e:
                logging.error("Follower failed to apply change %s: %s", change, e)
