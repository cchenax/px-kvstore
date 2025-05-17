#!/usr/bin/env
# -*- coding: utf-8 -*-

import binascii
import time
from collections import defaultdict
from threading import Lock


class _Node(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.prev = None
        self.next = None

class LRUKeyValueStore(object):
    def __init__(self, max_size=None):
        self._lock = Lock()
        self._max = max_size
        self._map = {}
        self._ttl = {}

        self._head = _Node(None, None)
        self._tail = _Node(None, None)
        self._head.next = self._tail
        self._tail.prev = self._head

    def _append(self, node):
        last = self._tail.prev
        last.next = node
        node.prev = last
        node.next = self._tail
        self._tail.prev = node

    def _remove(self, node):
        node.prev.next = node.next
        node.next.prev = node.prev
        node.prev = node.next = None

    def _move_to_tail(self, node):
        self._remove(node)
        self._append(node)

    def _purge_expired(self):
        now = time.time()

        expired = []
        for k, ts in self._ttl.items():
            if ts is not None and ts <= now:
                expired.append(k)

        for k in expired:
            self._delete(k)

    def _evict_if_needed(self):
        while self._max and len(self._map) > self._max:
            lru = self._head.next
            self._delete(lru.key)

    def _delete(self, key):
        node = self._map.pop(key, None)
        if node:
            self._remove(node)
        self._ttl.pop(key, None)

    def create(self, key, value, ttl=None):
        with self._lock:
            self._purge_expired()
            if key in self._map:
                raise KeyError(key)
            node = _Node(key, value)
            self._map[key] = node
            self._append(node)
            if ttl is not None:
                expire_ts = time.time() + ttl
            else:
                expire_ts = None
            self._ttl[key] = expire_ts

            self._evict_if_needed()

    def read(self, key):
        with self._lock:
            self._purge_expired()
            node = self._map.get(key)
            if node is None:
                raise KeyError(key)
            self._move_to_tail(node)
            return node.value

    def update(self, key, value, ttl=None):
        with self._lock:
            self._purge_expired()
            node = self._map.get(key)
            if node is None:
                raise KeyError(key)
            node.value = value
            self._move_to_tail(node)
            if ttl is not None:
                self._ttl[key] = time.time() + ttl
            self._evict_if_needed()

    def delete(self, key):
        with self._lock:
            if key not in self._map:
                raise KeyError(key)
            self._delete(key)

    def mset(self, items, ttl=None):
        now = time.time()
        with self._lock:
            self._purge_expired()
            for k, v in items.items():
                node = self._map.get(k)
                if node:
                    node.value = v
                    self._move_to_tail(node)
                else:
                    node = _Node(k, v)
                    self._map[k] = node
                    self._append(node)
                if ttl is not None:
                    expire_ts = time.time() + ttl
                else:
                    expire_ts = None
                self._ttl[k] = expire_ts
            self._evict_if_needed()

    def mget(self, keys):
        with self._lock:
            self._purge_expired()
            out = {}
            for k in keys:
                node = self._map.get(k)
                if node:
                    self._move_to_tail(node)
                    out[k] = node.value
            return out

class ShardedKeyValueStore(object):

    def __init__(self, shards=4, per_shard_max=1000):
        if shards < 1:
            raise ValueError("shards must be >= 1")
        self._num = shards
        self._shards = [LRUKeyValueStore(per_shard_max) for _ in range(shards)]

    def _idx(self, key):
        if isinstance(key, str):
            key = key.encode("utf-8")
        return binascii.crc32(key) % self._num

    def _bucket(self, key):
        return self._shards[self._idx(key)]

    def create(self, key, value, ttl=None):
        self._bucket(key).create(key, value, ttl)

    def read(self, key):
        return self._bucket(key).read(key)

    def update(self, key, value, ttl=None):
        self._bucket(key).update(key, value, ttl)

    def delete(self, key):
        self._bucket(key).delete(key)

    def mset(self, items, ttl=None):
        grouped = defaultdict(dict)
        for k, v in items.items():
            grouped[self._idx(k)][k] = v
        for idx, sub in grouped.items():
            self._shards[idx].mset(sub, ttl)

    def mget(self, keys):
        grouped = defaultdict(list)
        for k in keys:
            grouped[self._idx(k)].append(k)
        out = {}
        for idx, sub in grouped.items():
            out.update(self._shards[idx].mget(sub))
        return out