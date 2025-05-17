#!/usr/bin/env
# -*- coding: utf-8 -*-

import time
import unittest

from kvstore import LRUKeyValueStore, ShardedKeyValueStore


class SingleShardTests(unittest.TestCase):
    def setUp(self):
        self.store = LRUKeyValueStore(max_size=2)

    def test_crud(self):
        self.store.create("a", 1)
        self.assertEqual(self.store.read("a"), 1)

        self.store.update("a", 2)
        self.assertEqual(self.store.read("a"), 2)

        self.store.delete("a")
        with self.assertRaises(KeyError):
            self.store.read("a")

    def test_ttl(self):
        self.store.create("x", 99, ttl=0.3)
        time.sleep(0.5)
        with self.assertRaises(KeyError):
            self.store.read("x")

    def test_lru(self):
        self.store.create("p", 1)
        self.store.create("q", 2)
        _ = self.store.read("p")
        self.store.create("r", 3)
        with self.assertRaises(KeyError):
            self.store.read("q")
        self.assertEqual(self.store.read("p"), 1)
        self.assertEqual(self.store.read("r"), 3)

    def test_batch(self):
        self.store.mset({"k1": 1, "k2": 2})
        got = self.store.mget(["k2", "missing"])
        self.assertEqual(got, {"k2": 2})


class ShardTests(unittest.TestCase):
    def test_distribution_and_batch(self):
        s = ShardedKeyValueStore(shards=3, per_shard_max=5)
        s.mset({"a": 1, "b": 2, "c": 3})
        self.assertEqual(s.mget(["a", "c", "x"]), {"a": 1, "c": 3})


if __name__ == "__main__":
    unittest.main(verbosity=2)