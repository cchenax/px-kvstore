import pytest
import time
import tempfile
from pxkv.core.lru import LRUKeyValueStore
from pxkv.core.lfu import LFUKeyValueStore
from pxkv.core.sharded import ShardedKeyValueStore
from pxkv.tiering.file import FileTieringBackend

@pytest.fixture
def lru_store():
    return LRUKeyValueStore(max_size=2)

@pytest.fixture
def lfu_store():
    return LFUKeyValueStore(max_size=2)

@pytest.fixture
def sharded_store():
    return ShardedKeyValueStore(shards=3, per_shard_max=5)

class TestLRUStore:
    def test_crud(self, lru_store):
        lru_store.create("a", 1)
        assert lru_store.read("a") == 1

        lru_store.update("a", 2)
        assert lru_store.read("a") == 2

        lru_store.delete("a")
        with pytest.raises(KeyError):
            lru_store.read("a")

    def test_ttl(self, lru_store):
        lru_store.create("x", 99, ttl=0.1)
        time.sleep(0.2)
        with pytest.raises(KeyError):
            lru_store.read("x")

    def test_eviction(self, lru_store):
        lru_store.create("p", 1)
        lru_store.create("q", 2)
        _ = lru_store.read("p")
        lru_store.create("r", 3)
        with pytest.raises(KeyError):
            lru_store.read("q")
        assert lru_store.read("p") == 1
        assert lru_store.read("r") == 3

class TestLFUStore:
    def test_crud(self, lfu_store):
        lfu_store.create("a", 1)
        assert lfu_store.read("a") == 1

        lfu_store.update("a", 2)
        assert lfu_store.read("a") == 2

        lfu_store.delete("a")
        with pytest.raises(KeyError):
            lfu_store.read("a")

    def test_eviction(self, lfu_store):
        lfu_store.create("p", 1)
        lfu_store.create("q", 2)
        _ = lfu_store.read("q")
        _ = lfu_store.read("q")
        lfu_store.create("r", 3)
        with pytest.raises(KeyError):
            lfu_store.read("p")
        assert lfu_store.read("q") == 2
        assert lfu_store.read("r") == 3

class TestShardedStore:
    def test_distribution_and_batch(self, sharded_store):
        sharded_store.mset({"a": 1, "b": 2, "c": 3})
        assert sharded_store.mget(["a", "c", "x"]) == {"a": 1, "c": 3}

    def test_scan_prefix_and_pagination(self, sharded_store):
        sharded_store.mset({"foo": 1, "foo2": 2, "bar": 3, "fop": 4})
        assert sorted(sharded_store.scan(prefix="fo", limit=10)) == ["foo", "foo2", "fop"]
        results = sharded_store.scan(prefix="fo", limit=10)
        assert "foo" in results
        assert "foo2" in results
        assert "fop" in results

    def test_incr(self, sharded_store):
        assert sharded_store.incr("counter", 1) == 1.0
        assert sharded_store.incr("counter", 5.5) == 6.5
        assert sharded_store.read("counter") == 6.5

    def test_hash_tag_routing(self, sharded_store):
        k1 = "user:{42}:profile"
        k2 = "user:{42}:settings"
        assert sharded_store.shard_for_key(k1) == sharded_store.shard_for_key(k2)

    def test_scan_cursor_full_traversal(self):
        store = ShardedKeyValueStore(shards=4, per_shard_max=1000)
        items = {f"k{i:04d}": i for i in range(200)}
        store.mset(items)

        seen: list[str] = []
        cursor = "0"
        steps = 0
        while True:
            cursor, keys = store.scan_cursor(cursor, count=30)
            seen.extend(keys)
            steps += 1
            if cursor == "0":
                break
            assert steps < 1000, "cursor did not terminate"

        assert sorted(seen) == sorted(items.keys())
        assert len(seen) == len(set(seen)), "cursor must not yield duplicates within a single pass"

    def test_scan_cursor_match_filter(self):
        store = ShardedKeyValueStore(shards=3, per_shard_max=1000)
        store.mset({"user:1": 1, "user:2": 2, "post:1": 3, "user:3": 4, "post:2": 5})

        seen: list[str] = []
        cursor = "0"
        while True:
            cursor, keys = store.scan_cursor(cursor, match="user:*", count=2)
            seen.extend(keys)
            if cursor == "0":
                break
        assert sorted(seen) == ["user:1", "user:2", "user:3"]

    def test_scan_cursor_prefix(self):
        store = ShardedKeyValueStore(shards=3, per_shard_max=1000)
        store.mset({"foo": 1, "foo2": 2, "bar": 3, "fop": 4, "baz": 5})

        seen: list[str] = []
        cursor = "0"
        while True:
            cursor, keys = store.scan_cursor(cursor, prefix="fo", count=1)
            seen.extend(keys)
            if cursor == "0":
                break
        assert sorted(seen) == ["foo", "foo2", "fop"]

    def test_scan_cursor_empty_store(self):
        store = ShardedKeyValueStore(shards=2, per_shard_max=10)
        cursor, keys = store.scan_cursor("0", count=10)
        assert cursor == "0"
        assert keys == []

    def test_scan_cursor_invalid_cursor_restarts(self):
        store = ShardedKeyValueStore(shards=2, per_shard_max=10)
        store.mset({"a": 1, "b": 2})
        cursor, keys = store.scan_cursor("not-a-valid-cursor", count=10)
        assert sorted(keys) == ["a", "b"]
        assert cursor == "0"


def test_tiering_spill_and_promote_lru():
    with tempfile.TemporaryDirectory() as d:
        tiering = FileTieringBackend(d)
        s = LRUKeyValueStore(max_size=1, tiering=tiering)
        s.create("a", "va")
        s.create("b", "vb")
        assert s.read("a") == "va"


def test_tiering_spill_and_promote_lfu():
    with tempfile.TemporaryDirectory() as d:
        tiering = FileTieringBackend(d)
        s = LFUKeyValueStore(max_size=1, tiering=tiering)
        s.create("a", "va")
        s.create("b", "vb")
        assert s.read("a") == "va"
