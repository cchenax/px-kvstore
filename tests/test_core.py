import pytest
import time
from pxkv.core.lru import LRUKeyValueStore
from pxkv.core.lfu import LFUKeyValueStore
from pxkv.core.sharded import ShardedKeyValueStore

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
        _ = lru_store.read("p")  # Touch 'p'
        lru_store.create("r", 3)  # Should evict 'q'
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
        # Touch 'q' more times than 'p'
        _ = lfu_store.read("q")
        _ = lfu_store.read("q")
        lfu_store.create("r", 3)  # Should evict 'p'
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
        # Note: the order in scan depends on the K-way merge of sorted shard keys
        results = sharded_store.scan(prefix="fo", limit=10)
        assert "foo" in results
        assert "foo2" in results
        assert "fop" in results

    def test_incr(self, sharded_store):
        assert sharded_store.incr("counter", 1) == 1.0
        assert sharded_store.incr("counter", 5.5) == 6.5
        assert sharded_store.read("counter") == 6.5
