import pytest
import os
import json
from pxkv.core.sharded import ShardedKeyValueStore
from pxkv.persistence.snapshot import SnapshotManager, load_snapshot

@pytest.fixture
def store():
    return ShardedKeyValueStore(shards=2, per_shard_max=10)

def test_snapshot_restore(store, tmp_path):
    snapshot_path = str(tmp_path / "test.json")
    store.create("k1", "v1", ttl=3600)
    store.create("k2", {"complex": "data"})
    
    manager = SnapshotManager(store, snapshot_path, 60)
    manager.snapshot_once()
    
    assert os.path.exists(snapshot_path)
    
    new_store = ShardedKeyValueStore(shards=2, per_shard_max=10)
    load_snapshot(new_store, snapshot_path)
    
    assert new_store.read("k1") == "v1"
    assert new_store.read("k2") == {"complex": "data"}

def test_snapshot_invalid_path():
    store = ShardedKeyValueStore()
    assert load_snapshot(store, "/nonexistent/path") is False
