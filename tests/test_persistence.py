import pytest
import os
import json
from pxkv.core.sharded import ShardedKeyValueStore
from pxkv.persistence.snapshot import SnapshotManager, load_snapshot
from pxkv.persistence.wal import recover_from_wal
from pxkv.config.settings import settings

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

def test_wal_rotation_after_snapshot(tmp_path, monkeypatch):
    wal_path = str(tmp_path / "wal.log")
    snap_path = str(tmp_path / "snap.json")

    monkeypatch.setattr(settings, "WAL_ROTATE_ENABLED", True, raising=False)
    monkeypatch.setattr(settings, "WAL_ROTATE_KEEP", 0, raising=False)

    store = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path=wal_path)
    store.incr("c1", 1)
    store.incr("c1", 2)

    manager = SnapshotManager(store, snap_path, 60)
    manager.snapshot_once()
    assert os.path.exists(snap_path)

    snapshot_payload = json.loads(open(snap_path, "r").read())
    snapshot_lsn = int(snapshot_payload.get("_lsn", 0))
    assert snapshot_lsn > 0
    assert store._wal.get_oldest_lsn() == snapshot_lsn + 1

    store.incr("c1", 3)
    assert store._wal._lsn == snapshot_lsn + 1

    new_store = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path=wal_path)
    assert load_snapshot(new_store, snap_path) is True
    recover_from_wal(new_store, new_store._wal)
    assert new_store.read("c1") == 6.0
