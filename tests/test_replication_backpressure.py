import pytest

from pxkv.core.sharded import ShardedKeyValueStore
from pxkv.config.settings import settings
from pxkv.metrics.registry import registry


def test_replication_queue_is_bounded_and_drops(monkeypatch):
    monkeypatch.setattr(settings, "REPLICATION_ROLE", "leader", raising=False)
    monkeypatch.setattr(settings, "REPLICATION_FOLLOWERS", ["follower:1"], raising=False)
    monkeypatch.setattr(settings, "REPLICATION_QUEUE_MAX", 1, raising=False)
    monkeypatch.setattr(settings, "REPLICATION_SHED_POLICY", "drop_newest", raising=False)

    store = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path="", tiering_dir="")
    for i in range(10):
        store.create(f"k{i}", "v")

    repl = registry.get_all().get("replication", {})
    q = repl.get("queue", {}) or {}
    assert int(q.get("max", 0)) == 1
    assert 0 <= int(q.get("depth", 0)) <= 1
    assert int(q.get("drops_total", 0)) > 0

