import threading
import time

from pxkv.api.redis_server import RedisServer, encode_array
from pxkv.core.sharded import ShardedKeyValueStore
from pxkv.persistence.wal import recover_from_wal


def test_stream_xadd_xrange_xread_and_trim():
    store = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path="", tiering_dir="")

    id1 = store.stream_xadd("events", {"type": "created"}, entry_id="100-0")
    id2 = store.stream_xadd("events", {"type": "updated"}, entry_id="100-*")
    id3 = store.stream_xadd("events", {"type": "deleted"}, entry_id="101-0", maxlen=2)

    assert id1 == "100-0"
    assert id2 == "100-1"
    assert id3 == "101-0"
    assert [entry["id"] for entry in store.stream_xrange("events")] == ["100-1", "101-0"]
    assert [entry["id"] for entry in store.stream_xread({"events": "100-1"})["events"]] == ["101-0"]


def test_stream_xread_dollar_blocks_for_future_entries():
    store = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path="", tiering_dir="")
    store.stream_xadd("events", {"type": "existing"}, entry_id="1-0")
    result = {}

    def reader():
        result.update(store.stream_xread({"events": "$"}, block_ms=500))

    thread = threading.Thread(target=reader)
    thread.start()
    time.sleep(0.05)
    store.stream_xadd("events", {"type": "future"}, entry_id="2-0")
    thread.join(timeout=1)

    assert result["events"] == [{"id": "2-0", "fields": {"type": "future"}}]


def test_stream_consumer_group_pending_and_ack():
    store = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path="", tiering_dir="")
    id1 = store.stream_xadd("events", {"n": 1}, entry_id="1-0")
    store.stream_xadd("events", {"n": 2}, entry_id="2-0")
    assert store.stream_xgroup_create("events", "workers", entry_id="0-0")

    entries = store.stream_xreadgroup("events", "workers", "alice", count=1)
    assert [entry["id"] for entry in entries] == [id1]
    assert store.stream_xpending("events", "workers") == {
        "count": 1,
        "min": id1,
        "max": id1,
        "consumers": {"alice": 1},
    }

    assert store.stream_xack("events", "workers", [id1]) == 1
    assert store.stream_xpending("events", "workers")["count"] == 0


def test_stream_dump_load_restores_entries_and_group_state():
    store = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path="", tiering_dir="")
    id1 = store.stream_xadd("events", {"n": 1}, entry_id="1-0")
    store.stream_xgroup_create("events", "workers", entry_id="0-0")
    store.stream_xreadgroup("events", "workers", "alice")

    restored = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path="", tiering_dir="")
    restored.load(store.dump())

    assert restored.stream_xrange("events") == [{"id": id1, "fields": {"n": 1}}]
    assert restored.stream_xpending("events", "workers")["consumers"] == {"alice": 1}


def test_stream_wal_replays_delivery_and_ack(tmp_path):
    wal_path = str(tmp_path / "stream.wal")
    store = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path=wal_path, tiering_dir="")
    id1 = store.stream_xadd("events", {"n": 1}, entry_id="1-0")
    store.stream_xgroup_create("events", "workers", entry_id="0-0")
    store.stream_xreadgroup("events", "workers", "alice")

    restored = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path=wal_path, tiering_dir="")
    recover_from_wal(restored, restored._wal)
    assert restored.stream_xpending("events", "workers")["count"] == 1

    store.stream_xack("events", "workers", [id1])
    restored_after_ack = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path=wal_path, tiering_dir="")
    recover_from_wal(restored_after_ack, restored_after_ack._wal)
    assert restored_after_ack.stream_xpending("events", "workers")["count"] == 0


def test_redis_stream_commands_without_socket():
    store = ShardedKeyValueStore(shards=1, per_shard_max=10, wal_path="", tiering_dir="")
    server = RedisServer(store)

    response, role, namespace = server.handle_command([b"XADD", b"events", b"1-0", b"type", b"created"], None, None)
    assert response == b"$3\r\n1-0\r\n"
    assert role is None
    assert namespace is not None

    response, _, _ = server.handle_command([b"XRANGE", b"events", b"-", b"+"], role, namespace)
    assert response == encode_array([["1-0", ["type", "created"]]])

    response, _, _ = server.handle_command(
        [b"XGROUP", b"CREATE", b"events", b"workers", b"0-0"],
        role,
        namespace,
    )
    assert response == b"+OK\r\n"

    response, _, _ = server.handle_command(
        [b"XREADGROUP", b"GROUP", b"workers", b"alice", b"COUNT", b"1", b"STREAMS", b"events", b">"],
        role,
        namespace,
    )
    assert response == encode_array([["events", [["1-0", ["type", "created"]]]]])

    response, _, _ = server.handle_command([b"XPENDING", b"events", b"workers"], role, namespace)
    assert response == encode_array([1, "1-0", "1-0", [["alice", 1]]])

    response, _, _ = server.handle_command([b"XACK", b"events", b"workers", b"1-0"], role, namespace)
    assert response == b":1\r\n"
