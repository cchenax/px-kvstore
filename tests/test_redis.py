import pytest
import time
import redis
import json
from pxkv.api.server import STORE, RedisServer
from pxkv.config.settings import settings

@pytest.fixture(scope="module")
def redis_server():
    test_port = 16379
    server = RedisServer(STORE, host="127.0.0.1", port=test_port)
    server.start()
    time.sleep(0.5)
    yield server
    server.stop()

@pytest.fixture
def r_client(redis_server):
    return redis.Redis(host="127.0.0.1", port=16379, db=0, decode_responses=True)

def test_redis_ping(r_client):
    assert r_client.ping() is True

def test_redis_set_get(r_client):
    r_client.set("foo", "bar")
    assert r_client.get("foo") == "bar"
    assert r_client.get("nonexistent") is None

def test_redis_del(r_client):
    r_client.set("k1", "v1")
    r_client.set("k2", "v2")
    assert r_client.delete("k1", "k2", "k3") == 2
    assert r_client.get("k1") is None
    assert r_client.get("k2") is None

def test_redis_incr(r_client):
    r_client.set("counter", 10)
    assert r_client.incr("counter") == 11
    assert r_client.incr("counter", 5) == 16

def test_redis_expire(r_client):
    r_client.set("exp", "val", ex=1)
    assert r_client.get("exp") == "val"
    time.sleep(1.2)
    assert r_client.get("exp") is None

def test_redis_dbsize(r_client):
    r_client.flushall()
    r_client.set("a", 1)
    r_client.set("b", 2)
    assert r_client.dbsize() == 2

def test_redis_flushall(r_client):
    r_client.set("a", 1)
    r_client.flushall()
    assert r_client.dbsize() == 0


def test_redis_scan_full_traversal(r_client):
    r_client.flushall()
    expected = {f"scan:{i:03d}" for i in range(75)}
    for k in expected:
        r_client.set(k, "1")

    seen: set[str] = set()
    cursor = 0
    steps = 0
    while True:
        cursor, batch = r_client.scan(cursor=cursor, count=20)
        seen.update(batch)
        steps += 1
        if cursor == 0:
            break
        assert steps < 1000

    assert expected.issubset(seen)


def test_redis_scan_match(r_client):
    r_client.flushall()
    for i in range(10):
        r_client.set(f"u:{i}", "1")
        r_client.set(f"p:{i}", "1")

    seen: set[str] = set()
    cursor = 0
    while True:
        cursor, batch = r_client.scan(cursor=cursor, match="u:*", count=5)
        seen.update(batch)
        if cursor == 0:
            break

    assert seen == {f"u:{i}" for i in range(10)}


def test_redis_scan_iter(r_client):
    r_client.flushall()
    expected = {f"iter:{i}" for i in range(50)}
    for k in expected:
        r_client.set(k, "x")
    got = set(r_client.scan_iter(match="iter:*", count=7))
    assert got == expected


def test_redis_scan_unknown_cursor_terminates(r_client):
    r_client.flushall()
    r_client.set("a", "1")
    cursor, batch = r_client.scan(cursor=999999999, count=10)
    assert cursor == 0
    assert batch == []


def test_redis_pubsub_keyspace_notifications(r_client):
    p = r_client.pubsub()
    p.subscribe("pxkv:keyspace", "pxkv:keyspace:expire")

    deadline = time.time() + 2.0
    while time.time() < deadline:
        _ = p.get_message(timeout=0.2)
        if _ and _["type"] == "subscribe":
            pass
        if _ and _["type"] == "subscribe" and _["channel"] == "pxkv:keyspace:expire":
            break

    r_client.set("ps_set", "1")
    got_set = None
    for _ in range(50):
        msg = p.get_message(timeout=0.2, ignore_subscribe_messages=True)
        if msg and msg["type"] == "message":
            payload = json.loads(msg["data"])
            if payload.get("op") == "set" and payload.get("key") == "ps_set":
                got_set = payload
                break
    assert got_set is not None

    r_client.set("ps_exp", "1", ex=1)
    time.sleep(1.2)
    STORE.purge_expired()
    got_exp = None
    for _ in range(80):
        msg = p.get_message(timeout=0.2, ignore_subscribe_messages=True)
        if msg and msg["type"] == "message" and msg.get("channel") == "pxkv:keyspace:expire":
            payload = json.loads(msg["data"])
            if payload.get("op") == "expire" and payload.get("key") == "ps_exp":
                got_exp = payload
                break
    assert got_exp is not None
    p.close()
