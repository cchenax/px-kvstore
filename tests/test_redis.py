import pytest
import time
import redis
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
