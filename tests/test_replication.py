import os
import json
import subprocess
import time
import urllib.request
import urllib.error

import socket

import pytest


def get_free_port() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return str(s.getsockname()[1])


def http_put(url: str, data: bytes) -> int:
    req = urllib.request.Request(url, data=data, method="PUT")
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500))


def http_post(url: str, data: bytes = b"") -> int:
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500))


def http_get_json(url: str) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(url, timeout=3.0) as resp:
            raw = resp.read()
            body = json.loads(raw.decode("utf-8")) if raw else {}
            return resp.status, body
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500)), {}


def http_get_json_with_headers(url: str) -> tuple[int, dict, dict]:
    try:
        with urllib.request.urlopen(url, timeout=3.0) as resp:
            raw = resp.read()
            body = json.loads(raw.decode("utf-8")) if raw else {}
            return resp.status, body, dict(resp.headers.items())
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500)), {}, {}


@pytest.fixture
def leader_follower_cluster():
    leader_port = get_free_port()
    follower_port = get_free_port()
    
    leader_env = os.environ.copy()
    leader_env["PXKV_PORT"] = leader_port
    leader_env["PXKV_REPLICATION_ROLE"] = "leader"
    leader_env["PXKV_REPLICATION_FOLLOWERS"] = f"localhost:{follower_port}"
    leader_env["PXKV_WAL_FILE"] = "leader_wal.log"
    leader_env["PXKV_REDIS_ENABLED"] = "false"
    
    follower_env = os.environ.copy()
    follower_env["PXKV_PORT"] = follower_port
    follower_env["PXKV_REPLICATION_ROLE"] = "follower"
    follower_env["PXKV_REPLICATION_LEADER_ADDR"] = f"localhost:{leader_port}"
    follower_env["PXKV_WAL_FILE"] = "follower_wal.log"
    follower_env["PXKV_REDIS_ENABLED"] = "false"

    for f in ["leader_wal.log", "follower_wal.log"]:
        if os.path.exists(f): os.remove(f)

    leader_proc = subprocess.Popen(["python3", "server.py"], env=leader_env)
    time.sleep(2.0)
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    
    time.sleep(5.0)
    
    yield (leader_port, follower_port)
    
    leader_proc.terminate()
    follower_proc.terminate()
    leader_proc.wait()
    follower_proc.wait()

def test_replication_basic(leader_follower_cluster):
    leader_port, follower_port = leader_follower_cluster
    leader_url = f"http://localhost:{leader_port}/kv/repl_key"
    status = http_put(leader_url, b"repl_value")
    assert status in [201, 204]
    
    time.sleep(3.0)
    
    follower_url = f"http://localhost:{follower_port}/kv/repl_key"
    status, body, headers = http_get_json_with_headers(follower_url)
    assert status == 200
    assert body["value"] == "repl_value"
    assert headers.get("X-PXKV-Role") == "follower"
    assert int(headers.get("X-PXKV-Replication-Last-Applied-LSN", "0")) > 0

def test_replication_incr(leader_follower_cluster):
    leader_port, follower_port = leader_follower_cluster
    leader_url = f"http://localhost:{leader_port}/kv/incr/c1"
    assert http_post(leader_url) == 200
    assert http_post(leader_url) == 200
    
    time.sleep(2.0)
    
    follower_url = f"http://localhost:{follower_port}/kv/c1"
    status, body = http_get_json(follower_url)
    assert status == 200
    assert body["value"] == 2.0

def test_replication_full_sync():
    leader_port = get_free_port()
    follower_port = get_free_port()
    
    leader_env = os.environ.copy()
    leader_env["PXKV_PORT"] = leader_port
    leader_env["PXKV_REPLICATION_ROLE"] = "leader"
    leader_env["PXKV_REDIS_ENABLED"] = "false"
    
    leader_proc = subprocess.Popen(["python3", "server.py"], env=leader_env)
    time.sleep(2.0)
    assert http_put(f"http://localhost:{leader_port}/kv/pre_existing", b"pre_value") in [201, 204]
    
    follower_env = os.environ.copy()
    follower_env["PXKV_PORT"] = follower_port
    follower_env["PXKV_REPLICATION_ROLE"] = "follower"
    follower_env["PXKV_REPLICATION_LEADER_ADDR"] = f"localhost:{leader_port}"
    follower_env["PXKV_REDIS_ENABLED"] = "false"
    
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    time.sleep(3.0)
    
    status, body = http_get_json(f"http://localhost:{follower_port}/kv/pre_existing")
    
    leader_proc.terminate()
    follower_proc.terminate()
    leader_proc.wait()
    follower_proc.wait()
    
    assert status == 200
    assert body["value"] == "pre_value"

def test_replication_catchup():
    leader_port = get_free_port()
    follower_port = get_free_port()
    
    leader_env = os.environ.copy()
    leader_env["PXKV_PORT"] = leader_port
    leader_env["PXKV_REPLICATION_ROLE"] = "leader"
    leader_env["PXKV_REPLICATION_FOLLOWERS"] = f"localhost:{follower_port}"
    leader_env["PXKV_WAL_FILE"] = "catchup_leader_wal.log"
    leader_env["PXKV_REDIS_ENABLED"] = "false"
    
    if os.path.exists("catchup_leader_wal.log"): os.remove("catchup_leader_wal.log")

    leader_proc = subprocess.Popen(["python3", "server.py"], env=leader_env)
    time.sleep(2.0)
    
    follower_env = os.environ.copy()
    follower_env["PXKV_PORT"] = follower_port
    follower_env["PXKV_REPLICATION_ROLE"] = "follower"
    follower_env["PXKV_REPLICATION_LEADER_ADDR"] = f"localhost:{leader_port}"
    follower_env["PXKV_REDIS_ENABLED"] = "false"
    follower_env["PXKV_REPLICATION_SYNC_INTERVAL"] = "1.0"
    
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    time.sleep(3.0)
    
    assert http_put(f"http://localhost:{leader_port}/kv/k1", b"v1") in [201, 204]
    time.sleep(2.0)
    status, body = http_get_json(f"http://localhost:{follower_port}/kv/k1")
    assert status == 200
    assert body["value"] == "v1"
    
    follower_proc.terminate()
    follower_proc.wait()
    
    assert http_put(f"http://localhost:{leader_port}/kv/k2", b"v2") in [201, 204]
    assert http_put(f"http://localhost:{leader_port}/kv/k3", b"v3") in [201, 204]
    
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    time.sleep(5.0)
    
    status2, body2 = http_get_json(f"http://localhost:{follower_port}/kv/k2")
    status3, body3 = http_get_json(f"http://localhost:{follower_port}/kv/k3")
    
    leader_proc.terminate()
    follower_proc.terminate()
    leader_proc.wait()
    follower_proc.wait()
    
    assert status2 == 200 and body2["value"] == "v2"
    assert status3 == 200 and body3["value"] == "v3"


def test_replication_ack_and_lag_metrics(leader_follower_cluster):
    leader_port, follower_port = leader_follower_cluster
    follower_name = f"localhost:{follower_port}"
    assert http_put(f"http://localhost:{leader_port}/kv/ack_probe", b"v1") in [201, 204]

    deadline = time.time() + 8.0
    while time.time() < deadline:
        status, body = http_get_json(f"http://localhost:{leader_port}/admin/metrics")
        assert status == 200
        repl = body.get("replication", {})
        followers = repl.get("followers", {})
        if follower_name in followers:
            item = followers[follower_name]
            if int(item.get("ack_lsn", 0)) > 0:
                assert int(item.get("lag_lsn", 0)) >= 0
                return
        time.sleep(0.5)

    raise AssertionError("replication ack metrics not updated in time")


def test_follower_http_readonly_rejects_writes(leader_follower_cluster):
    leader_port, follower_port = leader_follower_cluster
    assert http_put(f"http://localhost:{leader_port}/kv/ro_seed", b"v1") in [201, 204]
    time.sleep(1.0)
    status = http_put(f"http://localhost:{follower_port}/kv/should_fail", b"nope")
    assert status == 403
