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


def stop_proc(proc: subprocess.Popen) -> None:
    try:
        proc.terminate()
    except Exception:
        return
    try:
        proc.wait(timeout=3.0)
        return
    except Exception:
        pass
    try:
        proc.kill()
    except Exception:
        return
    try:
        proc.wait(timeout=3.0)
    except Exception:
        pass


def wait_for_http_ready(base: str, timeout_s: float = 8.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base}/admin/health", timeout=1.0) as resp:
                if resp.status == 200:
                    return
        except Exception:
            time.sleep(0.2)
    raise AssertionError(f"server not ready: {base}")


def http_put(url: str, data: bytes) -> int:
    req = urllib.request.Request(url, data=data, method="PUT")
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500))


def http_get_json_with_headers(url: str) -> tuple[int, dict, dict]:
    try:
        with urllib.request.urlopen(url, timeout=3.0) as resp:
            raw = resp.read()
            body = json.loads(raw.decode("utf-8")) if raw else {}
            return resp.status, body, dict(resp.headers.items())
    except urllib.error.HTTPError as e:
        try:
            raw = e.read()
        except Exception:
            raw = b""
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            body = {}
        return int(getattr(e, "code", 500)), body, dict(getattr(e, "headers", {}) or {})


def wait_for_kv(base: str, key: str, expected: object, timeout_s: float = 8.0) -> None:
    url = f"{base}/kv/{key}"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        status, body, _hdrs = http_get_json_with_headers(url)
        if status == 200 and body.get("value") == expected:
            return
        time.sleep(0.2)
    raise AssertionError(f"kv not ready: {url}")


@pytest.fixture
def cluster():
    leader_port = get_free_port()
    follower_port = get_free_port()

    leader_env = os.environ.copy()
    leader_env["PXKV_PORT"] = leader_port
    leader_env["PXKV_REPLICATION_ROLE"] = "leader"
    leader_env["PXKV_REPLICATION_FOLLOWERS"] = f"localhost:{follower_port}"
    leader_env["PXKV_WAL_FILE"] = "leader_wal.log"
    leader_env["PXKV_REDIS_ENABLED"] = "false"
    leader_env["PXKV_FOLLOWER_READ_ENABLED"] = "true"
    leader_env["PXKV_FOLLOWER_READ_MAX_LAG_LSN"] = "0"
    leader_env["PXKV_FOLLOWER_READ_MAX_AGE_MS"] = "60000"

    follower_env = os.environ.copy()
    follower_env["PXKV_PORT"] = follower_port
    follower_env["PXKV_REPLICATION_ROLE"] = "follower"
    follower_env["PXKV_REPLICATION_LEADER_ADDR"] = f"localhost:{leader_port}"
    follower_env["PXKV_WAL_FILE"] = "follower_wal.log"
    follower_env["PXKV_REDIS_ENABLED"] = "false"

    for f in ["leader_wal.log", "follower_wal.log"]:
        if os.path.exists(f):
            os.remove(f)

    leader_proc = subprocess.Popen(["python3", "server.py"], env=leader_env)
    leader_base = f"http://localhost:{leader_port}"
    wait_for_http_ready(leader_base, timeout_s=8.0)
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    follower_base = f"http://localhost:{follower_port}"
    wait_for_http_ready(follower_base, timeout_s=8.0)

    yield leader_base, follower_base, f"localhost:{follower_port}"

    stop_proc(leader_proc)
    stop_proc(follower_proc)


def test_read_routed_to_follower_when_safe(cluster):
    leader_base, follower_base, follower_name = cluster
    assert http_put(f"{leader_base}/kv/r1", b"v1") in (201, 204)
    wait_for_kv(follower_base, "r1", "v1", timeout_s=8.0)

    status, body, headers = http_get_json_with_headers(f"{leader_base}/kv/r1?read_from=follower")
    assert status == 200
    assert body["value"] == "v1"
    assert headers.get("X-PXKV-Read-Source") == "follower"
    assert headers.get("X-PXKV-Read-Follower") == follower_name
    assert headers.get("X-PXKV-Role") == "follower"


def test_read_falls_back_to_leader_when_follower_stale(cluster):
    leader_base, follower_base, follower_name = cluster
    assert http_put(f"{leader_base}/kv/r2", b"v2") in (201, 204)
    wait_for_kv(follower_base, "r2", "v2", timeout_s=8.0)

    status, body, headers = http_get_json_with_headers(f"{leader_base}/kv/r2?read_from=follower&max_age_ms=0.0001")
    assert status == 200
    assert body["value"] == "v2"
    assert headers.get("X-PXKV-Read-Source") == "leader"
    assert headers.get("X-PXKV-Read-Follower") == follower_name
