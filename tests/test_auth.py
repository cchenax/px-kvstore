import base64
import os
import subprocess
import time
import urllib.request
import urllib.error

import pytest


def get_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def http_get(url: str, headers: dict[str, str] | None = None) -> int:
    req = urllib.request.Request(url, headers=headers or {}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500))


def http_put(url: str, data: bytes, headers: dict[str, str] | None = None) -> int:
    req = urllib.request.Request(url, data=data, headers=headers or {}, method="PUT")
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return int(getattr(e, "code", 500))

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


@pytest.fixture
def authed_http_server():
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_AUTH_ADMIN_TOKEN"] = "adm"
    env["PXKV_AUTH_WRITER_TOKEN"] = "w"
    env["PXKV_AUTH_READER_TOKEN"] = "r"
    proc = subprocess.Popen(["python3", "server.py"], env=env)
    base = f"http://localhost:{port}"
    deadline = time.time() + 8.0
    while time.time() < deadline:
        try:
            if http_get(f"{base}/admin/health", headers={"Authorization": "Bearer adm"}) == 200:
                break
        except Exception:
            pass
        time.sleep(0.2)
    yield base
    stop_proc(proc)


def test_http_auth_and_acl(authed_http_server):
    base = authed_http_server
    assert http_get(f"{base}/admin/health") == 401
    assert http_get(f"{base}/admin/health", headers={"Authorization": "Bearer r"}) == 403
    assert http_get(f"{base}/admin/health", headers={"Authorization": "Bearer adm"}) == 200

    assert http_put(f"{base}/kv/x", b"1", headers={"Authorization": "Bearer r"}) == 403
    assert http_put(f"{base}/kv/x", b"1", headers={"Authorization": "Bearer w"}) in (201, 204)


def test_http_basic_password_auth():
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_AUTH_ADMIN_PASSWORD"] = "p"
    proc = subprocess.Popen(["python3", "server.py"], env=env)
    base = f"http://localhost:{port}"
    try:
        deadline = time.time() + 8.0
        while time.time() < deadline:
            try:
                if http_get(f"{base}/admin/health", headers={"Authorization": "Bearer nope"}) in (401, 403):
                    break
            except Exception:
                pass
            time.sleep(0.2)

        token = base64.b64encode(b"u:p").decode("ascii")
        assert http_get(f"{base}/admin/health", headers={"Authorization": f"Basic {token}"}) == 200
    finally:
        stop_proc(proc)
