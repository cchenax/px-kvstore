import json
import os
import subprocess
import time
import urllib.request

import pytest


def get_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]

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
def server(tmp_path):
    port = get_free_port()
    config_file = tmp_path / "pxkv_config.json"
    config_file.write_text(json.dumps({"FAULT_LATENCY_MS": 0.0}), encoding="utf-8")

    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_CONFIG_FILE"] = str(config_file)
    env["PXKV_WAL_FILE"] = ""
    env["PXKV_SNAPSHOT_FILE"] = ""
    env["PXKV_SNAPSHOT_INTERVAL"] = "0"
    env["PXKV_REPLICATION_ROLE"] = "leader"
    env["PXKV_REPLICATION_FOLLOWERS"] = ""
    env["PXKV_AUTH_ADMIN_TOKEN"] = ""
    env["PXKV_AUTH_WRITER_TOKEN"] = ""
    env["PXKV_AUTH_READER_TOKEN"] = ""
    env["PXKV_AUTH_ADMIN_PASSWORD"] = ""
    env["PXKV_AUTH_WRITER_PASSWORD"] = ""
    env["PXKV_AUTH_READER_PASSWORD"] = ""

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    base = f"http://localhost:{port}"
    deadline = time.time() + 8.0
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base}/admin/health", timeout=1.0) as resp:
                if resp.status == 200:
                    break
        except Exception:
            time.sleep(0.2)
    yield base, proc, config_file
    stop_proc(proc)


def test_config_hot_reload_api(server):
    base_url, _proc, _config_file = server

    with urllib.request.urlopen(f"{base_url}/admin/config", timeout=2.0) as resp:
        assert resp.status == 200
        body = json.loads(resp.read().decode("utf-8"))
    assert body["FAULT_LATENCY_MS"] == 0.0

    data = json.dumps({"FAULT_LATENCY_MS": 100.0}).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/admin/config",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=2.0) as resp:
        assert resp.status == 200
        updated = json.loads(resp.read().decode("utf-8"))
    assert updated["config"]["FAULT_LATENCY_MS"] == 100.0

    start = time.perf_counter()
    with urllib.request.urlopen(f"{base_url}/", timeout=2.0) as _resp:
        pass
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    assert elapsed_ms >= 70.0


def test_config_reload_from_file(server):
    base_url, _proc, config_file = server

    config_file.write_text(json.dumps({"FAULT_LATENCY_MS": 50.0}), encoding="utf-8")
    req = urllib.request.Request(
        f"{base_url}/admin/config/reload",
        data=b"{}",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=2.0) as resp:
        assert resp.status == 200

    with urllib.request.urlopen(f"{base_url}/admin/config", timeout=2.0) as resp:
        assert resp.status == 200
        body = json.loads(resp.read().decode("utf-8"))
    assert body["FAULT_LATENCY_MS"] == 50.0
