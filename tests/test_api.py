import os
import subprocess
import time
import json
import urllib.request

import pytest


def get_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture
def http_server():
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_FAULT_LATENCY_MS"] = "0"
    env["PXKV_FAULT_LATENCY_JITTER_MS"] = "0"

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
    yield base
    proc.terminate()
    proc.wait()


def test_admin_health(http_server):
    with urllib.request.urlopen(f"{http_server}/admin/health", timeout=2.0) as resp:
        assert resp.status == 200
        body = json.loads(resp.read().decode("utf-8"))
    assert body["status"] == "ok"
    assert "uptime_seconds" in body


def test_metrics_prometheus(http_server):
    with urllib.request.urlopen(f"{http_server}/admin/metrics?format=prometheus", timeout=2.0) as resp:
        assert resp.status == 200
        text = resp.read().decode("utf-8", errors="replace")
    assert "pxkv_requests_total" in text
    assert "pxkv_replication_leader_lsn" in text
