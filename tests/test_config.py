import json
import os
import subprocess
import time

import pytest
import requests


def get_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture
def server(tmp_path):
    port = get_free_port()
    config_file = tmp_path / "pxkv_config.json"
    config_file.write_text(json.dumps({"FAULT_LATENCY_MS": 0.0}), encoding="utf-8")

    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_CONFIG_FILE"] = str(config_file)

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    time.sleep(2.0)
    yield f"http://localhost:{port}", proc, config_file
    proc.terminate()
    proc.wait()


def test_config_hot_reload_api(server):
    base_url, _proc, _config_file = server

    resp = requests.get(f"{base_url}/admin/config")
    assert resp.status_code == 200
    assert resp.json()["FAULT_LATENCY_MS"] == 0.0

    update_resp = requests.post(f"{base_url}/admin/config", json={"FAULT_LATENCY_MS": 100.0})
    assert update_resp.status_code == 200
    assert update_resp.json()["config"]["FAULT_LATENCY_MS"] == 100.0

    start = time.perf_counter()
    requests.get(f"{base_url}/")
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    assert elapsed_ms >= 70.0


def test_config_reload_from_file(server):
    base_url, _proc, config_file = server

    config_file.write_text(json.dumps({"FAULT_LATENCY_MS": 50.0}), encoding="utf-8")
    reload_resp = requests.post(f"{base_url}/admin/config/reload", json={})
    assert reload_resp.status_code == 200

    resp = requests.get(f"{base_url}/admin/config")
    assert resp.status_code == 200
    assert resp.json()["FAULT_LATENCY_MS"] == 50.0
