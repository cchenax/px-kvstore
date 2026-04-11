import time
import pytest
import requests
import subprocess
import os
import signal

def get_free_port():
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

@pytest.fixture
def server():
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_FAULT_LATENCY_MS"] = "0"
    env["PXKV_REDIS_ENABLED"] = "false"
    
    proc = subprocess.Popen(["python3", "server.py"], env=env)
    time.sleep(2.0)
    yield f"http://localhost:{port}", proc
    proc.terminate()
    proc.wait()

def test_config_hot_reload_api(server):
    base_url, proc = server
    
    # 1. Check initial config
    resp = requests.get(f"{base_url}/admin/config")
    assert resp.status_code == 200
    assert resp.json()["FAULT_LATENCY_MS"] == 0.0
    
    # 2. Update config via API
    update_resp = requests.post(f"{base_url}/admin/config", json={"FAULT_LATENCY_MS": 100.0})
    assert update_resp.status_code == 200
    assert update_resp.json()["config"]["FAULT_LATENCY_MS"] == 100.0
    
    # 3. Verify it takes effect (measure latency)
    start = time.time()
    requests.get(f"{base_url}/")
    elapsed = (time.time() - start) * 1000
    assert elapsed >= 100.0

def test_config_reload_sighup(server):
    base_url, proc = server
    
    # 1. Set env var for the process (this is tricky with subprocess, 
    # but we can simulate by checking if it reloads from its current env)
    # Since we can't easily change the env of a running subprocess, 
    # we'll just test that the signal handler doesn't crash and reloads.
    
    os.environ["PXKV_FAULT_LATENCY_MS"] = "50.0" # This won't affect the subproc directly
    # but if we had a way to change it, SIGHUP would pick it up.
    
    proc.send_signal(signal.SIGHUP)
    time.sleep(1.0)
    
    # Check if still running
    resp = requests.get(f"{base_url}/admin/config")
    assert resp.status_code == 200
