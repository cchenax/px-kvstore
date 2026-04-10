import time
import pytest
import requests
import subprocess
import os

import socket

def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return str(s.getsockname()[1])

@pytest.fixture
def leader_follower_cluster():
    # Use random free ports
    leader_port = get_free_port()
    follower_port = get_free_port()
    
    # Start Leader
    leader_env = os.environ.copy()
    leader_env["PXKV_PORT"] = leader_port
    leader_env["PXKV_REPLICATION_ROLE"] = "leader"
    leader_env["PXKV_REPLICATION_FOLLOWERS"] = f"localhost:{follower_port}"
    leader_env["PXKV_WAL_FILE"] = "leader_wal.log"
    leader_env["PXKV_REDIS_ENABLED"] = "false"
    
    # Start Follower
    follower_env = os.environ.copy()
    follower_env["PXKV_PORT"] = follower_port
    follower_env["PXKV_REPLICATION_ROLE"] = "follower"
    follower_env["PXKV_REPLICATION_LEADER_ADDR"] = f"localhost:{leader_port}"
    follower_env["PXKV_WAL_FILE"] = "follower_wal.log"
    follower_env["PXKV_REDIS_ENABLED"] = "false"

    # ... existing cleanup ...
    # Clean up logs
    for f in ["leader_wal.log", "follower_wal.log"]:
        if os.path.exists(f): os.remove(f)

    # Start processes
    leader_proc = subprocess.Popen(["python3", "server.py"], env=leader_env)
    time.sleep(2.0) # Give leader time to start
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    
    time.sleep(5.0) # More time for everything to stabilize
    
    yield (leader_port, follower_port)
    
    leader_proc.terminate()
    follower_proc.terminate()
    leader_proc.wait()
    follower_proc.wait()

def test_replication_basic(leader_follower_cluster):
    leader_port, follower_port = leader_follower_cluster
    # 1. Write to Leader
    leader_url = f"http://localhost:{leader_port}/kv/repl_key"
    put_resp = requests.put(leader_url, data=b"repl_value")
    assert put_resp.status_code in [201, 204]
    
    time.sleep(3.0) # More time for async sync
    
    # 2. Read from Follower
    follower_url = f"http://localhost:{follower_port}/kv/repl_key"
    resp = requests.get(follower_url)
    assert resp.status_code == 200, f"Follower returned {resp.status_code}: {resp.text}"
    assert resp.json()["value"] == "repl_value"

def test_replication_incr(leader_follower_cluster):
    leader_port, follower_port = leader_follower_cluster
    # 1. Incr on Leader
    leader_url = f"http://localhost:{leader_port}/kv/incr/c1"
    requests.post(leader_url)
    requests.post(leader_url)
    
    time.sleep(2.0)
    
    # 2. Check Follower
    follower_url = f"http://localhost:{follower_port}/kv/c1"
    resp = requests.get(follower_url)
    assert resp.status_code == 200
    assert resp.json()["value"] == 2.0

def test_replication_full_sync():
    # Test that a new follower pulls existing data from leader
    leader_port = get_free_port()
    follower_port = get_free_port()
    
    leader_env = os.environ.copy()
    leader_env["PXKV_PORT"] = leader_port
    leader_env["PXKV_REPLICATION_ROLE"] = "leader"
    leader_env["PXKV_REDIS_ENABLED"] = "false"
    
    # 1. Start Leader and write data
    leader_proc = subprocess.Popen(["python3", "server.py"], env=leader_env)
    time.sleep(2.0)
    requests.put(f"http://localhost:{leader_port}/kv/pre_existing", data=b"pre_value")
    
    # 2. Start Follower
    follower_env = os.environ.copy()
    follower_env["PXKV_PORT"] = follower_port
    follower_env["PXKV_REPLICATION_ROLE"] = "follower"
    follower_env["PXKV_REPLICATION_LEADER_ADDR"] = f"localhost:{leader_port}"
    follower_env["PXKV_REDIS_ENABLED"] = "false"
    
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    time.sleep(3.0) # Wait for full sync
    
    # 3. Check Follower has the data
    resp = requests.get(f"http://localhost:{follower_port}/kv/pre_existing")
    
    # Cleanup
    leader_proc.terminate()
    follower_proc.terminate()
    leader_proc.wait()
    follower_proc.wait()
    
    assert resp.status_code == 200
    assert resp.json()["value"] == "pre_value"

def test_replication_catchup():
    # Test that a follower catches up from WAL after being offline
    leader_port = get_free_port()
    follower_port = get_free_port()
    
    leader_env = os.environ.copy()
    leader_env["PXKV_PORT"] = leader_port
    leader_env["PXKV_REPLICATION_ROLE"] = "leader"
    leader_env["PXKV_REPLICATION_FOLLOWERS"] = f"localhost:{follower_port}"
    leader_env["PXKV_WAL_FILE"] = "catchup_leader_wal.log"
    leader_env["PXKV_REDIS_ENABLED"] = "false"
    
    if os.path.exists("catchup_leader_wal.log"): os.remove("catchup_leader_wal.log")

    # 1. Start Leader
    leader_proc = subprocess.Popen(["python3", "server.py"], env=leader_env)
    time.sleep(2.0)
    
    # 2. Start Follower
    follower_env = os.environ.copy()
    follower_env["PXKV_PORT"] = follower_port
    follower_env["PXKV_REPLICATION_ROLE"] = "follower"
    follower_env["PXKV_REPLICATION_LEADER_ADDR"] = f"localhost:{leader_port}"
    follower_env["PXKV_REDIS_ENABLED"] = "false"
    follower_env["PXKV_REPLICATION_SYNC_INTERVAL"] = "1.0"
    
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    time.sleep(3.0)
    
    # 3. Write data, check sync
    requests.put(f"http://localhost:{leader_port}/kv/k1", data=b"v1")
    time.sleep(2.0)
    assert requests.get(f"http://localhost:{follower_port}/kv/k1").json()["value"] == "v1"
    
    # 4. Stop Follower
    follower_proc.terminate()
    follower_proc.wait()
    
    # 5. Write more data to Leader while Follower is offline
    requests.put(f"http://localhost:{leader_port}/kv/k2", data=b"v2")
    requests.put(f"http://localhost:{leader_port}/kv/k3", data=b"v3")
    
    # 6. Restart Follower
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    time.sleep(5.0) # Wait for catch-up from WAL
    
    # 7. Check if Follower has k2 and k3
    resp2 = requests.get(f"http://localhost:{follower_port}/kv/k2")
    resp3 = requests.get(f"http://localhost:{follower_port}/kv/k3")
    
    # Cleanup
    leader_proc.terminate()
    follower_proc.terminate()
    leader_proc.wait()
    follower_proc.wait()
    
    assert resp2.status_code == 200 and resp2.json()["value"] == "v2"
    assert resp3.status_code == 200 and resp3.json()["value"] == "v3"
