import time
import pytest
import requests
import subprocess
import os

@pytest.fixture
def leader_follower_cluster():
    # Use random high ports for tests to avoid conflict
    leader_port = "18000"
    follower_port = "18001"
    
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
    follower_proc = subprocess.Popen(["python3", "server.py"], env=follower_env)
    
    time.sleep(3.0) # More time for startup
    
    yield (leader_port, follower_port)
    
    leader_proc.terminate()
    follower_proc.terminate()
    leader_proc.wait()
    follower_proc.wait()

def test_replication_basic(leader_follower_cluster):
    leader_port, follower_port = leader_follower_cluster
    # 1. Write to Leader
    leader_url = f"http://localhost:{leader_port}/kv/repl_key"
    requests.put(leader_url, data=b"repl_value")
    
    time.sleep(2.0)
    
    # 2. Read from Follower
    follower_url = f"http://localhost:{follower_port}/kv/repl_key"
    resp = requests.get(follower_url)
    assert resp.status_code == 200
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
