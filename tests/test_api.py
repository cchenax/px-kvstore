import os
import subprocess
import time
import json
import urllib.request
import urllib.error
import gzip
import threading
import ssl
import socket
import shutil

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
    stop_proc(proc)


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


def test_replication_snapshot_ndjson_gzip(http_server):
    url = f"{http_server}/replication/snapshot?format=ndjson&compress=gzip"
    req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"}, method="GET")
    with urllib.request.urlopen(req, timeout=3.0) as resp:
        assert resp.status == 200
        raw = resp.read()
        body = raw
        if (resp.headers.get("Content-Encoding", "") or "").lower() == "gzip":
            body = gzip.decompress(raw)
    lines = body.decode("utf-8", errors="replace").splitlines()
    assert len(lines) >= 1
    meta = json.loads(lines[0])
    assert "_lsn" in meta
    assert "shards" in meta


def test_sse_keyspace_notifications(http_server):
    url = f"{http_server}/events/keyspace"
    got = {"payload": None}

    def _reader():
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            deadline = time.time() + 3.0
            while time.time() < deadline:
                line = resp.readline()
                if not line:
                    break
                if line.startswith(b"data: "):
                    got["payload"] = line[len(b"data: ") :].decode("utf-8", errors="replace").strip()
                    break

    t = threading.Thread(target=_reader, daemon=True)
    t.start()

    put = urllib.request.Request(
        f"{http_server}/kv/sse_test_key",
        data=json.dumps({"value": "v"}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(put, timeout=2.0) as resp:
        assert resp.status in (200, 201)

    t.join(timeout=3.0)
    assert got["payload"] is not None
    ev = json.loads(got["payload"])
    assert ev["op"] == "set"
    assert ev["key"] == "sse_test_key"


def test_rate_limiting_per_route_admin_configurable():
    port = get_free_port()
    env = os.environ.copy()
    env["PXKV_PORT"] = str(port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_FAULT_LATENCY_MS"] = "0"
    env["PXKV_FAULT_LATENCY_JITTER_MS"] = "0"
    env["PXKV_RATE_LIMIT_ENABLED"] = "true"
    env["PXKV_RATE_LIMIT_DEFAULT_RPS"] = "0"
    env["PXKV_RATE_LIMIT_DEFAULT_BURST"] = "0"
    env["PXKV_RATE_LIMIT_ROUTES"] = json.dumps(
        {"GET /admin/health": {"rps": 0.1, "burst": 1, "per_ip": False}}
    )

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    base = f"http://localhost:{port}"
    try:
        deadline = time.time() + 8.0
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/admin/metrics", timeout=1.0) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                time.sleep(0.2)

        with urllib.request.urlopen(f"{base}/admin/health", timeout=2.0) as resp:
            assert resp.status == 200

        try:
            urllib.request.urlopen(f"{base}/admin/health", timeout=2.0)
            raise AssertionError("expected 429")
        except urllib.error.HTTPError as e:
            assert e.code == 429
            assert (e.headers.get("Retry-After", "") or "").strip() != ""

        with urllib.request.urlopen(f"{base}/admin/metrics", timeout=2.0) as resp:
            assert resp.status == 200
        with urllib.request.urlopen(f"{base}/admin/metrics", timeout=2.0) as resp:
            assert resp.status == 200

        data = json.dumps(
            {"RATE_LIMIT_ROUTES": {"GET /admin/health": {"rps": 100, "burst": 100, "per_ip": False}}}
        ).encode("utf-8")
        req = urllib.request.Request(
            f"{base}/admin/config",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            assert resp.status == 200
            updated = json.loads(resp.read().decode("utf-8"))
        assert updated["config"]["RATE_LIMIT_ROUTES"]["GET /admin/health"]["rps"] == 100.0

        with urllib.request.urlopen(f"{base}/admin/health", timeout=2.0) as resp:
            assert resp.status == 200
    finally:
        stop_proc(proc)


def test_tls_https_and_rediss(tmp_path):
    if shutil.which("openssl") is None:
        pytest.skip("openssl not available")

    cert_file = tmp_path / "cert.pem"
    key_file = tmp_path / "key.pem"
    try:
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(key_file),
                "-out",
                str(cert_file),
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pytest.skip("failed to generate self-signed cert with openssl")

    http_port = get_free_port()
    https_port = get_free_port()
    rediss_port = get_free_port()

    env = os.environ.copy()
    env["PXKV_PORT"] = str(http_port)
    env["PXKV_REDIS_ENABLED"] = "false"
    env["PXKV_HTTP_TLS_ENABLED"] = "true"
    env["PXKV_HTTPS_PORT"] = str(https_port)
    env["PXKV_TLS_CERT_FILE"] = str(cert_file)
    env["PXKV_TLS_KEY_FILE"] = str(key_file)
    env["PXKV_REDIS_TLS_ENABLED"] = "true"
    env["PXKV_REDIS_TLS_PORT"] = str(rediss_port)
    env["PXKV_REDIS_TLS_CERT_FILE"] = str(cert_file)
    env["PXKV_REDIS_TLS_KEY_FILE"] = str(key_file)

    proc = subprocess.Popen(["python3", "server.py"], env=env)
    try:
        https_base = f"https://localhost:{https_port}"
        deadline = time.time() + 8.0
        ctx = ssl._create_unverified_context()
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{https_base}/admin/health", timeout=1.0, context=ctx) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                time.sleep(0.2)

        with urllib.request.urlopen(f"{https_base}/admin/health", timeout=2.0, context=ctx) as resp:
            assert resp.status == 200

        raw = b"*1\r\n$4\r\nPING\r\n"
        sock = socket.create_connection(("127.0.0.1", rediss_port), timeout=2.0)
        try:
            with ctx.wrap_socket(sock, server_hostname="localhost") as ssock:
                ssock.sendall(raw)
                data = ssock.recv(1024)
        finally:
            try:
                sock.close()
            except Exception:
                pass
        assert b"+PONG" in data
    finally:
        stop_proc(proc)
