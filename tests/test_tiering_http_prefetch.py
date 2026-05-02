import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from pxkv.tiering.http import HttpTieringBackend
from pxkv.tiering.prefetch import AsyncPrefetchTieringBackend


class _ObjectStoreHandler(BaseHTTPRequestHandler):
    store = {}

    def log_message(self, format, *args):
        return

    def do_PUT(self):
        n = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(n)
        self.store[self.path] = body
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        body = self.store.get(self.path)
        if body is None:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_DELETE(self):
        self.store.pop(self.path, None)
        self.send_response(204)
        self.end_headers()


@pytest.fixture()
def object_store_base_url():
    server = HTTPServer(("127.0.0.1", 0), _ObjectStoreHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    host, port = server.server_address
    yield f"http://{host}:{port}/objects"
    server.shutdown()
    server.server_close()


def test_http_tiering_put_get_delete(object_store_base_url):
    backend = HttpTieringBackend(object_store_base_url, timeout=1.0)
    backend.put("k1", b"abc", ttl_remaining=1.0)
    hit = backend.get("k1")
    assert hit is not None
    assert hit.value == b"abc"
    backend.delete("k1")
    assert backend.get("k1") is None


def test_async_prefetch_wraps_http_backend(object_store_base_url):
    base = HttpTieringBackend(object_store_base_url, timeout=1.0)
    backend = AsyncPrefetchTieringBackend(base, workers=2, wait_ms=200, cache_max=32)

    backend.put("k2", {"x": 1}, ttl_remaining=1.0)
    backend.prefetch(["k2"])

    hit = None
    for _ in range(20):
        hit = backend.get("k2")
        if hit is not None:
            break
        time.sleep(0.01)
    assert hit is not None
    assert hit.value == {"x": 1}

    backend.delete("k2")
    assert backend.get("k2") is None
    backend.close()


def test_http_tiering_ttl_expiry(object_store_base_url):
    backend = HttpTieringBackend(object_store_base_url, timeout=1.0)
    payload = {"a": 1}
    backend.put("k3", payload, ttl_remaining=0.05)
    raw = backend.get("k3")
    assert raw is not None
    assert json.dumps(payload) is not None
    time.sleep(0.08)
    assert backend.get("k3") is None

