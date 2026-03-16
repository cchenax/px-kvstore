#!/usr/bin/env python
# -*- coding: utf-8 -*-

import http.server as BaseHTTPServer
import json
import logging
import os
import signal
import sys
import time
import urllib.parse as urlparse
from typing import Any, Dict, Tuple

from .store import ShardedKeyValueStore
from .persistence import SnapshotManager, load_snapshot


HOST = os.getenv("PXKV_HOST", "0.0.0.0")
PORT = int(os.getenv("PXKV_PORT", "8000"))
SHARDS = int(os.getenv("PXKV_SHARD_COUNT", os.getenv("SHARD_COUNT", "4")))
PER_SHARD_MAX = int(os.getenv("PXKV_PER_SHARD_MAX", "1000"))

STORE = ShardedKeyValueStore(shards=SHARDS, per_shard_max=PER_SHARD_MAX)

SNAPSHOT_FILE = os.getenv("PXKV_SNAPSHOT_FILE", "")
SNAPSHOT_INTERVAL = float(os.getenv("PXKV_SNAPSHOT_INTERVAL", "0"))

if SNAPSHOT_FILE:
    load_snapshot(STORE, SNAPSHOT_FILE)

_SNAPSHOT_MANAGER: SnapshotManager | None = None
if SNAPSHOT_FILE and SNAPSHOT_INTERVAL > 0:
    _SNAPSHOT_MANAGER = SnapshotManager(STORE, SNAPSHOT_FILE, SNAPSHOT_INTERVAL)
    _SNAPSHOT_MANAGER.start()

_METRICS: Dict[str, Any] = {
    "started_at": time.time(),
    "requests_total": 0,
    "requests_by_method": {},
    "errors_total": 0,
}


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class KVHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    server_version = "PX-KVStore/2.0"

    def _parse(self) -> Tuple[list[str], Dict[str, list[str]]]:
        parsed = urlparse.urlparse(self.path)
        parts = parsed.path.strip("/").split("/")
        if parts == [""]:
            parts = []
        query = urlparse.parse_qs(parsed.query)
        return parts, query

    def _body(self) -> bytes:
        size = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(size) if size else b""

    def _send(self, code: int, body: Any = b"", mime: str = "text/plain; charset=utf-8") -> None:
        if not isinstance(body, (bytes, bytearray)):
            body = str(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, obj: Any) -> None:
        def _default(v: Any):
            if isinstance(v, (bytes, bytearray)):
                return v.decode("utf-8", errors="replace")
            raise TypeError

        payload = json.dumps(obj, default=_default, ensure_ascii=False)
        self._send(code, payload.encode("utf-8"), "application/json")

    def _inc_metrics(self, method: str, error: bool = False) -> None:
        _METRICS["requests_total"] += 1
        by_method = _METRICS.setdefault("requests_by_method", {})
        by_method[method] = by_method.get(method, 0) + 1
        if error:
            _METRICS["errors_total"] += 1

    # Core KV APIs
    def do_GET(self) -> None:
        try:
            parts, query = self._parse()
            if not parts:
                self._json(200, {"status": "ok"})
                return

            if parts[0] == "admin":
                self._handle_admin_get(parts[1:], query)
                return

            if parts[0] != "kv":
                raise ValueError

            if len(parts) >= 2 and parts[1] == "batch":
                if "keys" not in query:
                    self._send(400, "keys query param required")
                    self._inc_metrics("GET", error=True)
                    return
                keys = query["keys"][0].split(",")
                self._json(200, STORE.mget(keys))
                self._inc_metrics("GET")
                return

            if len(parts) >= 2 and parts[1] == "scan":
                prefix = None
                start_after = None
                limit = 100
                if "prefix" in query:
                    prefix = query["prefix"][0]
                if "start_after" in query:
                    start_after = query["start_after"][0]
                if "limit" in query:
                    try:
                        limit = int(query["limit"][0])
                    except ValueError:
                        self._send(400, "limit must be int")
                        self._inc_metrics("GET", error=True)
                        return
                keys = STORE.scan(prefix=prefix, limit=limit, start_after=start_after)
                self._json(200, {"keys": keys})
                self._inc_metrics("GET")
                return

            if len(parts) < 2:
                raise ValueError

            key = parts[1]
            value = STORE.read(key)
            self._json(200, {"key": key, "value": value})
            self._inc_metrics("GET")
        except KeyError as e:
            self._send(404, str(e))
            self._inc_metrics("GET", error=True)
        except ValueError:
            self._send(404, "Not Found")
            self._inc_metrics("GET", error=True)

    def do_PUT(self) -> None:
        try:
            parts, query = self._parse()
            if len(parts) != 2 or parts[0] != "kv" or parts[1] == "":
                raise ValueError
            key = parts[1]
            ttl = float(query["ttl"][0]) if "ttl" in query else None

            raw = self._body()
            try:
                value = json.loads(raw or b"")
            except ValueError:
                value = raw

            if key in STORE.mget([key]):
                STORE.update(key, value, ttl)
                self._send(204)
            else:
                STORE.create(key, value, ttl)
                self._send(201)
            self._inc_metrics("PUT")
        except KeyError as e:
            self._send(409, str(e))
            self._inc_metrics("PUT", error=True)
        except ValueError:
            self._send(404, "Not Found")
            self._inc_metrics("PUT", error=True)

    def do_DELETE(self) -> None:
        try:
            parts, _ = self._parse()
            if len(parts) != 2 or parts[0] != "kv" or parts[1] == "":
                raise ValueError
            STORE.delete(parts[1])
            self._send(204)
            self._inc_metrics("DELETE")
        except KeyError as e:
            self._send(404, str(e))
            self._inc_metrics("DELETE", error=True)
        except ValueError:
            self._send(404, "Not Found")
            self._inc_metrics("DELETE", error=True)

    def do_POST(self) -> None:
        try:
            parts, _ = self._parse()
            if len(parts) >= 3 and parts[0] == "kv" and parts[1] == "incr":
                key = parts[2]
                delta = 1.0
                ttl = None
                # Parse query parameters for delta and ttl if provided
                _, query = self._parse()
                if "delta" in query:
                    try:
                        delta = float(query["delta"][0])
                    except ValueError:
                        self._send(400, "delta must be numeric")
                        self._inc_metrics("POST", error=True)
                        return
                if "ttl" in query:
                    try:
                        ttl = float(query["ttl"][0])
                    except ValueError:
                        self._send(400, "ttl must be numeric")
                        self._inc_metrics("POST", error=True)
                        return
                try:
                    new_val = STORE.incr(key, delta=delta, ttl=ttl)
                except TypeError as e:
                    self._send(400, str(e))
                    self._inc_metrics("POST", error=True)
                    return
                self._json(200, {"key": key, "value": new_val})
                self._inc_metrics("POST")
                return
            if parts == ["kv", "batch"]:
                payload = json.loads(self._body() or b"{}")
                items = payload.get("items", {})
                ttl = payload.get("ttl")
                if not isinstance(items, dict):
                    self._send(400, "items must be dict")
                    self._inc_metrics("POST", error=True)
                    return
                STORE.mset(items, ttl)
                self._send(201)
                self._inc_metrics("POST")
                return

            self._send(404, "Not Found")
            self._inc_metrics("POST", error=True)
        except ValueError:
            self._send(400, "Bad JSON")
            self._inc_metrics("POST", error=True)

    # Admin & metrics APIs
    def _handle_admin_get(self, parts: list[str], query: Dict[str, list[str]]) -> None:
        if not parts:
            self._json(200, {"status": "ok", "shards": SHARDS})
            return
        if parts[0] == "health":
            self._json(
                200,
                {
                    "status": "ok",
                    "uptime_seconds": time.time() - _METRICS["started_at"],
                    "shards": SHARDS,
                },
            )
            return
        if parts[0] == "metrics":
            self._json(200, _METRICS)
            return
        if parts[0] == "snapshot":
            if _SNAPSHOT_MANAGER is None or not SNAPSHOT_FILE:
                self._send(400, "snapshotting is disabled")
                return
            try:
                _SNAPSHOT_MANAGER.snapshot_once()
                self._json(200, {"status": "ok", "path": SNAPSHOT_FILE})
            except Exception as e:
                self._send(500, f"snapshot failed: {e}")
            return
        self._send(404, "Not Found")

    def log_message(self, fmt: str, *args: Any) -> None:
        logging.info("%s - %s", self.address_string(), fmt % args)


def run() -> None:
    httpd = BaseHTTPServer.HTTPServer((HOST, PORT), KVHandler)
    logging.info(
        "Serving on http://%s:%d  shards=%d per_shard_max=%d",
        HOST,
        PORT,
        SHARDS,
        PER_SHARD_MAX,
    )

    def stop(sig: int, _frame: Any) -> None:
        logging.info("Shutting down (%s)…", sig)
        httpd.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    httpd.serve_forever()


if __name__ == "__main__":
    run()

