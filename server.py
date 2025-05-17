#!/usr/bin/env
# -*- coding: utf-8 -*-

import http.server as BaseHTTPServer
import urllib.parse as urlparse
import json
import logging
import os
import signal
import sys

from kvstore import ShardedKeyValueStore

HOST, PORT = "0.0.0.0", 8000
SHARDS = int(os.getenv("SHARD_COUNT", "4"))
STORE = ShardedKeyValueStore(shards=SHARDS, per_shard_max=1000)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class KVHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    server_version = "PX-KVStore/1.0"

    def _parse(self):
        parsed = urlparse.urlparse(self.path)
        parts = parsed.path.strip("/").split("/")
        query = urlparse.parse_qs(parsed.query)
        return parts, query

    def _body(self):
        size = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(size) if size else ""

    def _send(self, code, body="", mime="text/plain; charset=utf-8"):
        if not isinstance(body, bytes):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code, obj):
        def _default(v):
            if isinstance(v, (bytes, bytearray)):
                return v.decode('utf-8', errors='replace')
            raise TypeError

        self._send(code,
                    json.dumps(obj, default=_default, ensure_ascii=False),
                   "application/json")

    def do_GET(self):
        try:
            parts, query = self._parse()
            if not parts or parts[0] != "kv":
                raise ValueError

            if len(parts) >= 2 and parts[1] == "batch":
                if "keys" not in query:
                    self._send(400, "keys query param required")
                    return
                keys = query["keys"][0].split(",")
                self._json(200, STORE.mget(keys))
                return

            key = parts[1]
            value = STORE.read(key)
            self._json(200, {"key": key, "value": value})
        except KeyError as e:
            self._send(404, str(e))
        except ValueError:
            self._send(404, "Not Found")

    def do_PUT(self):
        try:
            parts, query = self._parse()
            if len(parts) != 2 or parts[0] != "kv" or parts[1] == "":
                raise ValueError
            key = parts[1]
            ttl = float(query["ttl"][0]) if "ttl" in query else None

            raw = self._body()
            try:
                value = json.loads(raw)
            except ValueError:
                value = raw

            if key in STORE.mget([key]):
                STORE.update(key, value, ttl)
                self._send(204)
            else:
                STORE.create(key, value, ttl)
                self._send(201)
        except KeyError as e:
            self._send(409, str(e))
        except ValueError:
            self._send(404, "Not Found")

    def do_DELETE(self):
        try:
            parts, _ = self._parse()
            if len(parts) != 2 or parts[0] != "kv" or parts[1] == "":
                raise ValueError
            STORE.delete(parts[1])
            self._send(204)
        except KeyError as e:
            self._send(404, str(e))
        except ValueError:
            self._send(404, "Not Found")

    def do_POST(self):
        try:
            parts, _ = self._parse()
            if parts != ["kv", "batch"]:
                self._send(404, "Not Found")
                return
            payload = json.loads(self._body() or "{}")
            items = payload.get("items", {})
            ttl = payload.get("ttl")
            if not isinstance(items, dict):
                self._send(400, "items must be dict")
                return
            STORE.mset(items, ttl)
            self._send(201)
        except ValueError:
            self._send(400, "Bad JSON")

    def log_message(self, fmt, *args):
        logging.info("%s - %s", self.address_string(), fmt % args)


def run():
    httpd = BaseHTTPServer.HTTPServer((HOST, PORT), KVHandler)
    logging.info("Serving on http://%s:%d  shards=%d", HOST, PORT, SHARDS)

    def stop(sig, _):
        logging.info("Shutting down (%s)…", sig)
        httpd.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    httpd.serve_forever()


if __name__ == "__main__":
    run()