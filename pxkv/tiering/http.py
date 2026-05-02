from __future__ import annotations

import base64
import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from threading import Lock
from typing import Any, Optional

from .base import TieringBackend, TieringResult


class HttpTieringBackend(TieringBackend):
    def __init__(self, base_url: str, timeout: float = 2.0):
        self._base = (base_url or "").rstrip("/")
        self._timeout = float(timeout)
        self._lock = Lock()
        if not self._base:
            raise ValueError("base_url must not be empty")

    def _key_str(self, key: Any) -> str:
        if isinstance(key, bytes):
            return key.decode("utf-8", errors="replace")
        return str(key)

    def _object_key(self, key: Any) -> str:
        s = self._key_str(key).encode("utf-8", errors="replace")
        return hashlib.sha256(s).hexdigest()

    def _url_for(self, key: Any) -> str:
        obj = self._object_key(key)
        return f"{self._base}/{urllib.parse.quote(obj)}"

    def _json_default(self, v: Any):
        if isinstance(v, (bytes, bytearray)):
            return {"__type__": "bytes", "b64": base64.b64encode(bytes(v)).decode("ascii")}
        return str(v)

    def _decode_value(self, obj: Any) -> Any:
        if isinstance(obj, dict) and obj.get("__type__") == "bytes" and "b64" in obj:
            try:
                return base64.b64decode(obj["b64"].encode("ascii"))
            except Exception:
                return b""
        return obj

    def put(self, key: Any, value: Any, ttl_remaining: Optional[float]) -> None:
        if ttl_remaining is not None and float(ttl_remaining) <= 0:
            return
        payload = {
            "key": self._key_str(key),
            "stored_at": time.time(),
            "ttl_remaining": ttl_remaining,
            "value": value,
        }
        data = json.dumps(payload, default=self._json_default, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            self._url_for(key),
            data=data,
            headers={"Content-Type": "application/json"},
            method="PUT",
        )
        with self._lock:
            with urllib.request.urlopen(req, timeout=self._timeout) as _:
                return

    def get(self, key: Any) -> Optional[TieringResult]:
        req = urllib.request.Request(self._url_for(key), method="GET")
        with self._lock:
            try:
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    raw = resp.read()
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return None
                return None
            except Exception:
                return None
        try:
            payload = json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            return None

        stored_at = float(payload.get("stored_at", 0.0) or 0.0)
        ttl_remaining = payload.get("ttl_remaining")
        if ttl_remaining is None:
            remaining = None
        else:
            remaining = float(ttl_remaining) - max(0.0, time.time() - stored_at)
            if remaining <= 0:
                self.delete(key)
                return None

        value = self._decode_value(payload.get("value"))
        return TieringResult(value=value, ttl_remaining=remaining)

    def delete(self, key: Any) -> None:
        req = urllib.request.Request(self._url_for(key), method="DELETE")
        with self._lock:
            try:
                with urllib.request.urlopen(req, timeout=self._timeout) as _:
                    return
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return
                return
            except Exception:
                return

