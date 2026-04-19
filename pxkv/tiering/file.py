from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from threading import Lock
from typing import Any, Optional

from .base import TieringBackend, TieringResult


class FileTieringBackend(TieringBackend):
    def __init__(self, directory: str):
        self._dir = directory
        self._lock = Lock()
        os.makedirs(self._dir, exist_ok=True)

    def _key_str(self, key: Any) -> str:
        if isinstance(key, bytes):
            return key.decode("utf-8", errors="replace")
        return str(key)

    def _path_for(self, key: Any) -> str:
        s = self._key_str(key).encode("utf-8", errors="replace")
        h = hashlib.sha256(s).hexdigest()
        return os.path.join(self._dir, f"{h}.json")

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
        path = self._path_for(key)
        payload = {
            "key": self._key_str(key),
            "stored_at": time.time(),
            "ttl_remaining": ttl_remaining,
            "value": value,
        }
        tmp = f"{path}.tmp"
        data = json.dumps(payload, default=self._json_default, ensure_ascii=False).encode("utf-8")
        with self._lock:
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, path)

    def get(self, key: Any) -> Optional[TieringResult]:
        path = self._path_for(key)
        with self._lock:
            if not os.path.exists(path):
                return None
            try:
                raw = open(path, "rb").read()
                payload = json.loads(raw.decode("utf-8", errors="replace"))
            except Exception:
                try:
                    os.remove(path)
                except Exception:
                    pass
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
        path = self._path_for(key)
        with self._lock:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

