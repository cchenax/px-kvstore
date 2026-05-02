from __future__ import annotations

import base64
import hashlib
import json
import time
from typing import Any, Optional

from .base import TieringBackend, TieringResult


class S3TieringBackend(TieringBackend):
    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region: str = "",
        endpoint_url: str = "",
    ):
        try:
            import boto3  # type: ignore
        except Exception as e:
            raise RuntimeError("boto3 is required for S3TieringBackend") from e

        self._bucket = bucket
        self._prefix = (prefix or "").lstrip("/")
        self._client = boto3.client(
            "s3",
            region_name=region or None,
            endpoint_url=endpoint_url or None,
        )
        if not self._bucket:
            raise ValueError("bucket must not be empty")

    def _key_str(self, key: Any) -> str:
        if isinstance(key, bytes):
            return key.decode("utf-8", errors="replace")
        return str(key)

    def _object_key(self, key: Any) -> str:
        s = self._key_str(key).encode("utf-8", errors="replace")
        h = hashlib.sha256(s).hexdigest()
        if self._prefix:
            return f"{self._prefix}/{h}.json"
        return f"{h}.json"

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
        self._client.put_object(
            Bucket=self._bucket,
            Key=self._object_key(key),
            Body=data,
            ContentType="application/json",
        )

    def get(self, key: Any) -> Optional[TieringResult]:
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=self._object_key(key))
        except Exception:
            return None
        try:
            raw = resp["Body"].read()
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
        try:
            self._client.delete_object(Bucket=self._bucket, Key=self._object_key(key))
        except Exception:
            return

