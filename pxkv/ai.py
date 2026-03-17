#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import json
from typing import Any, Dict, Tuple


def _canonical_json(obj: Any) -> str:
    """
    Canonical JSON for deterministic cache keys.

    - sort keys
    - stable separators
    - UTF-8 (ensure_ascii=False)
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_ai_cache_key(
    *,
    prompt: str,
    model: str = "",
    params: Dict[str, Any] | None = None,
) -> Tuple[str, str]:
    """
    Compute a deterministic content-addressed cache key for AI requests.

    Returns (key_hex, canonical_payload_json).
    """
    payload = {
        "prompt": prompt,
        "model": model or "",
        "params": params or {},
    }
    canon = _canonical_json(payload)
    key = hashlib.sha256(canon.encode("utf-8")).hexdigest()
    return key, canon

