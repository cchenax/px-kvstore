#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import json
import zlib
import base64
from typing import Any, Dict, Tuple, Optional, Callable

def default_normalize(prompt: str) -> str:
    return " ".join(prompt.lower().split())

class AICacheManager:
    def __init__(self, normalize_fn: Optional[Callable[[str], str]] = None):
        self.normalize_fn = normalize_fn or default_normalize

    def compute_key(self, prompt: str, model: str, params: Dict[str, Any], model_version: Optional[str] = None) -> Tuple[str, str]:
        """
        Deterministic cache key for LLM requests.
        Includes model version and normalized prompt.
        """
        normalized_prompt = self.normalize_fn(prompt)
        obj = {
            "prompt": normalized_prompt,
            "model": model,
            "model_version": model_version,
            "params": params,
        }
        canon = json.dumps(obj, sort_keys=True, ensure_ascii=False)
        h = hashlib.sha256(canon.encode("utf-8")).hexdigest()
        return h, canon

    def compress_value(self, value: Any) -> str:
        """Compress response value if it's large string."""
        if not isinstance(value, str):
            return value
        if len(value) < 1024:
            return value
        compressed = zlib.compress(value.encode("utf-8"))
        return "gz:" + base64.b64encode(compressed).decode("utf-8")

    def decompress_value(self, value: Any) -> Any:
        if isinstance(value, str) and value.startswith("gz:"):
            compressed = base64.b64decode(value[3:])
            return zlib.decompress(compressed).decode("utf-8")
        return value

ai_cache_manager = AICacheManager()

def compute_ai_cache_key(prompt: str, model: str, params: Dict[str, Any]) -> Tuple[str, str]:
    return ai_cache_manager.compute_key(prompt, model, params)
