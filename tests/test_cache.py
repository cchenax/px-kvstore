import pytest
from pxkv.cache.ai import compute_ai_cache_key

class TestAICache:
    def test_compute_key_is_deterministic(self):
        params1 = {"model": "gpt-4", "temp": 0.7, "top_p": 1.0}
        params2 = {"top_p": 1.0, "model": "gpt-4", "temp": 0.7}
        
        k1, c1 = compute_ai_cache_key(prompt="hello world", model="openai", params=params1)
        k2, c2 = compute_ai_cache_key(prompt="hello world", model="openai", params=params2)
        
        assert k1 == k2
        assert c1 == c2
        assert isinstance(k1, str)
        assert len(k1) == 64
        
    def test_key_changes_on_input(self):
        k1, _ = compute_ai_cache_key(prompt="hi", model="m", params={})
        k2, _ = compute_ai_cache_key(prompt="hello", model="m", params={})
        assert k1 != k2
        
        k3, _ = compute_ai_cache_key(prompt="hi", model="m2", params={})
        assert k1 != k3
