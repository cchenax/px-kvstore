#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Compatibility layer.

This repo originally exposed the store classes from `kvstore.py`.
The maintained implementation now lives in `pxkv.store`.
"""

from pxkv.store import LFUKeyValueStore, LRUKeyValueStore, ShardedKeyValueStore

__all__ = ["LRUKeyValueStore", "LFUKeyValueStore", "ShardedKeyValueStore"]