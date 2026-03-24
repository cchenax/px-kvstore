#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Backward-compatible entry point to run the PX KV HTTP server.

Prefer importing and using `pxkv.api.server.run` directly.
"""

from pxkv.api.server import run


if __name__ == "__main__":
    run()
