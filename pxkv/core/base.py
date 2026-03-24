#!/usr/bin/env python
# -*- coding: utf-8 -*-

import bisect
from typing import Any, Iterator, List, Optional

class _SortedStringKeyIndex:
    """
    Maintain a sorted list of string keys for faster scan/pagination.
    """

    def __init__(self) -> None:
        self._keys: List[str] = []

    def clear(self) -> None:
        self._keys.clear()

    def add(self, key: Any) -> None:
        if not isinstance(key, str):
            return
        i = bisect.bisect_left(self._keys, key)
        if i < len(self._keys) and self._keys[i] == key:
            return
        self._keys.insert(i, key)

    def discard(self, key: Any) -> None:
        if not isinstance(key, str):
            return
        i = bisect.bisect_left(self._keys, key)
        if i < len(self._keys) and self._keys[i] == key:
            self._keys.pop(i)

    def iter_from(
        self, *, prefix: Optional[str] = None, start_after: Optional[str] = None
    ) -> Iterator[str]:
        if prefix is None and start_after is None:
            idx = 0
        else:
            if prefix is not None:
                idx = bisect.bisect_left(self._keys, prefix)
            else:
                idx = 0
            if start_after is not None:
                idx = max(idx, bisect.bisect_right(self._keys, start_after))

        while idx < len(self._keys):
            k = self._keys[idx]
            if prefix is not None and not k.startswith(prefix):
                break
            yield k
            idx += 1
