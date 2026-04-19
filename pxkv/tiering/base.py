from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Protocol


@dataclass(frozen=True)
class TieringResult:
    value: Any
    ttl_remaining: Optional[float]


class TieringBackend(Protocol):
    def put(self, key: Any, value: Any, ttl_remaining: Optional[float]) -> None: ...
    def get(self, key: Any) -> Optional[TieringResult]: ...
    def delete(self, key: Any) -> None: ...

