from __future__ import annotations

import json
import time
from dataclasses import dataclass
from queue import Queue
from threading import Lock
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class KeyspaceEvent:
    op: str
    key: Any
    ts: float
    lsn: int = 0
    shard: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "op": self.op,
            "key": self.key,
            "ts": self.ts,
            "lsn": self.lsn,
            "shard": self.shard,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, default=str)


class KeyspaceNotifier:
    def __init__(self, per_subscriber_max: int = 1024):
        self._lock = Lock()
        self._subs: Dict[int, Queue[KeyspaceEvent]] = {}
        self._next_id = 1
        self._max = max(1, int(per_subscriber_max))

    def subscribe(self) -> tuple[int, Queue[KeyspaceEvent]]:
        q: Queue[KeyspaceEvent] = Queue(maxsize=self._max)
        with self._lock:
            sid = self._next_id
            self._next_id += 1
            self._subs[sid] = q
        return sid, q

    def unsubscribe(self, sid: int) -> None:
        with self._lock:
            self._subs.pop(int(sid), None)

    def publish(self, op: str, key: Any, *, lsn: int = 0, shard: Optional[int] = None) -> None:
        ev = KeyspaceEvent(op=str(op), key=key, ts=time.time(), lsn=int(lsn or 0), shard=shard)
        with self._lock:
            subs = list(self._subs.values())
        for q in subs:
            try:
                q.put_nowait(ev)
            except Exception:
                pass


notifier = KeyspaceNotifier()

