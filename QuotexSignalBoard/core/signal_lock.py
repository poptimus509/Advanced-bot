import threading
from typing import Dict, Optional, Tuple

class SignalLockManager:
    def __init__(self, max_history: int = 1000):
        self._locks: Dict[Tuple[str, str, int], Dict] = {}
        self._mutex = threading.Lock()
        self._max_history = max_history

    def acquire_lock(self, symbol: str, timeframe: str, candle_epoch: int) -> bool:
        identity = (symbol, timeframe, candle_epoch)
        with self._mutex:
            if identity in self._locks:
                return False
            self._locks[identity] = {
                "status": "PROCESSING",
                "result": None,
                "created_at": candle_epoch
            }
            self._prune_if_needed()
            return True

    def commit_result(self, symbol: str, timeframe: str, candle_epoch: int, result: str, metadata: Optional[dict] = None):
        identity = (symbol, timeframe, candle_epoch)
        with self._mutex:
            if identity in self._locks:
                self._locks[identity]["status"] = "FINALIZED"
                self._locks[identity]["result"] = result
                self._locks[identity]["metadata"] = metadata or {}

    def is_locked(self, symbol: str, timeframe: str, candle_epoch: int) -> bool:
        identity = (symbol, timeframe, candle_epoch)
        with self._mutex:
            return identity in self._locks

    def get_result(self, symbol: str, timeframe: str, candle_epoch: int) -> Optional[str]:
        identity = (symbol, timeframe, candle_epoch)
        with self._mutex:
            record = self._locks.get(identity)
            return record["result"] if record else None

    def _prune_if_needed(self):
        if len(self._locks) > self._max_history:
            sorted_keys = sorted(self._locks.keys(), key=lambda k: k[2])
            for k in sorted_keys[: len(self._locks) - self._max_history]:
                del self._locks[k]