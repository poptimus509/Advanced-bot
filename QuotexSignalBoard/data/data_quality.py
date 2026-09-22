from enum import Enum
import time

class DataQualityStatus(str, Enum):
    HEALTHY = "HEALTHY"
    STALE = "STALE"
    MISSING = "MISSING"
    DISCONNECTED = "DISCONNECTED"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    INVALID = "INVALID"

class SymbolHealthTracker:
    def __init__(self, stale_threshold_sec: float = 25.0, min_history_required: int = 60):
        self.stale_threshold_sec = stale_threshold_sec
        self.min_history_required = min_history_required
        self.last_tick_epoch: float = 0.0
        self.last_tick_local: float = 0.0
        self.current_state: DataQualityStatus = DataQualityStatus.DISCONNECTED
        self.consecutive_gaps: int = 0

    def record_tick(self, server_epoch: int):
        self.last_tick_epoch = float(server_epoch)
        self.last_tick_local = time.time()

    def evaluate(self, ws_connected: bool, history_len: int) -> DataQualityStatus:
        if not ws_connected:
            self.current_state = DataQualityStatus.DISCONNECTED
            return self.current_state

        if self.last_tick_local == 0:
            self.current_state = DataQualityStatus.MISSING
            return self.current_state

        elapsed_since_tick = time.time() - self.last_tick_local
        if elapsed_since_tick > self.stale_threshold_sec:
            self.current_state = DataQualityStatus.STALE
            return self.current_state

        if history_len < self.min_history_required:
            self.current_state = DataQualityStatus.INSUFFICIENT_HISTORY
            return self.current_state

        self.current_state = DataQualityStatus.HEALTHY
        return self.current_state
