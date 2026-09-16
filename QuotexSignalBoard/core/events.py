from dataclasses import dataclass
from typing import Callable, List, Optional
import pandas as pd

@dataclass(frozen=True)
class Candle:
    symbol: str
    timeframe: str
    epoch: int
    open: float
    high: float
    low: float
    close: float
    is_closed: bool
    ticks_count: int
    close_epoch: int

@dataclass
class CandleClosedEvent:
    symbol: str
    timeframe: str
    candle_epoch: int
    candle: Candle
    server_time_at_close: int
    closed_history: pd.DataFrame

@dataclass
class DataQualityEvent:
    symbol: str
    previous_state: str
    new_state: str
    reason: str
    timestamp: float

class EventDispatcher:
    def __init__(self):
        self._listeners: List[Callable[[CandleClosedEvent], None]] = []

    def subscribe(self, listener: Callable[[CandleClosedEvent], None]):
        self._listeners.append(listener)

    def dispatch_candle_closed(self, event: CandleClosedEvent):
        for listener in self._listeners:
            try:
                listener(event)
            except Exception as e:
                print(f"[EventDispatcher] Error in listener {listener}: {e}", flush=True)