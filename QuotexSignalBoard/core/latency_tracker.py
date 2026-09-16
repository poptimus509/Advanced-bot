import time
from dataclasses import asdict, dataclass
from typing import Optional

@dataclass
class PipelineLatencyMetric:
    symbol: str
    timeframe: str
    candle_epoch: int
    tick_server_epoch: int
    local_tick_receipt_time: float
    candle_boundary_time: int
    candle_close_detection_time: float
    candle_finalization_time: float
    strategy_eval_start: Optional[float] = None
    strategy_eval_end: Optional[float] = None
    signal_creation_time: Optional[float] = None
    telegram_send_start: Optional[float] = None
    telegram_response_time: Optional[float] = None
    broker_execution_latency: str = "UNKNOWN"

    def calculate_engine_latency_ms(self) -> float:
        return (self.candle_finalization_time - self.candle_close_detection_time) * 1000.0

    def to_dict(self) -> dict:
        return asdict(self)