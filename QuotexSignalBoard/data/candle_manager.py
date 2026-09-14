from collections import deque
import math
import threading
from typing import Deque, Dict, List, Optional
import pandas as pd
from core.events import Candle, CandleClosedEvent, EventDispatcher
from core.latency_tracker import PipelineLatencyMetric
from data.data_quality import DataQualityStatus, SymbolHealthTracker

TIMEFRAME_SECONDS = {
    "1M": 60,
    "5M": 300,
    "15M": 900
}

class CandleManager:
    def __init__(self, symbol: str, event_dispatcher: EventDispatcher, max_history: int = 200):
        self.symbol = symbol
        self.dispatcher = event_dispatcher
        self.max_history = max_history
        self._mutex = threading.Lock()
        
        self.health_tracker = SymbolHealthTracker()

        self._closed_candles: Dict[str, Deque[Candle]] = {
            "1M": deque(maxlen=max_history),
            "5M": deque(maxlen=max_history),
            "15M": deque(maxlen=max_history)
        }

        self._forming_candles: Dict[str, Optional[dict]] = {
            "1M": None,
            "5M": None,
            "15M": None
        }

        self._mtf_1m_buffer: Dict[str, List[Candle]] = {
            "5M": [],
            "15M": []
        }

    @staticmethod
    def get_candle_boundary(epoch: int, tf_seconds: int) -> int:
        return math.floor(epoch / tf_seconds) * tf_seconds

    def seed_historical_candles(self, timeframe: str, raw_candles: List[dict], current_server_epoch: int):
        tf_sec = TIMEFRAME_SECONDS[timeframe]
        with self._mutex:
            self._closed_candles[timeframe].clear()
            for c in raw_candles:
                epoch = int(c["epoch"])
                close_epoch = epoch + tf_sec
                if close_epoch <= current_server_epoch:
                    candle = Candle(
                        symbol=self.symbol,
                        timeframe=timeframe,
                        epoch=epoch,
                        open=float(c["open"]),
                        high=float(c["high"]),
                        low=float(c["low"]),
                        close=float(c["close"]),
                        is_closed=True,
                        ticks_count=c.get("count", 1),
                        close_epoch=close_epoch
                    )
                    self._closed_candles[timeframe].append(candle)
                else:
                    self._forming_candles[timeframe] = {
                        "epoch": epoch,
                        "open": float(c["open"]),
                        "high": float(c["high"]),
                        "low": float(c["low"]),
                        "close": float(c["close"]),
                        "ticks_count": c.get("count", 1)
                    }

    def process_tick(self, tick_epoch: int, price: float, local_receipt_time: float) -> List[PipelineLatencyMetric]:
        metrics_captured = []
        closed_1m_events_to_dispatch = []
        closed_mtf_events_to_dispatch = []

        with self._mutex:
            self.health_tracker.record_tick(tick_epoch)
            tf_sec = TIMEFRAME_SECONDS["1M"]
            candle_start = self.get_candle_boundary(tick_epoch, tf_sec)

            curr_1m = self._forming_candles["1M"]

            if curr_1m is None:
                self._forming_candles["1M"] = {
                    "epoch": candle_start,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "ticks_count": 1
                }
            elif candle_start > curr_1m["epoch"]:
                detection_time = local_receipt_time
                prev_1m_candle = Candle(
                    symbol=self.symbol,
                    timeframe="1M",
                    epoch=curr_1m["epoch"],
                    open=curr_1m["open"],
                    high=curr_1m["high"],
                    low=curr_1m["low"],
                    close=curr_1m["close"],
                    is_closed=True,
                    ticks_count=curr_1m["ticks_count"],
                    close_epoch=curr_1m["epoch"] + tf_sec
                )
                self._closed_candles["1M"].append(prev_1m_candle)
                finalization_time = local_receipt_time

                metric = PipelineLatencyMetric(
                    symbol=self.symbol,
                    timeframe="1M",
                    candle_epoch=prev_1m_candle.epoch,
                    tick_server_epoch=tick_epoch,
                    local_tick_receipt_time=local_receipt_time,
                    candle_boundary_time=curr_1m["epoch"] + tf_sec,
                    candle_close_detection_time=detection_time,
                    candle_finalization_time=finalization_time
                )
                metrics_captured.append(metric)

                df_history_1m = self._build_dataframe("1M")
                
                # FIX: epoch এবং candle_epoch উভয়ই পাস করা হলো যাতে কোনো অ্যাট্রিবিউট মিস না হয়
                ev_1m = CandleClosedEvent(
                    symbol=self.symbol,
                    timeframe="1M",
                    candle_epoch=prev_1m_candle.epoch,
                    candle=prev_1m_candle,
                    server_time_at_close=tick_epoch,
                    closed_history=df_history_1m
                )
                setattr(ev_1m, "epoch", prev_1m_candle.epoch)
                closed_1m_events_to_dispatch.append(ev_1m)

                for htf in ["5M", "15M"]:
                    htf_sec = TIMEFRAME_SECONDS[htf]
                    self._mtf_1m_buffer[htf].append(prev_1m_candle)

                    next_1m_boundary = prev_1m_candle.epoch + 60
                    if next_1m_boundary % htf_sec == 0:
                        htf_epoch = next_1m_boundary - htf_sec
                        constituent_candles = [c for c in self._mtf_1m_buffer[htf] if c.epoch >= htf_epoch]
                        
                        if constituent_candles:
                            htf_candle = Candle(
                                symbol=self.symbol,
                                timeframe=htf,
                                epoch=htf_epoch,
                                open=constituent_candles[0].open,
                                high=max(c.high for c in constituent_candles),
                                low=min(c.low for c in constituent_candles),
                                close=constituent_candles[-1].close,
                                is_closed=True,
                                ticks_count=sum(c.ticks_count for c in constituent_candles),
                                close_epoch=next_1m_boundary
                            )
                            self._closed_candles[htf].append(htf_candle)
                            self._mtf_1m_buffer[htf] = [c for c in self._mtf_1m_buffer[htf] if c.epoch >= next_1m_boundary]

                            df_history_htf = self._build_dataframe(htf)
                            ev_htf = CandleClosedEvent(
                                symbol=self.symbol,
                                timeframe=htf,
                                candle_epoch=htf_candle.epoch,
                                candle=htf_candle,
                                server_time_at_close=tick_epoch,
                                closed_history=df_history_htf
                            )
                            setattr(ev_htf, "epoch", htf_candle.epoch)
                            closed_mtf_events_to_dispatch.append(ev_htf)

                self._forming_candles["1M"] = {
                    "epoch": candle_start,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "ticks_count": 1
                }
            else:
                curr_1m["high"] = max(curr_1m["high"], price)
                curr_1m["low"] = min(curr_1m["low"], price)
                curr_1m["close"] = price
                curr_1m["ticks_count"] += 1

        for ev in closed_1m_events_to_dispatch:
            try:
                self.dispatcher.dispatch_candle_closed(ev)
            except Exception as ex:
                pass

        for ev in closed_mtf_events_to_dispatch:
            try:
                self.dispatcher.dispatch_candle_closed(ev)
            except Exception as ex:
                pass

        return metrics_captured

    def _build_dataframe(self, timeframe: str) -> pd.DataFrame:
        candles = list(self._closed_candles[timeframe])
        if not candles:
            return pd.DataFrame(columns=["open", "high", "low", "close", "tickscount"])
        data = [{
            "time": c.epoch,
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
            "tickscount": c.ticks_count
        } for c in candles]
        df = pd.DataFrame(data)
        return df

    def get_closed_history(self, timeframe: str) -> pd.DataFrame:
        with self._mutex:
            return self._build_dataframe(timeframe)

    def get_latest_closed_candle(self, timeframe: str) -> Optional[Candle]:
        with self._mutex:
            if self._closed_candles[timeframe]:
                return self._closed_candles[timeframe][-1]
            return None

    def get_current_forming_candle(self, timeframe: str = "1M") -> Optional[dict]:
        with self._mutex:
            if self._forming_candles[timeframe]:
                return dict(self._forming_candles[timeframe])
            return None
