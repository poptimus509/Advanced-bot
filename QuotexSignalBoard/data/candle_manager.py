from collections import deque
import math
import threading
import time
from typing import Deque, Dict, List, Optional

import pandas as pd

from core.events import Candle, CandleClosedEvent, EventDispatcher
from core.latency_tracker import PipelineLatencyMetric
from data.data_quality import SymbolHealthTracker

TIMEFRAME_SECONDS = {
    "1M": 60,
    "5M": 300,
    "15M": 900,
}


class CandleManager:
    def __init__(self, symbol: str, event_dispatcher: EventDispatcher, max_history: int = 300):
        self.symbol = symbol
        self.dispatcher = event_dispatcher
        self.max_history = max_history
        self._mutex = threading.Lock()

        self.health_tracker = SymbolHealthTracker()

        self._closed_candles: Dict[str, Deque[Candle]] = {
            "1M": deque(maxlen=max_history),
            "5M": deque(maxlen=max_history),
            "15M": deque(maxlen=max_history),
        }

        self._forming_candles: Dict[str, Optional[dict]] = {
            "1M": None,
            "5M": None,
            "15M": None,
        }

        self._mtf_1m_buffer: Dict[str, List[Candle]] = {
            "5M": [],
            "15M": [],
        }

    @staticmethod
    def get_candle_boundary(epoch: int, tf_seconds: int) -> int:
        return math.floor(epoch / tf_seconds) * tf_seconds

    @staticmethod
    def _valid_ohlc(o: float, h: float, l: float, c: float) -> bool:
        values = (o, h, l, c)
        if not all(math.isfinite(v) and v > 0 for v in values):
            return False
        if h < l:
            return False
        if not (l <= o <= h and l <= c <= h):
            return False
        return True

    def _upsert_closed_candle_locked(self, timeframe: str, candle: Candle):
        """Insert/replace by epoch so REST + live paths can never duplicate a bar."""
        current = list(self._closed_candles[timeframe])
        replaced = False
        for idx, existing in enumerate(current):
            if existing.epoch == candle.epoch:
                current[idx] = candle
                replaced = True
                break
        if not replaced:
            current.append(candle)
        current.sort(key=lambda x: x.epoch)
        self._closed_candles[timeframe] = deque(current, maxlen=self.max_history)

    def _rebuild_higher_timeframes_from_1m_locked(self):
        """
        Rebuild 5M/15M history from complete, aligned 1M groups.

        This fixes the cold-start problem where 5M history was empty until
        enough live minutes had elapsed, and prevents partial 5M/15M bars
        from being created after a mid-block restart.
        """
        source = sorted(list(self._closed_candles["1M"]), key=lambda c: c.epoch)
        by_epoch = {c.epoch: c for c in source}

        for timeframe in ("5M", "15M"):
            tf_sec = TIMEFRAME_SECONDS[timeframe]
            expected_count = tf_sec // 60
            built: List[Candle] = []

            if source:
                first_boundary = self.get_candle_boundary(source[0].epoch, tf_sec)
                last_epoch = source[-1].epoch
                start = first_boundary
                while start + tf_sec - 60 <= last_epoch:
                    epochs = [start + i * 60 for i in range(expected_count)]
                    parts = [by_epoch.get(ep) for ep in epochs]
                    if all(parts):
                        built.append(
                            Candle(
                                symbol=self.symbol,
                                timeframe=timeframe,
                                epoch=start,
                                open=parts[0].open,
                                high=max(c.high for c in parts),
                                low=min(c.low for c in parts),
                                close=parts[-1].close,
                                is_closed=True,
                                ticks_count=sum(int(c.ticks_count or 0) for c in parts),
                                close_epoch=start + tf_sec,
                            )
                        )
                    start += tf_sec

            self._closed_candles[timeframe] = deque(built, maxlen=self.max_history)

            # Keep only the trailing incomplete block for future live ticks.
            buffer_start = None
            if source:
                latest_closed_end = source[-1].epoch + 60
                buffer_start = self.get_candle_boundary(latest_closed_end, tf_sec)
            self._mtf_1m_buffer[timeframe] = (
                [c for c in source if buffer_start is not None and c.epoch >= buffer_start]
                if buffer_start is not None
                else []
            )

    def seed_historical_candles(
        self,
        timeframe: str,
        raw_candles: List[dict],
        current_server_epoch: Optional[int] = None,
    ):
        """Merge closed historical candles without erasing live forming state."""
        if not raw_candles or timeframe not in TIMEFRAME_SECONDS:
            return

        tf_sec = TIMEFRAME_SECONDS[timeframe]
        now_epoch = (
            int(current_server_epoch)
            if current_server_epoch and current_server_epoch > 0
            else int(time.time())
        )

        with self._mutex:
            forming = self._forming_candles[timeframe]
            forming_epoch = forming["epoch"] if forming else None

            sorted_raw = sorted(
                raw_candles,
                key=lambda x: int(x.get("epoch", x.get("time", 0)) or 0),
            )

            for raw in sorted_raw:
                try:
                    epoch = int(raw.get("epoch", raw.get("time", 0)) or 0)
                    if epoch <= 0:
                        continue
                    close_epoch = epoch + tf_sec
                    if close_epoch > now_epoch:
                        continue
                    if forming_epoch is not None and epoch == forming_epoch:
                        continue

                    o = float(raw.get("open", 0.0))
                    h = float(raw.get("high", 0.0))
                    l = float(raw.get("low", 0.0))
                    c = float(raw.get("close", 0.0))
                    if not self._valid_ohlc(o, h, l, c):
                        continue

                    candle = Candle(
                        symbol=self.symbol,
                        timeframe=timeframe,
                        epoch=epoch,
                        open=o,
                        high=h,
                        low=l,
                        close=c,
                        is_closed=True,
                        ticks_count=int(raw.get("count", raw.get("ticks_count", 1)) or 1),
                        close_epoch=close_epoch,
                    )
                    self._upsert_closed_candle_locked(timeframe, candle)
                except Exception:
                    continue

            # Cold start: preserve a current, still-forming REST bar only if
            # live ticks have not already created a forming candle.
            if self._forming_candles[timeframe] is None and sorted_raw:
                try:
                    last_raw = sorted_raw[-1]
                    last_epoch = int(last_raw.get("epoch", last_raw.get("time", 0)) or 0)
                    if last_epoch > 0 and last_epoch + tf_sec > now_epoch:
                        o = float(last_raw.get("open", 0.0))
                        h = float(last_raw.get("high", 0.0))
                        l = float(last_raw.get("low", 0.0))
                        c = float(last_raw.get("close", 0.0))
                        if self._valid_ohlc(o, h, l, c):
                            self._forming_candles[timeframe] = {
                                "epoch": last_epoch,
                                "open": o,
                                "high": h,
                                "low": l,
                                "close": c,
                                "ticks_count": int(
                                    last_raw.get("count", last_raw.get("ticks_count", 1)) or 1
                                ),
                            }
                except Exception:
                    pass

            if timeframe == "1M":
                self._rebuild_higher_timeframes_from_1m_locked()

    def process_tick(
        self,
        tick_epoch: int,
        price: float,
        local_receipt_time: float,
    ) -> List[PipelineLatencyMetric]:
        metrics_captured: List[PipelineLatencyMetric] = []
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
                    "ticks_count": 1,
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
                    close_epoch=curr_1m["epoch"] + tf_sec,
                )
                self._upsert_closed_candle_locked("1M", prev_1m_candle)
                finalization_time = local_receipt_time

                metrics_captured.append(
                    PipelineLatencyMetric(
                        symbol=self.symbol,
                        timeframe="1M",
                        candle_epoch=prev_1m_candle.epoch,
                        tick_server_epoch=tick_epoch,
                        local_tick_receipt_time=local_receipt_time,
                        candle_boundary_time=curr_1m["epoch"] + tf_sec,
                        candle_close_detection_time=detection_time,
                        candle_finalization_time=finalization_time,
                    )
                )

                ev_1m = CandleClosedEvent(
                    symbol=self.symbol,
                    timeframe="1M",
                    candle_epoch=prev_1m_candle.epoch,
                    candle=prev_1m_candle,
                    server_time_at_close=tick_epoch,
                    closed_history=self._build_dataframe("1M"),
                )
                setattr(ev_1m, "epoch", prev_1m_candle.epoch)
                closed_1m_events_to_dispatch.append(ev_1m)

                # Build higher timeframe bars only when every constituent
                # one-minute candle is present and aligned.
                self._rebuild_higher_timeframes_from_1m_locked()
                for htf in ("5M", "15M"):
                    latest = self._closed_candles[htf][-1] if self._closed_candles[htf] else None
                    if latest and latest.close_epoch == prev_1m_candle.close_epoch:
                        ev_htf = CandleClosedEvent(
                            symbol=self.symbol,
                            timeframe=htf,
                            candle_epoch=latest.epoch,
                            candle=latest,
                            server_time_at_close=tick_epoch,
                            closed_history=self._build_dataframe(htf),
                        )
                        setattr(ev_htf, "epoch", latest.epoch)
                        closed_mtf_events_to_dispatch.append(ev_htf)

                self._forming_candles["1M"] = {
                    "epoch": candle_start,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "ticks_count": 1,
                }

            elif candle_start == curr_1m["epoch"]:
                curr_1m["high"] = max(curr_1m["high"], price)
                curr_1m["low"] = min(curr_1m["low"], price)
                curr_1m["close"] = price
                curr_1m["ticks_count"] += 1
            else:
                # Out-of-order tick from an older minute. Do not corrupt the
                # currently forming candle.
                return metrics_captured

        for event in closed_1m_events_to_dispatch:
            try:
                self.dispatcher.dispatch_candle_closed(event)
            except Exception:
                pass

        for event in closed_mtf_events_to_dispatch:
            try:
                self.dispatcher.dispatch_candle_closed(event)
            except Exception:
                pass

        return metrics_captured

    def _build_dataframe(self, timeframe: str) -> pd.DataFrame:
        candles = sorted(list(self._closed_candles[timeframe]), key=lambda c: c.epoch)
        if not candles:
            return pd.DataFrame(
                columns=["time", "epoch", "open", "high", "low", "close", "tickscount"]
            )
        return pd.DataFrame(
            [
                {
                    "time": c.epoch,
                    "epoch": c.epoch,
                    "open": float(c.open),
                    "high": float(c.high),
                    "low": float(c.low),
                    "close": float(c.close),
                    "tickscount": int(c.ticks_count or 0),
                }
                for c in candles
            ]
        )

    def get_closed_history(self, timeframe: str) -> pd.DataFrame:
        with self._mutex:
            return self._build_dataframe(timeframe)

    def get_latest_closed_candle(self, timeframe: str) -> Optional[Candle]:
        with self._mutex:
            if self._closed_candles[timeframe]:
                return sorted(self._closed_candles[timeframe], key=lambda c: c.epoch)[-1]
            return None

    def get_current_forming_candle(self, timeframe: str = "1M") -> Optional[dict]:
        with self._mutex:
            if self._forming_candles[timeframe]:
                return dict(self._forming_candles[timeframe])
            return None

    def diagnostics(self, timeframe: str = "1M") -> dict:
        """Small, log-friendly snapshot used to diagnose missing-history issues."""
        with self._mutex:
            candles = sorted(list(self._closed_candles.get(timeframe, [])), key=lambda c: c.epoch)
            epochs = [int(c.epoch) for c in candles]
            gaps = [epochs[i] - epochs[i - 1] for i in range(1, len(epochs))]
            expected = TIMEFRAME_SECONDS.get(timeframe, 0)
            abnormal = [g for g in gaps if expected and g != expected]
            forming = self._forming_candles.get(timeframe)
            return {
                "count": len(candles),
                "first_epoch": epochs[0] if epochs else None,
                "last_epoch": epochs[-1] if epochs else None,
                "forming_epoch": int(forming["epoch"]) if forming else None,
                "duplicate_epochs": len(epochs) - len(set(epochs)),
                "abnormal_gap_count": len(abnormal),
                "latest_abnormal_gap": abnormal[-1] if abnormal else None,
            }
