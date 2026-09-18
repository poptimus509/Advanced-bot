import unittest

import numpy as np
import pandas as pd

from core.events import EventDispatcher
from data.candle_manager import CandleManager
from indicators import calculate_adx, calculate_rsi


class DataPipelineTests(unittest.TestCase):
    def test_rsi_handles_monotonic_series(self):
        up = pd.Series(np.arange(1.0, 80.0))
        down = pd.Series(np.arange(80.0, 1.0, -1.0))
        self.assertGreater(float(calculate_rsi(up).iloc[-1]), 99.0)
        self.assertLess(float(calculate_rsi(down).iloc[-1]), 1.0)

    def test_adx_detects_both_up_and_down_trends(self):
        n = 100
        base_up = np.linspace(100.0, 120.0, n)
        base_down = np.linspace(120.0, 100.0, n)

        def frame(close):
            open_ = np.r_[close[0], close[:-1]]
            return pd.DataFrame({
                "open": open_,
                "high": np.maximum(open_, close) + 0.1,
                "low": np.minimum(open_, close) - 0.1,
                "close": close,
            })

        up_adx = float(calculate_adx(frame(base_up)).dropna().iloc[-1])
        down_adx = float(calculate_adx(frame(base_down)).dropna().iloc[-1])
        self.assertGreater(up_adx, 20.0)
        self.assertGreater(down_adx, 20.0)

    def test_historical_1m_seed_builds_only_complete_5m_bars(self):
        manager = CandleManager("EURUSD", EventDispatcher(), max_history=500)
        start = 1_800_000_000
        start -= start % 300
        raw = []
        price = 1.1
        for i in range(120):
            epoch = start + i * 60
            close = price + i * 0.00001
            raw.append({
                "epoch": epoch,
                "open": close,
                "high": close + 0.0001,
                "low": close - 0.0001,
                "close": close,
                "ticks_count": 10,
            })

        manager.seed_historical_candles("1M", raw, start + 120 * 60 + 1)
        df_1m = manager.get_closed_history("1M")
        df_5m = manager.get_closed_history("5M")
        self.assertEqual(len(df_1m), 120)
        self.assertEqual(len(df_5m), 24)
        self.assertTrue(((df_5m["time"] % 300) == 0).all())

    def test_seed_is_idempotent_no_duplicate_epochs(self):
        manager = CandleManager("EURUSD", EventDispatcher(), max_history=100)
        start = 1_800_000_000
        start -= start % 60
        raw = []
        for i in range(30):
            epoch = start + i * 60
            close = 1.1 + i * 0.00001
            raw.append({
                "epoch": epoch,
                "open": close,
                "high": close + 0.0001,
                "low": close - 0.0001,
                "close": close,
                "ticks_count": 5,
            })
        now = start + 31 * 60
        manager.seed_historical_candles("1M", raw, now)
        manager.seed_historical_candles("1M", raw, now)
        df = manager.get_closed_history("1M")
        self.assertEqual(len(df), 30)
        self.assertEqual(df["time"].nunique(), 30)


if __name__ == "__main__":
    unittest.main()
