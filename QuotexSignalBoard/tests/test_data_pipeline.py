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


    def test_rest_seed_promotes_old_forming_bar_after_boundary(self):
        manager = CandleManager("EURUSD", EventDispatcher(), max_history=100)
        start = 1_800_000_000
        start -= start % 60

        first_seed = []
        for i in range(4):
            epoch = start + i * 60
            close = 1.1000 + i * 0.0001
            first_seed.append({
                "epoch": epoch,
                "open": close - 0.00002,
                "high": close + 0.00005,
                "low": close - 0.00005,
                "close": close,
                "ticks_count": 5,
            })

        # At +3m30s, the +3m candle is still forming.
        manager.seed_historical_candles("1M", first_seed, start + 3 * 60 + 30)
        before = manager.get_closed_history("1M")
        self.assertEqual(int(before.iloc[-1]["time"]), start + 2 * 60)
        self.assertEqual(manager.get_current_forming_candle("1M")["epoch"], start + 3 * 60)

        # After the next minute begins, REST returns the now-complete +3m bar
        # plus the new +4m forming bar. The old implementation skipped +3m
        # forever because it matched forming_epoch.
        second_seed = list(first_seed)
        second_seed[-1] = dict(second_seed[-1], close=1.10037, high=1.10042)
        second_seed.append({
            "epoch": start + 4 * 60,
            "open": 1.1004,
            "high": 1.10045,
            "low": 1.10035,
            "close": 1.10042,
            "ticks_count": 2,
        })
        manager.seed_historical_candles("1M", second_seed, start + 4 * 60 + 3)

        after = manager.get_closed_history("1M")
        self.assertEqual(int(after.iloc[-1]["time"]), start + 3 * 60)
        promoted = after[after["time"] == start + 3 * 60].iloc[-1]
        self.assertAlmostEqual(float(promoted["close"]), 1.10037, places=7)
        self.assertEqual(manager.get_current_forming_candle("1M")["epoch"], start + 4 * 60)

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
