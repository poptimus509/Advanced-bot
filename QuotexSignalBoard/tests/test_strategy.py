import unittest

import numpy as np
import pandas as pd

from strategy import (
    prepare_history,
    confirmed_swings,
    evaluate_strategy,
    activity_pressure,
    candle_reaction,
)


def sample_history(rows=240):
    x = np.arange(rows, dtype=float)
    close = 100 + 0.03 * x + 1.5 * np.sin(x / 6)
    opening = np.r_[close[0], close[:-1]]

    return pd.DataFrame({
        "time": 1800000000 + np.arange(rows) * 60,
        "open": opening,
        "high": np.maximum(opening, close) + 0.12,
        "low": np.minimum(opening, close) - 0.12,
        "close": close,
        "tickscount": 1,
    })


class StrategyTests(unittest.TestCase):
    def test_missing_history_is_no_trade(self):
        self.assertEqual(evaluate_strategy(None)[0], "NO_TRADE")

    def test_invalid_ohlc_rejected(self):
        df = sample_history()
        df.loc[len(df) - 1, "high"] = 0
        self.assertEqual(evaluate_strategy(df)[0], "NO_TRADE")

    def test_gap_uses_latest_segment(self):
        df = sample_history()
        df.loc[200:, "time"] += 60
        self.assertEqual(len(prepare_history(df)), 40)
        self.assertEqual(evaluate_strategy(df)[0], "NO_TRADE")

    def test_higher_timeframes_have_no_effect(self):
        df = sample_history()
        expected = evaluate_strategy(df, None, None)
        actual = evaluate_strategy(
            df, pd.DataFrame({"garbage": [1]}), object()
        )
        self.assertEqual(expected, actual)

    def test_input_not_modified(self):
        df = sample_history()
        original = df.copy(deep=True)
        evaluate_strategy(df)
        pd.testing.assert_frame_equal(df, original)

    def test_timestamp_comes_from_time_column(self):
        df = sample_history()
        details = evaluate_strategy(df)[3]
        self.assertEqual(
            details["signal_candle_epoch"], int(df.iloc[-1]["time"])
        )

    def test_historical_count_is_not_volume(self):
        result = activity_pressure(sample_history())
        self.assertEqual(result["activity_status"], "UNAVAILABLE")
        self.assertIsNone(result["pressure"])

    def test_live_counts_enable_activity_proxy(self):
        df = sample_history()
        df["verified_ticks"] = 30.0
        df.loc[len(df) - 1, "verified_ticks"] = 60.0
        result = activity_pressure(df)
        self.assertEqual(result["activity_status"], "LIVE_TICK_PROXY")
        self.assertAlmostEqual(result["relative_activity"], 2.0)

    def test_future_bars_do_not_rewrite_confirmed_pivots(self):
        df = sample_history()
        cutoff = 170
        prefix = confirmed_swings(df.iloc[:cutoff].reset_index(drop=True))
        full = confirmed_swings(df)
        already_confirmed = [
            p for p in full if p["confirmed_index"] < cutoff
        ]
        self.assertGreater(len(prefix), 0)
        self.assertEqual(prefix, already_confirmed)

    def test_bearish_candle_cannot_confirm_bullish_entry(self):
        previous = pd.Series({
            "open": 100, "high": 102, "low": 99, "close": 101
        })
        current = pd.Series({
            "open": 101, "high": 101.2, "low": 98, "close": 98.5
        })
        self.assertIsNone(candle_reaction(current, previous, True))


if __name__ == "__main__":
    unittest.main()
