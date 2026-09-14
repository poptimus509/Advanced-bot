import unittest

import numpy as np
import pandas as pd

from strategy import evaluate_strategy


def make_trend(rows, start, step, last_candle_direction=None):
    close = start + np.arange(rows, dtype=float) * step
    open_price = close - (step * 0.55)
    high = np.maximum(open_price, close) + abs(step) * 0.25
    low = np.minimum(open_price, close) - abs(step) * 0.25

    if last_candle_direction == "BEARISH":
        open_price[-1] = close[-2] + abs(step) * 0.3
        close[-1] = open_price[-1] - abs(step) * 1.5
        high[-1] = open_price[-1] + abs(step) * 0.2
        low[-1] = close[-1] - abs(step) * 0.2
    elif last_candle_direction == "BULLISH":
        open_price[-1] = close[-2] - abs(step) * 0.3
        close[-1] = open_price[-1] + abs(step) * 1.5
        high[-1] = close[-1] + abs(step) * 0.2
        low[-1] = open_price[-1] - abs(step) * 0.2

    return pd.DataFrame({
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
    })


class StrategyDirectionTests(unittest.TestCase):
    def test_clean_bullish_alignment_returns_call(self):
        result = evaluate_strategy(
            make_trend(80, 1.10, 0.0004, "BULLISH"),
            make_trend(80, 1.10, 0.0010, "BULLISH"),
            make_trend(40, 1.10, 0.0020, "BULLISH"),
        )
        self.assertEqual(result[0], "CALL")
        self.assertGreaterEqual(result[1], 8)

    def test_clean_bearish_alignment_returns_put(self):
        result = evaluate_strategy(
            make_trend(80, 1.30, -0.0004, "BEARISH"),
            make_trend(80, 1.30, -0.0010, "BEARISH"),
            make_trend(40, 1.30, -0.0020, "BEARISH"),
        )
        self.assertEqual(result[0], "PUT")
        self.assertGreaterEqual(result[1], 8)

    def test_bearish_entry_against_bullish_higher_timeframe_is_blocked(self):
        result = evaluate_strategy(
            make_trend(80, 1.10, 0.0004, "BEARISH"),
            make_trend(80, 1.10, 0.0010, "BULLISH"),
            make_trend(40, 1.10, 0.0020, "BULLISH"),
        )
        self.assertEqual(result[0], "NO_TRADE")
        self.assertIn(result[2], {"CONTRADICTING_PA", "ENTRY_TIMEFRAME_MISALIGNMENT", "CANDLE_NOT_CONFIRMED"})

    def test_opposing_15m_bias_is_soft_and_does_not_block_entry(self):
        result = evaluate_strategy(
            make_trend(80, 1.10, 0.0004, "BULLISH"),
            make_trend(80, 1.10, 0.0010, "BULLISH"),
            make_trend(40, 1.30, -0.0020, "BEARISH"),
        )
        self.assertEqual(result[0], "CALL")
        self.assertEqual(result[3]["bias"], "BEARISH")

    def test_insufficient_history_is_blocked(self):
        result = evaluate_strategy(
            make_trend(20, 1.10, 0.0004),
            make_trend(20, 1.10, 0.0010),
            make_trend(10, 1.10, 0.0020),
        )
        self.assertEqual(result[:3], ("NO_TRADE", 0, "INSUFFICIENT_DATA"))


if __name__ == "__main__":
    unittest.main()
