"""
Calibration guard-rail.

The single most important property of a signal engine is that it does
NOT fire constantly on data with no real trend (pure random walk). The
original strategy fired on ~50% of pure noise segments - i.e. it was
close to a coin flip dressed up as "high confluence".

This test generates geometric random walks with zero drift and checks
that evaluate_strategy() stays quiet on the overwhelming majority of
them. The bound (15%) is intentionally generous: some false positives
are unavoidable because short random-walk segments occasionally produce
runs that look like real trends by chance. If this number creeps up
after a change to strategy.py, that change likely reintroduced double
counting or removed a real filter - re-run this before shipping.

This does NOT prove the strategy is profitable on real markets. It only
proves it isn't trivially over-triggering on noise.
"""

import unittest
import numpy as np
import pandas as pd

from strategy import evaluate_strategy


def random_walk(seed: int, rows: int = 200) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    returns = rng.normal(0.0, 0.0004, rows)
    close = 1.10 * np.exp(np.cumsum(returns))
    open_ = np.r_[close[0], close[:-1]]
    high = np.maximum(open_, close) + np.abs(rng.normal(0, 0.0002, rows))
    low = np.minimum(open_, close) - np.abs(rng.normal(0, 0.0002, rows))
    return pd.DataFrame({
        "time": np.arange(rows) * 60 + 1_800_000_000,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "tickscount": 10,
    })


class NoiseRejectionTests(unittest.TestCase):
    def test_pure_noise_rarely_triggers_a_signal(self):
        trials = 800
        signals = 0
        for seed in range(trials):
            direction = evaluate_strategy(random_walk(seed))[0]
            if direction in ("CALL", "PUT"):
                signals += 1

        rate = signals / trials
        self.assertLess(
            rate, 0.15,
            f"Signal rate on pure noise was {rate:.1%}; strategy is over-triggering."
        )


if __name__ == "__main__":
    unittest.main()
