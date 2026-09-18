"""
Research-only robustness harness for QuotexSignalBoard.

Purpose:
- Test whether the rule engine over-triggers on synthetic/no-edge data.
- Compare thresholds without treating a lower trigger rate as profitability.
- Measure directional balance and component activation rates.
- Run out-of-sample style seed splits to expose brittle parameter behavior.

This script intentionally does NOT fetch broker data, place trades, model payouts,
or report profit/win-rate. It is for software/strategy robustness testing only.
"""

from __future__ import annotations

import argparse
import contextlib
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

import config as cfg
from strategy import evaluate_strategy


@dataclass
class ThresholdResult:
    threshold: int
    trials: int
    signals: int
    signal_rate: float
    calls: int
    puts: int
    call_share: float
    train_signal_rate: float
    test_signal_rate: float
    train_test_gap: float


def synthetic_random_walk(seed: int, rows: int = 240, sigma: float = 0.0004) -> pd.DataFrame:
    """Zero-drift geometric random walk with plausible OHLC ranges."""
    rng = np.random.default_rng(seed)
    returns = rng.normal(0.0, sigma, rows)
    close = 1.10 * np.exp(np.cumsum(returns))
    open_ = np.r_[close[0], close[:-1]]
    wick = np.abs(rng.normal(0.0, sigma * 0.5, rows))
    high = np.maximum(open_, close) + wick
    low = np.minimum(open_, close) - wick

    return pd.DataFrame(
        {
            "time": 1_800_000_000 + np.arange(rows) * 60,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
        }
    )


def synthetic_drift(seed: int, rows: int = 240, sigma: float = 0.0004, drift: float = 0.00008) -> pd.DataFrame:
    """Small-drift series used only as a sensitivity test, not a profitability proxy."""
    rng = np.random.default_rng(seed)
    returns = rng.normal(drift, sigma, rows)
    close = 1.10 * np.exp(np.cumsum(returns))
    open_ = np.r_[close[0], close[:-1]]
    wick = np.abs(rng.normal(0.0, sigma * 0.5, rows))
    high = np.maximum(open_, close) + wick
    low = np.minimum(open_, close) - wick

    return pd.DataFrame(
        {
            "time": 1_800_000_000 + np.arange(rows) * 60,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
        }
    )


@contextlib.contextmanager
def temporary_threshold(value: int):
    old = getattr(cfg, "SIGNAL_THRESHOLD_CALL_PUT", 8)
    cfg.SIGNAL_THRESHOLD_CALL_PUT = value
    try:
        yield
    finally:
        cfg.SIGNAL_THRESHOLD_CALL_PUT = old


def evaluate_seeds(seeds: Iterable[int], threshold: int, rows: int, sigma: float) -> Tuple[int, int, int]:
    calls = puts = signals = 0
    with temporary_threshold(threshold):
        for seed in seeds:
            direction, _, _, _ = evaluate_strategy(synthetic_random_walk(seed, rows=rows, sigma=sigma))
            if direction == "CALL":
                calls += 1
                signals += 1
            elif direction == "PUT":
                puts += 1
                signals += 1
    return signals, calls, puts


def component_activation(seeds: Iterable[int], rows: int, sigma: float) -> Dict[str, float]:
    """Count which score reasons activate before thresholding at the lowest supported value."""
    counts: Dict[str, int] = {}
    total = 0

    with temporary_threshold(0):
        for seed in seeds:
            _, _, _, details = evaluate_strategy(synthetic_random_walk(seed, rows=rows, sigma=sigma))
            total += 1
            reason = str(details.get("score_reason", ""))
            for token in [x for x in reason.split("+") if x and x != "WAITING_SETUP"]:
                counts[token] = counts.get(token, 0) + 1

    if total == 0:
        return {}
    return {k: v / total for k, v in sorted(counts.items())}


def run_threshold_sweep(trials: int, rows: int, sigma: float, thresholds: List[int]) -> List[ThresholdResult]:
    split = int(trials * 0.7)
    train_seeds = range(0, split)
    test_seeds = range(split, trials)
    all_seeds = range(trials)
    results: List[ThresholdResult] = []

    for threshold in thresholds:
        signals, calls, puts = evaluate_seeds(all_seeds, threshold, rows, sigma)
        train_signals, _, _ = evaluate_seeds(train_seeds, threshold, rows, sigma)
        test_signals, _, _ = evaluate_seeds(test_seeds, threshold, rows, sigma)

        signal_rate = signals / trials if trials else 0.0
        call_share = calls / signals if signals else 0.0
        train_rate = train_signals / max(split, 1)
        test_n = max(trials - split, 1)
        test_rate = test_signals / test_n

        results.append(
            ThresholdResult(
                threshold=threshold,
                trials=trials,
                signals=signals,
                signal_rate=signal_rate,
                calls=calls,
                puts=puts,
                call_share=call_share,
                train_signal_rate=train_rate,
                test_signal_rate=test_rate,
                train_test_gap=abs(train_rate - test_rate),
            )
        )
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic robustness test for the signal engine")
    parser.add_argument("--trials", type=int, default=800)
    parser.add_argument("--rows", type=int, default=240)
    parser.add_argument("--sigma", type=float, default=0.0004)
    parser.add_argument("--thresholds", default="5,6,7,8")
    parser.add_argument("--output-dir", default="research_results")
    args = parser.parse_args()

    thresholds = [int(x.strip()) for x in args.thresholds.split(",") if x.strip()]
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    results = run_threshold_sweep(args.trials, args.rows, args.sigma, thresholds)
    result_df = pd.DataFrame([asdict(r) for r in results])
    result_df.to_csv(out_dir / "threshold_sweep.csv", index=False)

    components = component_activation(range(args.trials), args.rows, args.sigma)
    with open(out_dir / "component_activation.json", "w", encoding="utf-8") as f:
        json.dump(components, f, indent=2)

    summary = {
        "purpose": "research_only_synthetic_robustness",
        "trials": args.trials,
        "rows": args.rows,
        "sigma": args.sigma,
        "thresholds": thresholds,
        "results": [asdict(r) for r in results],
        "component_activation": components,
        "interpretation": (
            "Lower signal rate on zero-drift synthetic data can indicate better noise rejection, "
            "but it does not establish predictive edge or profitability."
        ),
    }
    with open(out_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\nSynthetic robustness results")
    print(result_df.to_string(index=False, formatters={
        "signal_rate": "{:.2%}".format,
        "call_share": "{:.2%}".format,
        "train_signal_rate": "{:.2%}".format,
        "test_signal_rate": "{:.2%}".format,
        "train_test_gap": "{:.2%}".format,
    }))
    print(f"\nSaved: {out_dir / 'threshold_sweep.csv'}")
    print(f"Saved: {out_dir / 'component_activation.json'}")
    print(f"Saved: {out_dir / 'summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
