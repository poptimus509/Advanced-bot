"""
backtest.py
-----------
Walk-forward backtester for strategy.evaluate_strategy().

WHY THIS EXISTS
    The live bot calls evaluate_strategy(1M, 5M, 15M) once per closed 1M candle.
    This script replays historical 1M candles bar-by-bar, rebuilding the 5M/15M
    context on every step EXACTLY the way the live bot would see it at that
    moment in time (no lookahead: a 5M/15M candle is only "seen" once it has
    actually closed). It then simulates the 60-second binary outcome for every
    CALL/PUT the strategy would have fired, and reports win-rate, expectancy,
    and a few useful breakdowns.

IMPORTANT - READ BEFORE TRUSTING THE NUMBERS
    1. This backtests the SIGNAL LOGIC only. It does not, and cannot, fix the
       Deriv-vs-Quotex feed mismatch already documented in CORRECTIONS.md.
       A good win-rate here on Deriv history is evidence the *logic* has an
       edge - it is NOT proof the same edge exists on Quotex's own OTC feed.
    2. Binary options pay less than 100% on a win (typically ~70-90%) and
       100% of stake on a loss. A 50% win-rate LOSES money. Use --payout to
       set your broker's actual payout and look at "expectancy", not just
       win-rate.
    3. No spread/slippage/execution-delay is modeled beyond the deliberate
       60-second expiry. Treat results as an upper bound on real performance.

USAGE
    # Real historical data (recommended): one CSV per symbol with at least
    # the columns: time (unix epoch seconds, 1-minute spacing), open, high,
    # low, close.
    python backtest.py --data-dir ./historical_1m --payout 0.85

    # Quick sanity/demo run on synthetic random-walk data (clearly labeled,
    # NOT a real result) - useful to confirm the script itself works:
    python backtest.py --demo --payout 0.85

    # Sweep SIGNAL_THRESHOLD to help pick a value before touching strategy.py:
    python backtest.py --data-dir ./historical_1m --sweep-threshold 6 7 8 9

CSV FORMAT
    time,open,high,low,close
    1800000000,1.10502,1.10515,1.10495,1.10510
    1800000060,1.10510,1.10520,1.10501,1.10518
    ...
    (epoch seconds, exactly one row per closed 1-minute candle; gaps are fine,
    prepare_history()'s own gap-detection already handles that on the live
    path, and this script re-uses the same aggregation logic here.)
"""

import argparse
import glob
import os
import sys
import math
import numpy as np
import pandas as pd

import strategy


# ---------------------------------------------------------------------------
# 1M -> 5M / 15M aggregation (no lookahead: only fully-closed buckets kept)
# ---------------------------------------------------------------------------
def aggregate_candles(df_1m: pd.DataFrame, timeframe_seconds: int) -> pd.DataFrame:
    if df_1m is None or df_1m.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close"])

    bars_per_bucket = timeframe_seconds // 60
    df = df_1m.copy()
    df["bucket"] = (df["time"] // timeframe_seconds) * timeframe_seconds

    grouped = df.groupby("bucket")
    rows = []
    for bucket_start, g in grouped:
        if len(g) < bars_per_bucket:
            # Incomplete bucket (still forming, or a gap swallowed some bars) -
            # never expose a partially-formed HTF candle to the strategy.
            continue
        g = g.sort_values("time")
        rows.append({
            "time": int(bucket_start),
            "open": float(g.iloc[0]["open"]),
            "high": float(g["high"].max()),
            "low": float(g["low"].min()),
            "close": float(g.iloc[-1]["close"]),
        })
    return pd.DataFrame(rows).sort_values("time").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_ohlc_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    rename_map = {}
    for candidate in ("time", "epoch", "timestamp"):
        if candidate in df.columns:
            rename_map[candidate] = "time"
            break
    df = df.rename(columns=rename_map)
    required = {"time", "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path}: missing required columns {missing}")
    df = df[["time", "open", "high", "low", "close"]].dropna()
    df["time"] = df["time"].astype(np.int64)
    df = df.sort_values("time").drop_duplicates(subset="time").reset_index(drop=True)
    return df


def make_synthetic_demo_data(rows: int = 5000, seed: int = 7) -> pd.DataFrame:
    """
    Clearly-labeled synthetic 1M data: random walk + a slow regime-switching
    drift, so the script has something non-trivial to chew on. This is ONLY
    for confirming the backtester itself runs end-to-end - it says nothing
    about real market performance.
    """
    rng = np.random.default_rng(seed)
    steps = rng.normal(loc=0.0, scale=0.05, size=rows)
    drift = np.sin(np.arange(rows) / 180.0) * 0.03
    close = 100 + np.cumsum(steps + drift)
    opening = np.r_[close[0], close[:-1]]
    wick = np.abs(rng.normal(0.03, 0.02, size=rows))
    high = np.maximum(opening, close) + wick
    low = np.minimum(opening, close) - wick
    time = 1_800_000_000 + np.arange(rows) * 60
    return pd.DataFrame({"time": time, "open": opening, "high": high, "low": low, "close": close})


# ---------------------------------------------------------------------------
# Walk-forward simulation
# ---------------------------------------------------------------------------
def run_backtest(df_1m: pd.DataFrame, symbol_label: str = "SYMBOL",
                  lookback_1m: int = 300, lookback_htf: int = 150,
                  expiry_seconds: int = 60, warmup: int = 150) -> pd.DataFrame:
    """
    Returns a DataFrame of every CALL/PUT the strategy would have fired,
    one row per trade, with the simulated outcome already attached.
    """
    df_5m_full = aggregate_candles(df_1m, 300)
    df_15m_full = aggregate_candles(df_1m, 900)

    time_to_index = {int(t): i for i, t in enumerate(df_1m["time"].values)}
    trades = []

    n = len(df_1m)
    for i in range(warmup, n):
        window_1m = df_1m.iloc[max(0, i - lookback_1m + 1): i + 1]
        current_time = int(df_1m.iloc[i]["time"])

        window_5m = df_5m_full[df_5m_full["time"] + 300 <= current_time].tail(lookback_htf)
        window_15m = df_15m_full[df_15m_full["time"] + 900 <= current_time].tail(lookback_htf)

        try:
            direction, score, quality, details = strategy.evaluate_strategy(
                window_1m, window_5m, window_15m
            )
        except Exception as exc:  # a bad row should not kill the whole backtest
            print(f"[warn] evaluate_strategy failed at i={i}: {exc}", file=sys.stderr)
            continue

        if direction not in ("CALL", "PUT"):
            continue

        entry_price = float(df_1m.iloc[i]["close"])
        expiry_time = current_time + expiry_seconds
        exit_idx = time_to_index.get(expiry_time)
        if exit_idx is None:
            continue  # end of data / gap right after entry - can't score this trade

        exit_price = float(df_1m.iloc[exit_idx]["close"])

        if direction == "CALL":
            outcome = "WIN" if exit_price > entry_price else ("LOSE" if exit_price < entry_price else "DRAW")
        else:
            outcome = "WIN" if exit_price < entry_price else ("LOSE" if exit_price > entry_price else "DRAW")

        htf_aligned = (
            (direction == "CALL" and details.get("trend_5m") == "BULLISH" and details.get("trend_15m") == "BULLISH")
            or (direction == "PUT" and details.get("trend_5m") == "BEARISH" and details.get("trend_15m") == "BEARISH")
        )

        trades.append({
            "symbol": symbol_label,
            "entry_time": current_time,
            "direction": direction,
            "score": score,
            "quality": quality,
            "trend_5m": details.get("trend_5m"),
            "trend_15m": details.get("trend_15m"),
            "sr_context": details.get("sr_context"),
            "htf_aligned": htf_aligned,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "outcome": outcome,
        })

    return pd.DataFrame(trades)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def summarize(trades: pd.DataFrame, payout: float):
    if trades.empty:
        print("No trades were generated - either the data is too short, or the")
        print("strategy's threshold was never reached. Try more history, or use")
        print("--sweep-threshold to see how sensitive the count is to the cutoff.")
        return

    def block(df, title):
        n = len(df)
        wins = (df["outcome"] == "WIN").sum()
        loses = (df["outcome"] == "LOSE").sum()
        draws = (df["outcome"] == "DRAW").sum()
        decided = wins + loses
        win_rate = (wins / decided * 100) if decided else 0.0
        # Expectancy per trade, in units of stake, assuming DRAW refunds stake.
        expectancy = (wins * payout - loses * 1.0) / n if n else 0.0
        breakeven_wr = 1.0 / (1.0 + payout) * 100
        print(f"\n{title}")
        print(f"  Trades: {n}   Win: {wins}   Lose: {loses}   Draw: {draws}")
        print(f"  Win rate (decided only): {win_rate:.2f}%   "
              f"(breakeven at this payout: {breakeven_wr:.2f}%)")
        print(f"  Expectancy per trade: {expectancy:+.4f} units of stake "
              f"({'PROFITABLE' if expectancy > 0 else 'LOSING'} at payout={payout})")

    block(trades, "=== OVERALL ===")

    print("\n--- By quality tier ---")
    for q, g in trades.groupby("quality"):
        block(g, f"[{q}]")

    print("\n--- HTF-aligned vs not (does the 5M/15M bias bonus actually help?) ---")
    for aligned, g in trades.groupby("htf_aligned"):
        label = "ALIGNED with 5M+15M bias" if aligned else "NOT aligned / bias unavailable"
        block(g, f"[{label}]")

    if trades["symbol"].nunique() > 1:
        print("\n--- By symbol ---")
        for sym, g in trades.groupby("symbol"):
            block(g, f"[{sym}]")


def sweep_threshold(df_1m: pd.DataFrame, symbol_label: str, thresholds, payout: float):
    original = strategy.SIGNAL_THRESHOLD
    print("\n=== THRESHOLD SWEEP ===")
    print(f"{'Threshold':>10} | {'Trades':>7} | {'WinRate%':>9} | {'Expectancy':>11}")
    print("-" * 48)
    try:
        for t in thresholds:
            strategy.SIGNAL_THRESHOLD = t
            trades = run_backtest(df_1m, symbol_label)
            if trades.empty:
                print(f"{t:>10} | {0:>7} | {'n/a':>9} | {'n/a':>11}")
                continue
            wins = (trades["outcome"] == "WIN").sum()
            loses = (trades["outcome"] == "LOSE").sum()
            n = len(trades)
            decided = wins + loses
            win_rate = (wins / decided * 100) if decided else 0.0
            expectancy = (wins * payout - loses * 1.0) / n if n else 0.0
            print(f"{t:>10} | {n:>7} | {win_rate:>8.2f}% | {expectancy:>+10.4f}")
    finally:
        strategy.SIGNAL_THRESHOLD = original


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Walk-forward backtest for evaluate_strategy()")
    parser.add_argument("--data-dir", type=str, default=None,
                         help="Directory of *_1m.csv files (one per symbol, columns: time,open,high,low,close)")
    parser.add_argument("--demo", action="store_true",
                         help="Run on synthetic random-walk data instead of real files (sanity check only)")
    parser.add_argument("--payout", type=float, default=0.85,
                         help="Broker payout ratio on a win, e.g. 0.85 for 85%% (default: 0.85)")
    parser.add_argument("--sweep-threshold", type=int, nargs="+", default=None,
                         help="List of SIGNAL_THRESHOLD values to compare, e.g. --sweep-threshold 6 7 8 9")
    parser.add_argument("--out-csv", type=str, default=None,
                         help="Optional path to dump the full trade log as CSV")
    args = parser.parse_args()

    all_trades = []

    if args.demo:
        print(">>> DEMO MODE: synthetic random-walk data. Results below are NOT a real backtest. <<<")
        df = make_synthetic_demo_data()
        if args.sweep_threshold:
            sweep_threshold(df, "DEMO", args.sweep_threshold, args.payout)
            return
        all_trades.append(run_backtest(df, "DEMO"))

    elif args.data_dir:
        paths = sorted(glob.glob(os.path.join(args.data_dir, "*.csv")))
        if not paths:
            print(f"No CSV files found in {args.data_dir}")
            sys.exit(1)
        for path in paths:
            symbol_label = os.path.splitext(os.path.basename(path))[0]
            print(f"Loading {path} ...")
            df = load_ohlc_csv(path)
            if len(df) < 300:
                print(f"  skipped: only {len(df)} rows, need at least ~300 for a meaningful run")
                continue
            if args.sweep_threshold:
                sweep_threshold(df, symbol_label, args.sweep_threshold, args.payout)
                continue
            all_trades.append(run_backtest(df, symbol_label))

        if args.sweep_threshold:
            return
    else:
        parser.error("Pass either --data-dir <folder of CSVs> or --demo")

    trades = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    summarize(trades, args.payout)

    if args.out_csv and not trades.empty:
        trades.to_csv(args.out_csv, index=False)
        print(f"\nFull trade log written to {args.out_csv}")


if __name__ == "__main__":
    main()
