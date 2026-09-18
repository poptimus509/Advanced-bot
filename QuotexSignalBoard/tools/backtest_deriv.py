"""
backtest_deriv.py  (v2)
========================

Walk-forward backtest of the QuotexSignalBoard strategy (strategy.py) against
REAL historical candles from Deriv's public WebSocket API.

This is a DIAGNOSTIC tool, not a win-rate guarantee. It exists to let you
test specific hypotheses about the strategy against real data instead of
guessing. Every number it prints is something you should look at
skeptically - especially small subsets (few dozen trades = noisy).

What it reports
----------------
  1. Real win rate per threshold (5/6/7/8), full period + chronological
     70/30 train/test split (catches overfitting to one period)
  2. Per-component contribution (structure / EMA / RSI / candle reaction)
     - does each one actually correlate with a win, independent of the
       others?
  3. NEW: win rate broken down by ATR volatility tercile (Low/Mid/High) -
     tests whether trading only in higher-volatility regimes helps
  4. NEW: win rate broken down by hour-of-day (UTC) - tests whether some
     sessions (Asian / London / NY overlap) are meaningfully better
  5. NEW: multi-pair mode - run several symbols and see whether any
     effect (threshold, component, volatility, session) is consistent
     across pairs, or whether it's just noise on one pair's 6-8 weeks
     of data

No look-ahead: at candle i, the strategy only ever sees candles [0..i].
Trade simulation matches the bot's real 1-minute binary logic (entry =
close of signal candle, exit = close of the candle 60s later).

Setup
-----
    pip install pandas numpy websocket-client

Usage
-----
    # Single pair
    python backtest_deriv.py --bot-dir /path/to/QuotexSignalBoard \
        --symbol EURUSD --weeks 6

    # Multiple pairs aggregated together (slower - fetches each pair)
    python backtest_deriv.py --bot-dir /path/to/QuotexSignalBoard \
        --symbols EURUSD,GBPJPY,USDJPY,AUDUSD --weeks 6

    # Re-run instantly from cache after the first fetch
    python backtest_deriv.py --bot-dir /path/to/QuotexSignalBoard \
        --symbols EURUSD,GBPJPY,USDJPY,AUDUSD --weeks 6 --use-cache

--bot-dir must point at the folder containing strategy.py and config.py
(the actual files your bot runs), so the backtest scores candles with the
exact same code path as production.
"""

import argparse
import json
import os
import sys
import time
from typing import List, Dict, Optional

import numpy as np
import pandas as pd

try:
    import websocket  # websocket-client
except ImportError:
    print("Missing dependency. Run: pip install websocket-client")
    sys.exit(1)


# ------------------------------------------------------------------
# 1. Deriv historical candle fetch (paginated, synchronous)
# ------------------------------------------------------------------

DERIV_MAX_COUNT = 5000  # Deriv's per-request cap for ticks_history/candles


def fetch_deriv_candles(symbol: str, granularity: int, total_needed: int,
                         app_id: int = 1089) -> pd.DataFrame:
    clean = symbol.replace("frx", "").replace("/", "").upper()
    target_sym = f"frx{clean}"
    url = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"

    ws = websocket.create_connection(url, timeout=15)
    collected: List[dict] = []
    end_param = "latest"
    req_id = 0

    try:
        while len(collected) < total_needed:
            remaining = total_needed - len(collected)
            count = min(DERIV_MAX_COUNT, remaining)
            req_id += 1
            req = {
                "ticks_history": target_sym,
                "adjust_start_time": 1,
                "count": count,
                "end": end_param,
                "granularity": granularity,
                "style": "candles",
                "req_id": req_id,
            }
            ws.send(json.dumps(req))
            raw = ws.recv()
            data = json.loads(raw)

            if data.get("msg_type") == "error":
                msg = data.get("error", {}).get("message", "unknown error")
                print(f"  Deriv API error: {msg}")
                break

            candles = data.get("candles", [])
            if not candles:
                print("  No more candles returned - reached start of available history.")
                break

            batch = [
                {
                    "time": int(c.get("epoch") or c.get("time") or 0),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                }
                for c in candles
            ]
            batch.sort(key=lambda r: r["time"])
            collected = batch + collected
            end_param = batch[0]["time"] - 1

            print(f"  [{symbol}] fetched {len(batch)} candles (total so far: {len(collected)})")
            time.sleep(0.3)  # be polite to the API
    finally:
        ws.close()

    df = pd.DataFrame(collected).drop_duplicates(subset="time").sort_values("time")
    return df.reset_index(drop=True)


def load_or_fetch(symbol: str, granularity: int, total_needed: int,
                   cache_dir: str, use_cache: bool, app_id: int) -> pd.DataFrame:
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"{symbol}_{granularity}s.csv")

    if use_cache and os.path.exists(cache_path):
        df = pd.read_csv(cache_path)
        if len(df) >= total_needed * 0.9:
            print(f"Loaded {len(df)} cached candles from {cache_path}")
            return df

    print(f"Fetching {total_needed} candles for {symbol} @ {granularity}s granularity from Deriv...")
    df = fetch_deriv_candles(symbol, granularity, total_needed, app_id)
    df.to_csv(cache_path, index=False)
    print(f"Saved {len(df)} candles to {cache_path}")
    return df


# ------------------------------------------------------------------
# 2. Walk-forward replay using the ACTUAL strategy.py
# ------------------------------------------------------------------

def build_5m_view(df_5m: pd.DataFrame, up_to_time: int, window: int) -> Optional[pd.DataFrame]:
    if df_5m is None or df_5m.empty:
        return None
    view = df_5m[df_5m["time"] <= up_to_time].tail(window)
    return view.reset_index(drop=True) if len(view) else None


def component_flags(strategy, cfg, prepared: pd.DataFrame) -> Dict[str, Optional[str]]:
    """
    Re-derives which side (CALL/PUT/None) each of the four independent
    scoring components pointed to for the latest candle, by calling the
    SAME private helpers strategy.py's evaluate_strategy() uses. Never
    reimplements the logic - just calls the real functions.
    """
    last_row = prepared.iloc[-1]
    prev_row = prepared.iloc[-2]

    swings = strategy.confirmed_swings(prepared)
    structure = strategy._structure_from_swings(swings)
    struct_side = "CALL" if structure == "HH_HL" else ("PUT" if structure == "LH_LL" else None)

    close = float(last_row["close"])
    ema9, ema21, ema50 = float(last_row["ema_9"]), float(last_row["ema_21"]), float(last_row["ema_50"])
    if close > ema9 > ema21 > ema50:
        ema_side = "CALL"
    elif close < ema9 < ema21 < ema50:
        ema_side = "PUT"
    else:
        ema_side = None

    rsi = float(last_row["rsi"])
    rsi_call_min = getattr(cfg, "RSI_CALL_MIN", 55.0)
    rsi_call_max = getattr(cfg, "RSI_CALL_MAX", 70.0)
    rsi_put_min = getattr(cfg, "RSI_PUT_MIN", 30.0)
    rsi_put_max = getattr(cfg, "RSI_PUT_MAX", 45.0)
    if rsi_call_min < rsi < rsi_call_max:
        rsi_side = "CALL"
    elif rsi_put_min < rsi < rsi_put_max:
        rsi_side = "PUT"
    else:
        rsi_side = None

    bull = strategy.candle_reaction(last_row, prev_row, True)
    bear = strategy.candle_reaction(last_row, prev_row, False)
    if bull and bull["type"] == "BULLISH_REACTION":
        candle_side = "CALL"
    elif bear and bear["type"] == "BEARISH_REACTION":
        candle_side = "PUT"
    else:
        candle_side = None

    return {"structure": struct_side, "ema": ema_side, "rsi": rsi_side, "candle": candle_side}


def run_walkforward(strategy, cfg, symbol: str, df_1m: pd.DataFrame, df_5m: Optional[pd.DataFrame],
                     window_1m: int = 300, window_5m: int = 60) -> pd.DataFrame:
    original_threshold = getattr(cfg, "SIGNAL_THRESHOLD_CALL_PUT", 6)
    cfg.SIGNAL_THRESHOLD_CALL_PUT = 0  # force-score everything; filter by threshold afterward

    records = []
    n = len(df_1m)
    min_rows = getattr(strategy, "MIN_ROWS", 15)

    for i in range(min_rows, n - 1):  # -1: need candle i+1 to know the outcome
        lo = max(0, i - window_1m + 1)
        window_df = df_1m.iloc[lo:i + 1].reset_index(drop=True)

        cur_time = int(df_1m.iloc[i]["time"])
        view_5m = build_5m_view(df_5m, cur_time, window_5m)

        direction, score, quality, details = strategy.evaluate_strategy(window_df, view_5m, None)

        entry_price = float(df_1m.iloc[i]["close"])
        exit_price = float(df_1m.iloc[i + 1]["close"])
        diff = exit_price - entry_price
        if direction == "PUT":
            diff = -diff

        if direction in ("CALL", "PUT"):
            outcome = "WIN" if diff > 0 else ("LOSS" if diff < 0 else "TIE")
        else:
            outcome = None

        comps = {"structure": None, "ema": None, "rsi": None, "candle": None}
        prepared = strategy.prepare_history(window_df)
        if prepared is not None and len(prepared) >= min_rows:
            comps = component_flags(strategy, cfg, prepared)

        call_score = details.get("call_score", 0)
        put_score = details.get("put_score", 0)

        records.append({
            "symbol": symbol,
            "time": cur_time,
            "hour_utc": time.gmtime(cur_time).tm_hour,
            "direction": direction,
            "call_score": call_score,
            "put_score": put_score,
            "final_score": max(call_score, put_score),
            "atr": details.get("atr", np.nan),
            "gated": direction == "NO_TRADE" and max(call_score, put_score) > 0 and call_score != put_score,
            "outcome": outcome,
            "structure_agrees": comps["structure"] == direction if direction in ("CALL", "PUT") else None,
            "ema_agrees": comps["ema"] == direction if direction in ("CALL", "PUT") else None,
            "rsi_agrees": comps["rsi"] == direction if direction in ("CALL", "PUT") else None,
            "candle_agrees": comps["candle"] == direction if direction in ("CALL", "PUT") else None,
        })

        if (i - min_rows) % 2000 == 0:
            print(f"  [{symbol}] replayed {i - min_rows}/{n - min_rows - 1} candles...")

    cfg.SIGNAL_THRESHOLD_CALL_PUT = original_threshold

    df = pd.DataFrame(records)
    if len(df):
        # ATR volatility tercile, computed PER SYMBOL so pairs with
        # structurally different price scales (e.g. JPY pairs) don't
        # distort each other's buckets.
        df["atr_tercile"] = df.groupby("symbol")["atr"].transform(
            lambda s: pd.qcut(s.rank(method="first"), 3, labels=["Low", "Mid", "High"])
        )
    return df


# ------------------------------------------------------------------
# 3. Analysis
# ------------------------------------------------------------------

def win_rate(df: pd.DataFrame) -> Dict[str, float]:
    wins = (df["outcome"] == "WIN").sum()
    losses = (df["outcome"] == "LOSS").sum()
    ties = (df["outcome"] == "TIE").sum()
    total_decided = wins + losses
    return {
        "trades": int(wins + losses + ties),
        "wins": int(wins),
        "losses": int(losses),
        "ties": int(ties),
        "win_rate_pct": round(100 * wins / total_decided, 2) if total_decided else float("nan"),
    }


def analyze(records: pd.DataFrame, thresholds: List[int], train_frac: float = 0.7,
            min_bucket_trades: int = 15):
    records = records.sort_values("time").reset_index(drop=True)
    split_idx = int(len(records) * train_frac)
    train, test = records.iloc[:split_idx], records.iloc[split_idx:]

    print("\n" + "=" * 74)
    print("1) PER-THRESHOLD REAL WIN RATE (full period / train / test)")
    print("=" * 74)
    print(f"{'Thr':<5}{'Full trades':<13}{'Full WR%':<10}{'Train WR%':<11}{'Test WR%':<10}")
    for t in thresholds:
        full_sub = records[(records["final_score"] >= t) & (records["direction"].isin(["CALL", "PUT"]))]
        train_sub = train[(train["final_score"] >= t) & (train["direction"].isin(["CALL", "PUT"]))]
        test_sub = test[(test["final_score"] >= t) & (test["direction"].isin(["CALL", "PUT"]))]
        full_wr, train_wr, test_wr = win_rate(full_sub), win_rate(train_sub), win_rate(test_sub)
        print(f"{t:<5}{full_wr['trades']:<13}{full_wr['win_rate_pct']:<10}"
              f"{train_wr['win_rate_pct']:<11}{test_wr['win_rate_pct']:<10}")

    baseline_t = min(thresholds)
    base = records[(records["final_score"] >= baseline_t) & (records["direction"].isin(["CALL", "PUT"]))]
    gated_out = records[(records["gated"]) & (records["final_score"] >= baseline_t)]
    print(f"\nSignals blocked by the 5M/ADX gate at score>={baseline_t}: {len(gated_out)}")

    print("\n" + "=" * 74)
    print(f"2) PER-COMPONENT CONTRIBUTION (signals with final_score >= {baseline_t})")
    print("=" * 74)
    for comp in ["structure_agrees", "ema_agrees", "rsi_agrees", "candle_agrees"]:
        present = base[base[comp] == True]
        absent = base[base[comp] == False]
        wr_present, wr_absent = win_rate(present), win_rate(absent)
        print(f"{comp:<18} present: n={wr_present['trades']:<5} WR={wr_present['win_rate_pct']:<8}"
              f" | absent: n={wr_absent['trades']:<5} WR={wr_absent['win_rate_pct']}")

    # How correlated are structure and EMA stack? (both are trend-following;
    # if they almost always agree, they aren't adding independent evidence)
    both = base.dropna(subset=["structure_agrees", "ema_agrees"])
    if len(both):
        agree_rate = (both["structure_agrees"] == both["ema_agrees"]).mean() * 100
        print(f"\nstructure_agrees == ema_agrees on {agree_rate:.1f}% of signals "
              f"(high % suggests the two components are duplicating trend info,\n"
              f"not adding independent confluence)")

    print("\n" + "=" * 74)
    print(f"3) VOLATILITY (ATR tercile, per-symbol) at final_score >= {baseline_t}")
    print("=" * 74)
    for bucket in ["Low", "Mid", "High"]:
        sub = base[base["atr_tercile"] == bucket]
        wr = win_rate(sub)
        flag = "" if wr["trades"] >= min_bucket_trades else "  (few trades - noisy)"
        print(f"{bucket:<6} n={wr['trades']:<6} WR={wr['win_rate_pct']}{flag}")

    print("\n" + "=" * 74)
    print(f"4) HOUR OF DAY (UTC) at final_score >= {baseline_t}")
    print("=" * 74)
    print(f"{'Hour':<6}{'Trades':<9}{'WR%':<8}{'Note'}")
    hourly = []
    for h in range(24):
        sub = base[base["hour_utc"] == h]
        wr = win_rate(sub)
        hourly.append((h, wr["trades"], wr["win_rate_pct"]))
        note = "" if wr["trades"] >= min_bucket_trades else "few trades"
        print(f"{h:<6}{wr['trades']:<9}{wr['win_rate_pct']:<8}{note}")
    valid_hours = [(h, t, w) for h, t, w in hourly if t >= min_bucket_trades and not np.isnan(w)]
    if valid_hours:
        best = max(valid_hours, key=lambda r: r[2])
        worst = min(valid_hours, key=lambda r: r[2])
        print(f"\nBest hour (with >= {min_bucket_trades} trades): {best[0]:02d}:00 UTC -> {best[2]}% (n={best[1]})")
        print(f"Worst hour (with >= {min_bucket_trades} trades): {worst[0]:02d}:00 UTC -> {worst[2]}% (n={worst[1]})")

    if records["symbol"].nunique() > 1:
        print("\n" + "=" * 74)
        print(f"5) PER-SYMBOL BREAKDOWN at final_score >= {baseline_t}")
        print("=" * 74)
        print(f"{'Symbol':<10}{'Trades':<9}{'WR%'}")
        for sym, sub in base.groupby("symbol"):
            wr = win_rate(sub)
            print(f"{sym:<10}{wr['trades']:<9}{wr['win_rate_pct']}")
        print("\nIf win rate swings wildly between pairs with similar trade counts,\n"
              "treat any single-pair 'good' result with real skepticism.")

    return train, test


# ------------------------------------------------------------------
# 4. Entry point
# ------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Backtest QuotexSignalBoard strategy on real Deriv data")
    ap.add_argument("--bot-dir", required=True, help="Path to the folder containing strategy.py and config.py")
    ap.add_argument("--symbol", default=None, help="Single forex pair, e.g. EURUSD")
    ap.add_argument("--symbols", default=None, help="Comma-separated pairs for aggregated multi-pair mode, e.g. EURUSD,GBPJPY,USDJPY")
    ap.add_argument("--weeks", type=int, default=6, help="Weeks of 1M history per symbol")
    ap.add_argument("--app-id", type=int, default=1089)
    ap.add_argument("--cache-dir", default="./deriv_cache")
    ap.add_argument("--use-cache", action="store_true")
    ap.add_argument("--thresholds", default="5,6,7,8")
    ap.add_argument("--train-frac", type=float, default=0.7)
    ap.add_argument("--min-bucket-trades", type=int, default=15,
                     help="Minimum trades before an ATR/hour bucket is treated as meaningful, not noise")
    ap.add_argument("--output-csv", default=None, help="Optional path to dump the raw per-candle records")
    args = ap.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols \
        else [(args.symbol or "EURUSD").upper()]

    sys.path.insert(0, os.path.abspath(args.bot_dir))
    import strategy  # noqa: E402  (the real, unmodified strategy.py)
    import config as cfg  # noqa: E402

    minutes_needed = args.weeks * 7 * 24 * 60
    fivemin_needed = args.weeks * 7 * 24 * 12

    all_records = []
    for symbol in symbols:
        print(f"\n--- {symbol} ---")
        df_1m = load_or_fetch(symbol, 60, minutes_needed, args.cache_dir, args.use_cache, args.app_id)
        df_5m = load_or_fetch(symbol, 300, fivemin_needed, args.cache_dir, args.use_cache, args.app_id)

        if len(df_1m) < strategy.MIN_ROWS + 10:
            print(f"  Not enough 1M data for {symbol} - skipping.")
            continue

        print(f"  Replaying {len(df_1m)} 1M candles chronologically (no look-ahead)...")
        rec = run_walkforward(strategy, cfg, symbol, df_1m, df_5m)
        all_records.append(rec)

    if not all_records:
        print("No data collected - aborting.")
        return

    records = pd.concat(all_records, ignore_index=True)
    thresholds = [int(t) for t in args.thresholds.split(",")]
    analyze(records, thresholds, args.train_frac, args.min_bucket_trades)

    if args.output_csv:
        records.to_csv(args.output_csv, index=False)
        print(f"\nRaw per-candle records saved to {args.output_csv}")


if __name__ == "__main__":
    main()
