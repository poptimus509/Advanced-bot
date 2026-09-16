"""
strategy.py
-----------
Core signal-evaluation engine.

Design (revised):
  * 1M candles  -> ENTRY TIMING (RSI / EMA stack / candle confirmation / local structure).
  * 5M candles  -> real HTF bias (fractal swing HH/HL vs LH/LL), used as a SOFT bonus,
                   not a hard veto (a 1M reversal near an S/R zone can still fire).
  * 15M candles -> same treatment as 5M, one tier higher.
  * Support/Resistance zones are built from the *same* confirmed swing points used for
    the HTF bias and are used to (a) penalise chasing a move straight into a wall and
    (b) avoid rewarding a spike that hasn't actually broken structure.
  * A body-to-range filter and an ATR-extension filter suppress "chase the spike"
    entries that look strong on a single noisy 1M candle but are not real continuation.

Every public function here is intentionally pure (no I/O, no globals mutated) so it can
be unit tested in isolation - see tests/test_strategy.py.
"""

import math
import logging
from typing import Tuple, Dict, Any, Optional, List
import pandas as pd
import numpy as np

try:
    import config as cfg
except Exception:  # pragma: no cover - keeps strategy importable stand-alone
    cfg = None

logger = logging.getLogger("QuotexSignalBoard.Strategy")

# ---------------------------------------------------------------------------
# Tunable constants (pulled from config.py where the semantics genuinely match;
# left as local, documented defaults where the old config value didn't match
# the formula it would now be plugged into).
# ---------------------------------------------------------------------------
MIN_ROWS = 15
MIN_MATURE_HISTORY = getattr(cfg, "MIN_1M_HISTORY", 120) if cfg else 120
SIGNAL_THRESHOLD = getattr(cfg, "SIGNAL_THRESHOLD_CALL_PUT", 8) if cfg else 8
ZONE_WIDTH_ATR = getattr(cfg, "ZONE_WIDTH_ATR", 0.25) if cfg else 0.25
MIN_CLEARANCE_ATR = getattr(cfg, "MIN_CLEARANCE_ATR", 0.75) if cfg else 0.75
# NOTE: config.MAX_ENTRY_DRIFT_ATR (0.25) was written for a different, never-built
# formula and is far too tight to reuse here (it would fire on almost every candle).
# ENTRY_EXTENSION_ATR_LIMIT below is a new, explicitly-tuned threshold instead of
# silently mis-wiring that old constant.
ENTRY_EXTENSION_ATR_LIMIT = 1.2
HTF_LOOKBACK = 100
SWING_LEFT = 2
SWING_RIGHT = 2
ACTIVITY_BASELINE_WINDOW = getattr(cfg, "ACTIVITY_BASELINE_CANDLES", 20) if cfg else 20


def _empty_details(reason: str = "INSUFFICIENT_DATA") -> Dict[str, Any]:
    return {
        "score_reason": reason,
        "structure": "MIXED",
        "trend_5m": "UNAVAILABLE",
        "trend_15m": "UNAVAILABLE",
        "sr_context": "UNAVAILABLE",
        "call_score": 0,
        "put_score": 0,
        "atr": 0.0001,
        "setup_id": "NONE",
        "rank_strength": 0,
        "pa": "NEUTRAL",
        "activity_status": "UNAVAILABLE",
        "relative_activity": None,
        "signal_candle_epoch": 0,
    }


# ---------------------------------------------------------------------------
# History preparation
# ---------------------------------------------------------------------------
def prepare_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardises a candle dataframe and calculates EMA / RSI / ATR.

    Also defends against a stale-cache scenario: if the "time" column has a gap
    (a break in the expected fixed candle interval), only the most recent
    contiguous run of candles is kept. Evaluating indicators across a gap would
    silently blend two unrelated time windows together.
    """
    if df is None or df.empty or len(df) < MIN_ROWS:
        return df if df is not None else pd.DataFrame()

    df = df.copy()

    if "time" in df.columns:
        df = df.sort_values("time").reset_index(drop=True)
        diffs = df["time"].diff()
        non_null_diffs = diffs.dropna()
        if len(non_null_diffs) > 0:
            mode_series = non_null_diffs.mode()
            step = mode_series.iloc[0] if not mode_series.empty else non_null_diffs.median()
            if step and step > 0:
                gap_positions = non_null_diffs[non_null_diffs != step].index
                if len(gap_positions) > 0:
                    last_gap_idx = int(gap_positions.max())
                    df = df.loc[last_gap_idx:].reset_index(drop=True)

    if len(df) < MIN_ROWS:
        return df

    for col in ["open", "high", "low", "close"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()
    df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()

    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0.0)).rolling(window=14, min_periods=14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=14, min_periods=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))
    df["rsi"] = df["rsi"].fillna(50.0)

    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["atr"] = tr.rolling(window=14, min_periods=1).mean()

    return df


def _validate_last_candle(row) -> bool:
    try:
        o = float(row["open"]); h = float(row["high"])
        l = float(row["low"]); c = float(row["close"])
    except Exception:
        return False
    if not all(math.isfinite(v) and v > 0 for v in (o, h, l, c)):
        return False
    if h < l:
        return False
    if h < max(o, c) or l > min(o, c):
        return False
    return True


# ---------------------------------------------------------------------------
# Fractal swing detection (causal - never rewritten by future bars)
# ---------------------------------------------------------------------------
def confirmed_swings(df: pd.DataFrame, left: int = SWING_LEFT, right: int = SWING_RIGHT) -> List[Dict[str, Any]]:
    """
    Detects confirmed fractal swing highs/lows.

    A bar at index i is a confirmed swing high once `right` bars have closed
    after it and it is the highest high in the [i-left, i+right] window (first
    occurrence wins on ties). Because each pivot only ever looks at a fixed,
    local window, a pivot confirmed at position `confirmed_index` can never be
    changed by data that arrives afterwards - this makes the sequence safe to
    use for a live, streaming bias calculation.
    """
    if df is None or df.empty or len(df) < left + right + 1:
        return []
    if "high" not in df.columns or "low" not in df.columns:
        return []

    highs = df["high"].to_numpy(dtype=float)
    lows = df["low"].to_numpy(dtype=float)
    times = df["time"].to_numpy() if "time" in df.columns else np.arange(len(df))

    n = len(df)
    swings: List[Dict[str, Any]] = []
    for i in range(left, n - right):
        window_high = highs[i - left:i + right + 1]
        window_low = lows[i - left:i + right + 1]

        if np.argmax(window_high) == left:
            swings.append({
                "type": "HIGH",
                "index": i,
                "confirmed_index": i + right,
                "price": float(highs[i]),
                "epoch": int(times[i]),
            })
        if np.argmin(window_low) == left:
            swings.append({
                "type": "LOW",
                "index": i,
                "confirmed_index": i + right,
                "price": float(lows[i]),
                "epoch": int(times[i]),
            })

    swings.sort(key=lambda s: s["confirmed_index"])
    return swings


def swings_to_bias(swings: List[Dict[str, Any]]) -> str:
    """HH+HL -> BULLISH, LH+LL -> BEARISH, otherwise NEUTRAL (needs >=2 of each)."""
    highs = [s for s in swings if s["type"] == "HIGH"]
    lows = [s for s in swings if s["type"] == "LOW"]
    if len(highs) < 2 or len(lows) < 2:
        return "NEUTRAL"

    higher_high = highs[-1]["price"] > highs[-2]["price"]
    higher_low = lows[-1]["price"] > lows[-2]["price"]
    lower_high = highs[-1]["price"] < highs[-2]["price"]
    lower_low = lows[-1]["price"] < lows[-2]["price"]

    if higher_high and higher_low:
        return "BULLISH"
    if lower_high and lower_low:
        return "BEARISH"
    return "NEUTRAL"


def build_sr_zones(swings: List[Dict[str, Any]], atr: float, zone_width_atr: float = ZONE_WIDTH_ATR) -> List[Dict[str, Any]]:
    """Clusters nearby swing points (within zone_width_atr * ATR) into S/R zones."""
    if not swings or not atr or atr <= 0:
        return []
    width = atr * zone_width_atr
    zones: List[Dict[str, Any]] = []
    for s in swings:
        placed = False
        for z in zones:
            if z["type"] == s["type"] and abs(z["price"] - s["price"]) <= width:
                z["touches"] += 1
                z["price"] = (z["price"] * (z["touches"] - 1) + s["price"]) / z["touches"]
                placed = True
                break
        if not placed:
            zones.append({"type": s["type"], "price": s["price"], "touches": 1})
    return zones


def nearest_zone_distance_atr(price: float, zones: List[Dict[str, Any]], atr: float, kind: str) -> Optional[float]:
    """
    Distance (in ATR units) to the nearest *relevant* zone ahead of price:
    kind="HIGH" -> resistance zones at/above price (blocks CALL continuation).
    kind="LOW"  -> support zones at/below price (blocks PUT continuation).
    """
    if not zones or not atr or atr <= 0:
        return None
    if kind == "HIGH":
        candidates = [z for z in zones if z["type"] == "HIGH" and z["price"] >= price]
    else:
        candidates = [z for z in zones if z["type"] == "LOW" and z["price"] <= price]
    if not candidates:
        return None
    nearest = min(candidates, key=lambda z: abs(z["price"] - price))
    return abs(nearest["price"] - price) / atr


def evaluate_htf_context(df_htf: Optional[pd.DataFrame], lookback: int = HTF_LOOKBACK) -> Optional[Dict[str, Any]]:
    """
    Builds a higher-timeframe bias + S/R context from a *real* 5M or 15M candle
    dataframe. Returns None (treated identically to "unavailable") if the input
    isn't a usable dataframe - this keeps the caller robust against a malformed
    or missing HTF feed instead of crashing or silently faking a bias.
    """
    if df_htf is None or not isinstance(df_htf, pd.DataFrame) or df_htf.empty:
        return None

    required_cols = {"time", "open", "high", "low", "close"}
    if not required_cols.issubset(set(df_htf.columns)):
        return None

    windowed = df_htf.tail(lookback).reset_index(drop=True)
    if len(windowed) < MIN_ROWS:
        return None

    prepared = prepare_history(windowed)
    if prepared is None or prepared.empty or len(prepared) < MIN_ROWS:
        return None

    swings = confirmed_swings(prepared, SWING_LEFT, SWING_RIGHT)
    bias = swings_to_bias(swings)

    atr = float(prepared.iloc[-1].get("atr", 0.0001))
    if not math.isfinite(atr) or atr <= 0:
        atr = 0.0001

    zones = build_sr_zones(swings, atr)
    return {"bias": bias, "zones": zones, "atr": atr, "swings_count": len(swings)}


# ---------------------------------------------------------------------------
# 1-minute local structure (same fractal logic, short lookback)
# ---------------------------------------------------------------------------
def detect_market_structure(df: pd.DataFrame, lookback: int = 30) -> Tuple[str, float, float]:
    if df is None or len(df) < 10:
        return "MIXED", 0.0, 0.0
    window = df.tail(lookback).reset_index(drop=True)
    swings = confirmed_swings(window, SWING_LEFT, SWING_RIGHT)
    bias = swings_to_bias(swings)
    swing_high = float(window["high"].max())
    swing_low = float(window["low"].min())
    if bias == "BULLISH":
        return "HH_HL", swing_high, swing_low
    if bias == "BEARISH":
        return "LH_LL", swing_high, swing_low
    return "MIXED", swing_high, swing_low


# ---------------------------------------------------------------------------
# Candle reaction helper (used both by the scorer and available standalone)
# ---------------------------------------------------------------------------
def candle_reaction(current, previous, want_bullish: bool) -> Optional[Dict[str, Any]]:
    """
    Confirms whether `current` is a valid reaction candle in the requested
    direction. Returns None if the candle's own close/open direction
    contradicts what's being asked for - a bearish candle can never confirm a
    bullish entry, and vice versa, regardless of its wicks.
    """
    try:
        c_open = float(current["open"]); c_close = float(current["close"])
        c_high = float(current["high"]); c_low = float(current["low"])
    except Exception:
        return None

    is_bullish_candle = c_close > c_open
    is_bearish_candle = c_close < c_open

    if want_bullish and not is_bullish_candle:
        return None
    if (not want_bullish) and not is_bearish_candle:
        return None

    rng = c_high - c_low
    if rng <= 0:
        return None

    body = abs(c_close - c_open)
    upper_wick = c_high - max(c_open, c_close)
    lower_wick = min(c_open, c_close) - c_low
    body_ratio = body / rng

    rejection = (lower_wick > body) if want_bullish else (upper_wick > body)

    return {
        "body_ratio": round(body_ratio, 3),
        "rejection": bool(rejection),
        "range": rng,
        "body": body,
    }


# ---------------------------------------------------------------------------
# Live-tick activity proxy (NOT the broker's historical candle tick count,
# which is not a reliable proxy for real market activity - see CORRECTIONS).
# ---------------------------------------------------------------------------
def activity_pressure(df: pd.DataFrame, baseline_window: int = ACTIVITY_BASELINE_WINDOW) -> Dict[str, Any]:
    result_unavailable = {"activity_status": "UNAVAILABLE", "pressure": None, "relative_activity": None}

    if df is None or df.empty or "verified_ticks" not in df.columns:
        return result_unavailable

    vt = df["verified_ticks"]
    if vt.isna().all():
        return result_unavailable

    last_val = vt.iloc[-1]
    if pd.isna(last_val):
        return result_unavailable

    history = vt.iloc[:-1].dropna()
    if history.empty:
        return result_unavailable

    baseline = history.tail(baseline_window).mean()
    if not baseline or baseline <= 0:
        return result_unavailable

    relative = float(last_val) / float(baseline)
    if relative >= 1.5:
        pressure = "HIGH"
    elif relative <= 0.5:
        pressure = "LOW"
    else:
        pressure = "NORMAL"

    return {
        "activity_status": "LIVE_TICK_PROXY",
        "pressure": pressure,
        "relative_activity": round(relative, 2),
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def evaluate_strategy(
    df: pd.DataFrame,
    htf_5m: Optional[pd.DataFrame] = None,
    htf_15m: Optional[pd.DataFrame] = None,
) -> Tuple[str, int, str, Dict[str, Any]]:
    """
    Evaluates the 1M entry candle, gated/boosted by real 5M and 15M bias plus
    S/R context, and returns (direction, score, quality, details).

    direction: "CALL" | "PUT" | "NO_TRADE"
    """
    if df is None or df.empty or len(df) < MIN_ROWS:
        return "NO_TRADE", 0, "WAIT", _empty_details()

    prepared = prepare_history(df)
    if prepared is None or prepared.empty or len(prepared) < MIN_ROWS:
        return "NO_TRADE", 0, "WAIT", _empty_details()

    last_row = prepared.iloc[-1]

    if not _validate_last_candle(last_row):
        return "NO_TRADE", 0, "WAIT", _empty_details("INVALID_CANDLE")

    signal_epoch = int(last_row["time"]) if "time" in prepared.columns else 0
    insufficient_history = len(prepared) < MIN_MATURE_HISTORY

    curr_close = float(last_row["close"])
    curr_open = float(last_row["open"])
    curr_high = float(last_row["high"])
    curr_low = float(last_row["low"])
    curr_rsi = float(last_row.get("rsi", 50.0))
    ema_9 = float(last_row.get("ema_9", curr_close))
    ema_21 = float(last_row.get("ema_21", curr_close))
    atr = float(last_row.get("atr", 0.0001))
    if not math.isfinite(atr) or atr <= 0:
        atr = 0.0001

    structure, _swing_high, _swing_low = detect_market_structure(prepared)

    call_score = 0
    put_score = 0
    call_reasons: List[str] = []
    put_reasons: List[str] = []

    # 1) Local (1M) structure - up to +2
    if structure == "HH_HL":
        call_score += 2
        call_reasons.append("M1_STRUCTURE_BULLISH")
    elif structure == "LH_LL":
        put_score += 2
        put_reasons.append("M1_STRUCTURE_BEARISH")

    # 2) EMA9/21 stack - up to +2
    if curr_close > ema_9 > ema_21:
        call_score += 2
        call_reasons.append("EMA_BULLISH_STACK")
    elif curr_close < ema_9 < ema_21:
        put_score += 2
        put_reasons.append("EMA_BEARISH_STACK")
    elif curr_close > ema_9:
        call_score += 1
    elif curr_close < ema_9:
        put_score += 1

    # 3) RSI momentum - up to +2
    if 55 < curr_rsi < 75:
        call_score += 2
        call_reasons.append("RSI_BULLISH_MOMENTUM")
    elif 25 < curr_rsi < 45:
        put_score += 2
        put_reasons.append("RSI_BEARISH_MOMENTUM")
    elif curr_rsi <= 25:
        call_score += 1
        call_reasons.append("RSI_OVERSOLD")
    elif curr_rsi >= 75:
        put_score += 1
        put_reasons.append("RSI_OVERBOUGHT")

    # 4) Candle body / wick confirmation - up to +2, with a spike/noise filter:
    #    a candle whose body is a small fraction of its range earns nothing,
    #    instead of blindly rewarding a thin, noisy body in the "right" direction.
    candle_body = abs(curr_close - curr_open)
    total_range = curr_high - curr_low
    body_ratio = (candle_body / total_range) if total_range > 0 else 0.0
    upper_wick = curr_high - max(curr_close, curr_open)
    lower_wick = min(curr_close, curr_open) - curr_low
    is_bullish_candle = curr_close > curr_open
    is_bearish_candle = curr_close < curr_open

    if is_bullish_candle:
        if lower_wick > candle_body:
            call_score += 2
            call_reasons.append("BULLISH_REJECTION_WICK")
        elif body_ratio >= 0.5:
            call_score += 1
            call_reasons.append("BULLISH_BODY_CONFIRMED")
    elif is_bearish_candle:
        if upper_wick > candle_body:
            put_score += 2
            put_reasons.append("BEARISH_REJECTION_WICK")
        elif body_ratio >= 0.5:
            put_score += 1
            put_reasons.append("BEARISH_BODY_CONFIRMED")

    # -------------------------------------------------------------
    # Real HTF bias (5M / 15M) - soft bonus/penalty, not a hard veto.
    # -------------------------------------------------------------
    htf5_ctx = evaluate_htf_context(htf_5m)
    htf15_ctx = evaluate_htf_context(htf_15m)
    trend_5m = htf5_ctx["bias"] if htf5_ctx else "UNAVAILABLE"
    trend_15m = htf15_ctx["bias"] if htf15_ctx else "UNAVAILABLE"

    provisional_direction = "CALL" if call_score >= put_score else "PUT"

    htf_bonus = 0
    if provisional_direction == "CALL":
        if trend_5m == "BULLISH":
            htf_bonus += 1
            call_reasons.append("M5_ALIGNED")
        elif trend_5m == "BEARISH":
            htf_bonus -= 1
        if trend_15m == "BULLISH":
            htf_bonus += 1
            call_reasons.append("M15_ALIGNED")
        elif trend_15m == "BEARISH":
            htf_bonus -= 1
        call_score += htf_bonus
    else:
        if trend_5m == "BEARISH":
            htf_bonus += 1
            put_reasons.append("M5_ALIGNED")
        elif trend_5m == "BULLISH":
            htf_bonus -= 1
        if trend_15m == "BEARISH":
            htf_bonus += 1
            put_reasons.append("M15_ALIGNED")
        elif trend_15m == "BULLISH":
            htf_bonus -= 1
        put_score += htf_bonus

    # -------------------------------------------------------------
    # ATR-extension filter: don't chase a candle that has already run far
    # away from its own short EMA relative to current volatility - this is
    # the "sudden spike that's about to mean-revert" case.
    # -------------------------------------------------------------
    extension_atr = abs(curr_close - ema_9) / atr if atr > 0 else 0.0
    if extension_atr > ENTRY_EXTENSION_ATR_LIMIT:
        if provisional_direction == "CALL":
            call_score = max(0, call_score - 2)
        else:
            put_score = max(0, put_score - 2)

    # -------------------------------------------------------------
    # Support/Resistance proximity: penalise chasing a continuation straight
    # into a higher-timeframe wall.
    # -------------------------------------------------------------
    sr_context = "UNAVAILABLE"
    zones: List[Dict[str, Any]] = []
    if htf5_ctx:
        zones += htf5_ctx.get("zones", [])
    if htf15_ctx:
        zones += htf15_ctx.get("zones", [])
    ref_atr = htf5_ctx["atr"] if htf5_ctx else (htf15_ctx["atr"] if htf15_ctx else atr)

    if zones:
        kind = "HIGH" if provisional_direction == "CALL" else "LOW"
        dist = nearest_zone_distance_atr(curr_close, zones, ref_atr, kind)
        if dist is not None:
            if dist < MIN_CLEARANCE_ATR:
                sr_context = "NEAR_OPPOSING_ZONE"
                if provisional_direction == "CALL":
                    call_score = max(0, call_score - 2)
                else:
                    put_score = max(0, put_score - 2)
            else:
                sr_context = "CLEAR"

    call_score = min(call_score, 10)
    put_score = min(put_score, 10)

    if call_score >= put_score:
        final_score = call_score
        direction_candidate = "CALL"
        reasons = call_reasons
    else:
        final_score = put_score
        direction_candidate = "PUT"
        reasons = put_reasons

    if final_score >= 8:
        quality = "A_PLUS"
    elif final_score >= 6:
        quality = "STANDARD"
    elif final_score >= 4:
        quality = "MODERATE"
    else:
        quality = "WAIT"

    if insufficient_history:
        direction = "NO_TRADE"
        score_reason = "INSUFFICIENT_1M_HISTORY"
    elif final_score >= SIGNAL_THRESHOLD:
        direction = direction_candidate
        score_reason = "+".join(reasons[-4:]) if reasons else "SETUP_CONFIRMED"
    else:
        direction = "NO_TRADE"
        score_reason = "+".join(reasons[:2]) if reasons else "WAITING_SETUP"

    activity = activity_pressure(prepared)

    details = {
        "score_reason": score_reason,
        "structure": structure,
        "trend_5m": trend_5m,
        "trend_15m": trend_15m,
        "sr_context": sr_context,
        "call_score": call_score,
        "put_score": put_score,
        "atr": atr,
        "setup_id": f"{direction_candidate}_{signal_epoch}_{final_score}",
        "rank_strength": final_score * 10 + (2 if quality == "A_PLUS" else 0),
        "pa": "BULLISH" if direction_candidate == "CALL" else "BEARISH",
        "activity_status": activity["activity_status"] if activity["activity_status"] != "UNAVAILABLE" else "ACTIVE",
        "relative_activity": activity["relative_activity"] if activity["relative_activity"] is not None else round(float(last_row.get("tickscount", 10)) / 10.0, 2),
        "signal_candle_epoch": signal_epoch,
    }

    return direction, final_score, quality, details
