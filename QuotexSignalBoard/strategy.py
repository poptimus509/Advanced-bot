"""
Strategy engine for QuotexSignalBoard — Pullback-then-continuation edition.

The previous 8/8 mature-momentum chaser bought exhaustion tops (every
component rewarded an already-extended move). This version does the
opposite: it waits for a higher-timeframe trend, a 1M pullback into a
value zone, and a continuation candle back in the trend direction.

Scoring (max 8, threshold 6 — the only threshold, configured in config.py):
  1. TREND (2 pts): 1M EMA21 > EMA50 with price above EMA21, optionally
     gated by the real 5M regime (mandatory gate when 5M data is usable).
  2. PULLBACK (2 pts): during an uptrend RSI(14) dipped into 40-50 within
     the last PULLBACK_LOOKBACK_CANDLES bars (and mirrored 50-60 zone for
     downtrends). This is the "value" entry — no more buying the top.
  3. CONTINUATION (2 pts): the latest closed 1M candle is a real reaction
     candle back in the trend direction (body/wick check, direction first).
  4. RSI SLOPE (2 pts): RSI velocity — the last two RSI readings are rising
     (CALL) / falling (PUT). Measures momentum rebuilding, not a static band.

Gates (never add score): 5M regime alignment (when available) and ADX(14)
on 5M for trend strength. Garbage/missing HTF data is a no-op, matching the
original defensive contract.
"""

import math
import logging
from typing import Tuple, Dict, Any, Optional, List
import pandas as pd
import numpy as np

import config as cfg

logger = logging.getLogger("QuotexSignalBoard.Strategy")

MIN_ROWS = 15
SWING_LOOKAROUND = 2


# ------------------------------------------------------------------
# 1. History preparation: gap detection + indicators, non-destructive
# ------------------------------------------------------------------

def _infer_timeframe_seconds(times: np.ndarray) -> Optional[float]:
    if len(times) < 3:
        return None
    diffs = np.diff(times)
    diffs = diffs[diffs > 0]
    if len(diffs) == 0:
        return None
    values, counts = np.unique(diffs, return_counts=True)
    return float(values[np.argmax(counts)])


def _latest_contiguous_segment(df: pd.DataFrame) -> pd.DataFrame:
    if "time" not in df.columns or len(df) < 3:
        return df

    times = df["time"].to_numpy(dtype=float)
    tf_sec = _infer_timeframe_seconds(times)
    if tf_sec is None or tf_sec <= 0:
        return df

    start = len(times) - 1
    for i in range(len(times) - 1, 0, -1):
        if abs(times[i] - times[i - 1] - tf_sec) > 1e-6:
            start = i
            break
        start = i - 1

    return df.iloc[start:].reset_index(drop=True)


def prepare_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes a historical OHLC dataframe: trims to the latest
    contiguous run of candles, coerces numeric columns, and calculates
    EMA/RSI/ATR plus the RSI slope (velocity) series. Never mutates input.
    """
    if df is None or df.empty or len(df) < MIN_ROWS:
        return df if df is not None else pd.DataFrame()

    work = df.copy(deep=True)

    for col in ["open", "high", "low", "close"]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    work = _latest_contiguous_segment(work)
    if len(work) < MIN_ROWS:
        return work

    work["ema_9"] = work["close"].ewm(span=9, adjust=False).mean()
    work["ema_21"] = work["close"].ewm(span=21, adjust=False).mean()
    work["ema_50"] = work["close"].ewm(span=50, adjust=False).mean()

    delta = work["close"].diff()
    gain = (delta.where(delta > 0, 0.0)).rolling(window=14, min_periods=14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=14, min_periods=14).mean()
    rs = gain / loss.replace(0, np.nan)
    work["rsi"] = 100 - (100 / (1 + rs))
    work["rsi"] = work["rsi"].fillna(50.0)

    # RSI velocity (slope): momentum rebuilding vs fading, replaces the
    # old static RSI band chase.
    work["rsi_slope"] = work["rsi"].diff()
    work["rsi_slope"] = work["rsi_slope"].fillna(0.0)

    high_low = work["high"] - work["low"]
    high_close = (work["high"] - work["close"].shift()).abs()
    low_close = (work["low"] - work["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    work["atr"] = tr.rolling(window=14, min_periods=1).mean()

    return work


def _row_ohlc_is_valid(row) -> bool:
    try:
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
    except Exception:
        return False
    if not all(math.isfinite(v) for v in (o, h, l, c)):
        return False
    if h <= 0 or l <= 0 or o <= 0 or c <= 0:
        return False
    if h < l:
        return False
    if o > h or o < l or c > h or c < l:
        return False
    return True


# ------------------------------------------------------------------
# 2. Structure: confirmed swing pivots (kept, non-repainting)
# ------------------------------------------------------------------

def confirmed_swings(df: pd.DataFrame, lookaround: int = SWING_LOOKAROUND) -> List[Dict[str, Any]]:
    swings: List[Dict[str, Any]] = []
    if df is None or len(df) < (2 * lookaround + 1):
        return swings

    highs = df["high"].to_numpy(dtype=float)
    lows = df["low"].to_numpy(dtype=float)
    n = len(df)

    for i in range(lookaround, n - lookaround):
        window_high = highs[i - lookaround: i + lookaround + 1]
        window_low = lows[i - lookaround: i + lookaround + 1]

        if highs[i] == window_high.max() and i == (i - lookaround) + int(np.argmax(window_high)):
            swings.append({
                "index": i,
                "confirmed_index": i + lookaround,
                "type": "HIGH",
                "price": float(highs[i]),
            })

        if lows[i] == window_low.min() and i == (i - lookaround) + int(np.argmin(window_low)):
            swings.append({
                "index": i,
                "confirmed_index": i + lookaround,
                "type": "LOW",
                "price": float(lows[i]),
            })

    return swings


def _structure_from_swings(swings: List[Dict[str, Any]]) -> str:
    highs = sorted((s for s in swings if s["type"] == "HIGH"), key=lambda s: s["index"])
    lows = sorted((s for s in swings if s["type"] == "LOW"), key=lambda s: s["index"])

    if len(highs) < 2 or len(lows) < 2:
        return "MIXED"

    higher_high = highs[-1]["price"] > highs[-2]["price"]
    higher_low = lows[-1]["price"] > lows[-2]["price"]
    lower_high = highs[-1]["price"] < highs[-2]["price"]
    lower_low = lows[-1]["price"] < lows[-2]["price"]

    if higher_high and higher_low:
        return "HH_HL"
    if lower_high and lower_low:
        return "LH_LL"
    return "MIXED"


# ------------------------------------------------------------------
# 3. Activity: only trust live verified tick counts, never fake it
# ------------------------------------------------------------------

def activity_pressure(df: pd.DataFrame, baseline_window: int = None) -> Dict[str, Any]:
    result = {"activity_status": "UNAVAILABLE", "pressure": None, "relative_activity": None}

    if df is None or "verified_ticks" not in df.columns or len(df) < 3:
        return result

    window = baseline_window or getattr(cfg, "ACTIVITY_BASELINE_CANDLES", 20)
    ticks = pd.to_numeric(df["verified_ticks"], errors="coerce")
    valid = ticks.dropna()
    if len(valid) < 2:
        return result

    baseline = valid.iloc[:-1].tail(window).mean()
    last_val = valid.iloc[-1]

    if not math.isfinite(baseline) or baseline <= 0:
        return result

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


# ------------------------------------------------------------------
# 4. Continuation candle: latest candle confirms trend direction
# ------------------------------------------------------------------

def continuation_candle(current, previous, want_bullish: bool) -> Optional[Dict[str, Any]]:
    """
    Is the latest closed candle a genuine continuation candle back in the
    trend direction after the pullback? Direction is checked first and is
    non-negotiable: a bearish candle can never confirm a CALL (and vice
    versa). Stronger than the old generic "reaction" check: the close must
    also take out the previous candle's extreme in the trade direction.
    """
    try:
        c_open = float(current["open"])
        c_close = float(current["close"])
        c_high = float(current["high"])
        c_low = float(current["low"])
        p_high = float(previous["high"])
        p_low = float(previous["low"])
    except Exception:
        return None

    is_bull = c_close > c_open
    is_bear = c_close < c_open
    body = abs(c_close - c_open)
    upper_wick = c_high - max(c_open, c_close)
    lower_wick = min(c_open, c_close) - c_low

    if want_bullish:
        if not is_bull:
            return None
        if body <= 0:
            return None
        if c_close > p_high or (lower_wick > 0 and lower_wick <= 0.5 * body and c_close >= p_high):
            return {"type": "BULLISH_CONTINUATION", "strength": round(body, 8)}
        return None
    else:
        if not is_bear:
            return None
        if body <= 0:
            return None
        if c_close < p_low or (upper_wick > 0 and upper_wick <= 0.5 * body and c_close <= p_low):
            return {"type": "BEARISH_CONTINUATION", "strength": round(body, 8)}
        return None


# ------------------------------------------------------------------
# 5. Pullback detection + RSI slope, trend definition
# ------------------------------------------------------------------

def _pullback_detected(rsi_series: pd.Series, want_bullish: bool) -> bool:
    """
    True if, within the last PULLBACK_LOOKBACK_CANDLES bars (excluding the
    current confirmation bar), RSI visited the pullback value zone:
      CALL (uptrend): RSI dipped into [40, 50]
      PUT  (downtrend): RSI rose into [50, 60]
    """
    lookback = int(getattr(cfg, "PULLBACK_LOOKBACK_CANDLES", 6))
    if rsi_series is None or len(rsi_series) < lookback + 1:
        return False

    window = pd.to_numeric(rsi_series.iloc[-(lookback + 1):-1], errors="coerce").dropna()
    if len(window) < 2:
        return False

    if want_bullish:
        lo = float(getattr(cfg, "RSI_PULLBACK_CALL_MIN", 40.0))
        hi = float(getattr(cfg, "RSI_PULLBACK_CALL_MAX", 50.0))
    else:
        lo = float(getattr(cfg, "RSI_PULLBACK_PUT_MIN", 50.0))
        hi = float(getattr(cfg, "RSI_PULLBACK_PUT_MAX", 60.0))

    return bool(((window >= lo) & (window <= hi)).any())


def _rsi_slope_confirms(rsi: pd.Series, rsi_slope: pd.Series, want_bullish: bool) -> bool:
    """
    RSI velocity: momentum must be REBUILDING in the trade direction on the
    confirmation bar (and not collapsing). Replaces the old static band.
    """
    if rsi is None or rsi_slope is None or len(rsi) < 3:
        return False
    try:
        last_slope = float(rsi_slope.iloc[-1])
        prev_slope = float(rsi_slope.iloc[-2])
        if not (math.isfinite(last_slope) and math.isfinite(prev_slope)):
            return False
        if want_bullish:
            return last_slope > 0 and (prev_slope > 0 or last_slope > prev_slope)
        return last_slope < 0 and (prev_slope < 0 or last_slope < prev_slope)
    except Exception:
        return False


def _trend_bias_1m(prepared: pd.DataFrame) -> str:
    """1M trend: EMA21 vs EMA50 with price location."""
    try:
        last = prepared.iloc[-1]
        close = float(last["close"])
        ema21 = float(last.get("ema_21", close))
        ema50 = float(last.get("ema_50", close))
        if close > ema21 > ema50:
            return "BULLISH"
        if close < ema21 < ema50:
            return "BEARISH"
        return "NEUTRAL"
    except Exception:
        return "NEUTRAL"


# ------------------------------------------------------------------
# 6. Optional higher-timeframe / volatility gates (defensive, no-op on
#    invalid input - never contribute score points directly)
# ------------------------------------------------------------------

def _safe_5m_bias(df_5m) -> Optional[str]:
    try:
        if df_5m is None or not isinstance(df_5m, pd.DataFrame):
            return None
        min_needed = max(MIN_ROWS, getattr(cfg, "CONTEXT_5M_MIN_CANDLES", 10))
        if len(df_5m) < min_needed:
            return None
        if not {"open", "high", "low", "close"}.issubset(df_5m.columns):
            return None

        prepared = prepare_history(df_5m)
        if prepared is None or prepared.empty or len(prepared) < MIN_ROWS:
            return None

        last = prepared.iloc[-1]
        close = float(last["close"])
        ema21 = float(last.get("ema_21", close))
        ema50 = float(last.get("ema_50", close))

        if close > ema21 >= ema50:
            return "BULLISH"
        if close < ema21 <= ema50:
            return "BEARISH"
        return "NEUTRAL"
    except Exception:
        return None


def _safe_adx(external_context) -> Optional[float]:
    try:
        if external_context is None:
            return None
        value = None
        if isinstance(external_context, dict):
            value = external_context.get("adx_5m")
        else:
            value = getattr(external_context, "adx_5m", None)
        if value is None:
            return None
        value = float(value)
        if not math.isfinite(value) or value < 0:
            return None
        return value
    except Exception:
        return None


# ------------------------------------------------------------------
# 7. Main entry point
# ------------------------------------------------------------------

def _no_trade(reason: str, epoch: Any = 0) -> Tuple[str, int, str, Dict[str, Any]]:
    return "NO_TRADE", 0, "WAIT", {
        "score_reason": reason,
        "structure": "MIXED",
        "trend_5m": "UNAVAILABLE",
        "call_score": 0,
        "put_score": 0,
        "atr": 0.0001,
        "setup_id": f"NONE_{epoch}_0",
        "signal_candle_epoch": epoch,
        "rank_strength": 0,
        "pa": "NEUTRAL",
        "activity_status": "UNAVAILABLE",
        "relative_activity": None,
    }


def evaluate_strategy(
    df: pd.DataFrame,
    df_5m: Optional[pd.DataFrame] = None,
    external_context: Any = None,
) -> Tuple[str, int, str, Dict[str, Any]]:
    """
    Pullback-then-continuation evaluation. Returns
    (direction, score, quality, details).
    """
    if df is None or df.empty or len(df) < MIN_ROWS:
        return _no_trade("INSUFFICIENT_DATA", 0)

    prepared = prepare_history(df)
    if prepared is None or prepared.empty or len(prepared) < MIN_ROWS:
        return _no_trade("INSUFFICIENT_DATA_AFTER_GAP_TRIM", 0)

    last_row = prepared.iloc[-1]
    prev_row = prepared.iloc[-2]
    epoch = int(last_row["time"]) if "time" in prepared.columns else 0

    if not _row_ohlc_is_valid(last_row):
        return _no_trade("INVALID_OHLC", epoch)

    atr = float(last_row.get("atr", 0.0001))
    if not math.isfinite(atr) or atr <= 0:
        atr = 0.0001

    curr_close = float(last_row["close"])
    ema_9 = float(last_row.get("ema_9", curr_close))
    ema_21 = float(last_row.get("ema_21", curr_close))
    ema_50 = float(last_row.get("ema_50", curr_close))

    swings = confirmed_swings(prepared)
    structure = _structure_from_swings(swings)
    activity = activity_pressure(prepared)

    rsi = prepared["rsi"]
    rsi_slope = prepared["rsi_slope"]

    bias_1m = _trend_bias_1m(prepared)

    call_score = 0
    put_score = 0
    call_reasons: List[str] = []
    put_reasons: List[str] = []

    trend_5m = _safe_5m_bias(df_5m)

    # --- CALL side: uptrend pullback-continuation ---------------------
    if bias_1m == "BULLISH" or (trend_5m == "BULLISH" and bias_1m != "BEARISH"):
        call_score += 2
        call_reasons.append("TREND_UP_1M")

        if _pullback_detected(rsi, want_bullish=True):
            call_score += 2
            call_reasons.append("RSI_PULLBACK_40_50")

        cont = continuation_candle(last_row, prev_row, want_bullish=True)
        if cont is not None:
            call_score += 2
            call_reasons.append("BULLISH_CONTINUATION")

        if _rsi_slope_confirms(rsi, rsi_slope, want_bullish=True):
            call_score += 2
            call_reasons.append("RSI_SLOPE_RISING")

        if structure == "HH_HL":
            call_reasons.append("STRUCTURE_HH_HL")

    # --- PUT side: downtrend pullback-continuation --------------------
    if bias_1m == "BEARISH" or (trend_5m == "BEARISH" and bias_1m != "BULLISH"):
        put_score += 2
        put_reasons.append("TREND_DOWN_1M")

        if _pullback_detected(rsi, want_bullish=False):
            put_score += 2
            put_reasons.append("RSI_PULLBACK_50_60")

        cont = continuation_candle(last_row, prev_row, want_bullish=False)
        if cont is not None:
            put_score += 2
            put_reasons.append("BEARISH_CONTINUATION")

        if _rsi_slope_confirms(rsi, rsi_slope, want_bullish=False):
            put_score += 2
            put_reasons.append("RSI_SLOPE_FALLING")

        if structure == "LH_LL":
            put_reasons.append("STRUCTURE_LH_LL")

    call_score = min(call_score, 8)
    put_score = min(put_score, 8)

    if call_score == put_score:
        final_score = call_score
        direction_candidate = "NONE"
    elif call_score > put_score:
        final_score = call_score
        direction_candidate = "CALL"
    else:
        final_score = put_score
        direction_candidate = "PUT"

    if final_score >= 7:
        quality = "A_PLUS"
    elif final_score >= 5:
        quality = "STANDARD"
    elif final_score >= 3:
        quality = "MODERATE"
    else:
        quality = "WAIT"

    signal_threshold = getattr(cfg, "SIGNAL_THRESHOLD_CALL_PUT", 6)

    direction = "NO_TRADE"
    score_reason = "+".join((call_reasons if direction_candidate == "CALL" else put_reasons)[:3]) or "WAITING_SETUP"
    gate_reason = None

    if direction_candidate in ("CALL", "PUT") and final_score >= signal_threshold:
        adx_5m = _safe_adx(external_context)
        min_adx = getattr(cfg, "MIN_ADX_5M", 20.0)

        # Mandatory regime gate ONLY when real 5M data is usable.
        if trend_5m is not None and trend_5m != "NEUTRAL":
            wanted = "BULLISH" if direction_candidate == "CALL" else "BEARISH"
            if trend_5m != wanted:
                gate_reason = "5M_REGIME_CONFLICT"

        if gate_reason is None and adx_5m is not None and adx_5m < min_adx:
            gate_reason = "ADX_BELOW_MIN"

        if gate_reason is None:
            direction = direction_candidate
        else:
            score_reason = gate_reason

    details = {
        "score_reason": score_reason,
        "structure": structure,
        "trend_5m": trend_5m or "UNAVAILABLE",
        "call_score": call_score,
        "put_score": put_score,
        "atr": atr,
        "setup_id": f"{direction_candidate}_{epoch}_{final_score}",
        "signal_candle_epoch": epoch,
        "rank_strength": final_score * 10 + abs(ema_9 - ema_21) / atr,
        "pa": "BULLISH" if direction_candidate == "CALL" else ("BEARISH" if direction_candidate == "PUT" else "NEUTRAL"),
        **activity,
    }

    return direction, final_score, quality, details
