"""M1 trend/structure entries, with an explicit strong-pressure M5 override.

Score: trend+structure (2), candle pressure (2), RSI momentum (2),
support/resistance clearance (2). Mixed structure never trades.
"""

import math
import logging
from typing import Tuple, Dict, Any, Optional, List
import pandas as pd
import numpy as np

import config as cfg
from indicators import calculate_rsi

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
    Standardizes historical OHLC dataframe: trims to contiguous run,
    coerces numerics, and computes EMA, RSI, RSI slope, and ATR.
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

    work["rsi"] = calculate_rsi(work["close"], period=14)

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
# 2. Structure: confirmed swing pivots
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
# 3. Activity: live verified tick counts
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
# 4. Continuation candle
# ------------------------------------------------------------------

def continuation_candle(current, previous, want_bullish: bool) -> Optional[Dict[str, Any]]:
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
        if not is_bull or body <= 0:
            return None
        if c_close > p_high or (lower_wick > 0 and lower_wick <= 0.5 * body and c_close >= p_high):
            return {"type": "BULLISH_CONTINUATION", "strength": round(body, 8)}
        return None
    else:
        if not is_bear or body <= 0:
            return None
        if c_close < p_low or (upper_wick > 0 and upper_wick <= 0.5 * body and c_close <= p_low):
            return {"type": "BEARISH_CONTINUATION", "strength": round(body, 8)}
        return None


# ------------------------------------------------------------------
# 5. Trend, Pullback, and Context Helpers
# ------------------------------------------------------------------

def _pullback_detected(rsi_series: pd.Series, want_bullish: bool) -> bool:
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


def _trend_bias_1m(prepared: pd.DataFrame) -> str:
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
# 6. Main Strategy Evaluation Engine
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



def recent_candle_pressure(df, bullish, atr):
    """OHLC pressure proxy, not trade volume or order flow."""
    recent = df.tail(int(getattr(cfg, "RECENT_TREND_CANDLES", 5)))
    if len(recent) < 5:
        return False, False
    sign = 1 if bullish else -1
    body = (recent["close"] - recent["open"]) * sign
    ranges = (recent["high"] - recent["low"]).replace(0, np.nan)
    ratio = body / ranges
    location = ((recent["close"] - recent["low"]) / ranges if bullish
                else (recent["high"] - recent["close"]) / ranges)
    rising = (recent["close"].iloc[-1] - recent["close"].iloc[0]) * sign > 0
    extrema = ((recent["high"].iloc[-1] - recent["high"].iloc[0]) * sign > 0
               and (recent["low"].iloc[-1] - recent["low"].iloc[0]) * sign > 0)
    aligned = bool((body > 0).sum() >= 3 and rising and extrema
                   and body.iloc[-1] > 0 and location.iloc[-1] >= 0.65)
    strong_bars = (ratio >= getattr(cfg, "STRONG_PRESSURE_BODY_RATIO", 0.60)) & (location >= 0.75)
    last_three = recent.tail(3)
    pressure_move = (last_three["close"].iloc[-1] - last_three["open"].iloc[0]) * sign
    strong = bool(aligned and strong_bars.tail(3).sum() >= 2
                  and strong_bars.iloc[-1]
                  and (body.tail(2) > 0).all()
                  and pressure_move >= 0.5 * atr)
    return aligned, strong


def support_resistance_context(df, swings, close, atr, bullish):
    """Use already closed prior bars; do not count the signal bar as its own barrier."""
    lookback = int(getattr(cfg, "SR_LOOKBACK_CANDLES", 30))
    prior = df.iloc[:-1].tail(lookback)
    cutoff = len(df) - lookback - 1
    highs = [s["price"] for s in swings if s["type"] == "HIGH" and s["index"] >= cutoff]
    lows = [s["price"] for s in swings if s["type"] == "LOW" and s["index"] >= cutoff]
    highs.append(float(prior["high"].max()))
    lows.append(float(prior["low"].min()))
    resistance = min((v for v in highs if v >= close), default=None)
    support = max((v for v in lows if v <= close), default=None)
    clearance = float(getattr(cfg, "SR_CLEARANCE_ATR", 0.25)) * atr
    distance = (resistance - close if resistance is not None else math.inf) if bullish else (
        close - support if support is not None else math.inf)
    return distance > clearance, support, resistance


# Compatibility for the earlier public candle helper.
candle_reaction = continuation_candle


def evaluate_strategy(df, df_5m=None, external_context=None):
    if df is None or df.empty or len(df) < MIN_ROWS:
        return _no_trade("INSUFFICIENT_DATA")
    prepared = prepare_history(df)
    if len(prepared) < max(MIN_ROWS, int(getattr(cfg, "MIN_1M_HISTORY", 20))):
        return _no_trade("INSUFFICIENT_DATA_AFTER_GAP_TRIM")
    epoch = int(prepared.iloc[-1].get("time", 0))
    if not all(_row_ohlc_is_valid(row) for _, row in prepared.iterrows()):
        return _no_trade("INVALID_OHLC", epoch)
    if "time" in prepared and not (prepared["time"].diff().dropna() == 60).all():
        return _no_trade("INVALID_1M_SPACING", epoch)

    last, prev = prepared.iloc[-1], prepared.iloc[-2]
    atr = float(last["atr"])
    if not math.isfinite(atr) or atr <= 0:
        return _no_trade("NO_PRICE_MOVEMENT", epoch)
    body = abs(float(last["close"] - last["open"]))
    if body > 1.2 * atr:
        return _no_trade("CLIMAX_CANDLE", epoch)
    if abs(float(prev["close"] - prev["open"])) > 1.5 * atr and body < 0.5 * atr:
        return _no_trade("POST_CLIMAX_DRIFT", epoch)

    # Confirmed pivots need more than the five recent pressure candles.
    context = prepared.tail(int(getattr(cfg, "STRUCTURE_LOOKBACK_CANDLES", 60))).reset_index(drop=True)
    swings = confirmed_swings(context)
    structure = _structure_from_swings(swings)
    trend = _trend_bias_1m(prepared)
    trend_5m = _safe_5m_bias(df_5m)
    details = _no_trade("WAITING_SETUP", epoch)[3]
    details.update(structure=structure, trend_1m=trend,
                   trend_5m=trend_5m or "UNAVAILABLE", atr=atr,
                   **activity_pressure(prepared))
    if structure == "HH_HL" and trend == "BULLISH":
        candidate, bullish = "CALL", True
    elif structure == "LH_LL" and trend == "BEARISH":
        candidate, bullish = "PUT", False
    else:
        details["score_reason"] = "MIXED_OR_CONFLICTING_1M_STRUCTURE"
        return "NO_TRADE", 0, "WAIT", details

    close = float(last["close"])
    relevant = [s for s in swings if s["type"] == ("LOW" if bullish else "HIGH")]
    if not relevant or (bullish and close <= relevant[-1]["price"]) or (
            not bullish and close >= relevant[-1]["price"]):
        details["score_reason"] = "STRUCTURE_LEVEL_BROKEN"
        return "NO_TRADE", 0, "WAIT", details

    pressure, strong = recent_candle_pressure(prepared, bullish, atr)
    safe_level, support, resistance = support_resistance_context(context, swings, close, atr, bullish)
    current_rsi, previous_rsi = float(last["rsi"]), float(prev["rsi"])
    momentum = False
    if math.isfinite(current_rsi) and math.isfinite(previous_rsi):
        # Recovery is allowed to leave the old pullback band.
        momentum = ((current_rsi >= 50 and (current_rsi > previous_rsi or
                     (strong and current_rsi >= 55 and current_rsi == previous_rsi))) if bullish else
                    (current_rsi <= 50 and (current_rsi < previous_rsi or
                     (strong and current_rsi <= 45 and current_rsi == previous_rsi))))
    score = 2 + 2 * int(pressure) + 2 * int(momentum) + 2 * int(safe_level)
    reasons = ["STRUCTURE_" + structure, "TREND_" + trend + "_1M"]
    reasons.extend(name for ok, name in [(pressure, "CANDLE_PRESSURE"), (momentum, "RSI_MOMENTUM"),
                                          (safe_level, "SR_CLEAR")] if ok)
    details.update(call_score=score if bullish else 0, put_score=score if not bullish else 0,
                   support=support, resistance=resistance, strong_pressure=strong,
                   pressure_basis="OHLC_PROXY", setup_id=f"{candidate}_{epoch}_{score}",
                   rank_strength=score * 10 + body / atr, pa=trend, m5_override=False)
    direction = "NO_TRADE"
    reason = "+".join(reasons)
    if not pressure:
        reason = "RECENT_1M_PRESSURE_WEAK"
    elif not safe_level:
        reason = "RESISTANCE_TOO_CLOSE" if bullish else "SUPPORT_TOO_CLOSE"
    elif not momentum:
        reason = "RSI_MOMENTUM_NOT_CONFIRMED"
    elif score < int(getattr(cfg, "SIGNAL_THRESHOLD_CALL_PUT", 8)):
        reason = "SCORE_BELOW_THRESHOLD"
    else:
        adx = _safe_adx(external_context)
        aligned_5m = trend_5m == trend and adx is not None and adx >= getattr(cfg, "MIN_ADX_5M", 20)
        if aligned_5m:
            direction = candidate
        elif strong and getattr(cfg, "ALLOW_STRONG_1M_OVERRIDE", True):
            direction = candidate
            details["m5_override"] = True
            reason += "+STRONG_1M_OVERRIDE"
        else:
            reason = "5M_NOT_CONFIRMED_AND_1M_PRESSURE_NOT_STRONG"
    details["score_reason"] = reason
    return direction, score, "A_PLUS" if score == 8 else "WAIT", details
