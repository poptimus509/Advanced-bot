"""
Strategy engine for QuotexSignalBoard.

Design goals (see CORRECTIONS.md for the full rationale):

1.  Every scoring component must be measuring something independent.
    The previous version scored "market structure", "5M trend" and "EMA
    stack" from the *same* 1M closing price three separate times, which
    inflated scores without adding real confluence.

2.  "5M trend" must actually come from 5M candles. If the caller does not
    have a real, sufficiently long 5M dataframe, the regime filter is
    skipped rather than faked from 1M data.

3.  The threshold must be calibrated against noise. A strategy that fires
    on ~50% of pure random walk data is not "high-confluence" - it is a
    coin flip with extra steps. See tests/test_strategy.py and the
    accompanying noise-check in tests/test_noise_rejection.py.

4.  No forced direction on ties. If bullish and bearish evidence are
    equal, the correct answer is "no trade", not "always call".

evaluate_strategy(df_1m, df_5m=None, external_context=None) is the public
entry point. df_5m and external_context are optional, defensive inputs:
if they are missing, malformed, or otherwise unusable, they are silently
ignored and have no effect on the result (this is asserted by
test_higher_timeframes_have_no_effect). When df_5m is a real, adequately
sized 5-minute OHLC dataframe, it is used as a mandatory regime alignment
gate, and when external_context carries a valid numeric "adx_5m", it is
used as a volatility/trend-strength gate. Neither ever contributes score
points on its own, so they cannot be double-counted against the 1M
components.
"""

import math
import logging
from typing import Tuple, Dict, Any, Optional, List
import pandas as pd
import numpy as np

import config as cfg
from indicators import calculate_atr, calculate_rsi

logger = logging.getLogger("QuotexSignalBoard.Strategy")

MIN_ROWS = 15
SWING_LOOKAROUND = 2  # bars required on each side to confirm a fractal pivot


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
    """
    Finds the timeframe's modal candle spacing and returns only the most
    recent run of candles that respects that spacing, dropping anything
    before a gap (missed candles, feed outage, symbol resync, etc.).
    Never mutates the input.
    """
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
    EMA/RSI/ATR. Always returns a new dataframe; never mutates the input.
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

    # Use one standard implementation for momentum/volatility throughout
    # the project.  The previous strategy had a separate simple-rolling
    # RSI implementation and converted zero-loss cases to RSI=50, which
    # made strong one-sided moves appear neutral.
    work["rsi"] = calculate_rsi(work["close"], period=14)
    work["atr"] = calculate_atr(work, period=14)

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
# 2. Structure: confirmed swing pivots (non-repainting)
# ------------------------------------------------------------------

def confirmed_swings(df: pd.DataFrame, lookaround: int = SWING_LOOKAROUND) -> List[Dict[str, Any]]:
    """
    Detects fractal swing highs/lows. A pivot at index i only depends on
    bars [i-lookaround, i+lookaround], so once confirmed it can never be
    rewritten by future bars arriving later (no repainting). The pivot is
    considered "confirmed" once i+lookaround bars exist, i.e. at index
    (i + lookaround) in the sequence.
    """
    swings: List[Dict[str, Any]] = []
    if df is None or len(df) < (2 * lookaround + 1):
        return swings

    highs = df["high"].to_numpy(dtype=float)
    lows = df["low"].to_numpy(dtype=float)
    n = len(df)

    for i in range(lookaround, n - lookaround):
        window_high = highs[i - lookaround: i + lookaround + 1]
        window_low = lows[i - lookaround: i + lookaround + 1]

        # On a plateau (two adjacent bars tying for the window extreme,
        # e.g. a flat top), always credit the leftmost bar so the result
        # is deterministic and identical whether or not later bars have
        # arrived yet (required for the no-repaint guarantee).
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
    """
    Historical candle "tick count" fields from a REST candle endpoint are
    not real trade volume and must never be presented as one. This only
    reports a signal when the dataframe carries genuine, verified live
    tick counts (see data/candle_manager.py's verified_ticks column).
    Otherwise it honestly reports UNAVAILABLE instead of inventing a
    baseline of "10".
    """
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
# 4. Candle reaction: does the latest candle actually confirm direction?
# ------------------------------------------------------------------

def candle_reaction(current, previous, want_bullish: bool) -> Optional[Dict[str, Any]]:
    """
    Checks whether `current` is a real reaction candle in the requested
    direction relative to `previous`. A bearish candle can never confirm
    a bullish entry (and vice versa) - direction is checked first and is
    non-negotiable.
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
        if lower_wick > body or c_close > p_high:
            return {"type": "BULLISH_REACTION", "strength": round(body, 8)}
        return {"type": "BULLISH_WEAK", "strength": round(body, 8)}
    else:
        if not is_bear:
            return None
        if upper_wick > body or c_close < p_low:
            return {"type": "BEARISH_REACTION", "strength": round(body, 8)}
        return {"type": "BEARISH_WEAK", "strength": round(body, 8)}


# ------------------------------------------------------------------
# 5. Optional higher-timeframe / volatility gates (defensive, no-op on
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
# 6. Main entry point
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
    Evaluates one symbol's 1M history and returns
    (direction, score, quality, details).

    Every scoring component below is derived from a distinct piece of
    evidence (structure via confirmed swings, EMA stack alignment,
    RSI trend-following momentum, and the latest candle's reaction) so
    that no single fact is counted twice. df_5m / external_context are
    optional and gate-only (see module docstring); malformed values are
    ignored and produce the exact same result as omitting them.
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
    curr_rsi = float(last_row.get("rsi", 50.0))
    ema_9 = float(last_row.get("ema_9", curr_close))
    ema_21 = float(last_row.get("ema_21", curr_close))
    ema_50 = float(last_row.get("ema_50", curr_close))

    swings = confirmed_swings(prepared)
    structure = _structure_from_swings(swings)
    activity = activity_pressure(prepared)

    call_score = 0
    put_score = 0
    call_reasons: List[str] = []
    put_reasons: List[str] = []

    # 1) Market structure via confirmed swing pivots (independent of EMA/RSI)
    if structure == "HH_HL":
        call_score += 2
        call_reasons.append("STRUCTURE_HH_HL")
    elif structure == "LH_LL":
        put_score += 2
        put_reasons.append("STRUCTURE_LH_LL")

    # 2) EMA stack alignment (trend direction of the 1M series)
    if curr_close > ema_9 > ema_21 > ema_50:
        call_score += 2
        call_reasons.append("EMA_BULLISH_STACK")
    elif curr_close < ema_9 < ema_21 < ema_50:
        put_score += 2
        put_reasons.append("EMA_BEARISH_STACK")

    # 3) RSI trend-following momentum only (mean-reversion branch removed:
    #    it previously awarded points to the OPPOSITE side of a strong
    #    trend, contradicting the structure/EMA components above).
    rsi_call_min = getattr(cfg, "RSI_CALL_MIN", 55.0)
    rsi_call_max = getattr(cfg, "RSI_CALL_MAX", 70.0)
    rsi_put_min = getattr(cfg, "RSI_PUT_MIN", 30.0)
    rsi_put_max = getattr(cfg, "RSI_PUT_MAX", 45.0)

    if rsi_call_min < curr_rsi < rsi_call_max:
        call_score += 2
        call_reasons.append("RSI_BULLISH_MOMENTUM")
    elif rsi_put_min < curr_rsi < rsi_put_max:
        put_score += 2
        put_reasons.append("RSI_BEARISH_MOMENTUM")

    # 4) Candle reaction confirmation from the latest closed candle
    bull_reaction = candle_reaction(last_row, prev_row, True)
    bear_reaction = candle_reaction(last_row, prev_row, False)
    if bull_reaction and bull_reaction["type"] == "BULLISH_REACTION":
        call_score += 2
        call_reasons.append("BULLISH_REACTION_CANDLE")
    if bear_reaction and bear_reaction["type"] == "BEARISH_REACTION":
        put_score += 2
        put_reasons.append("BEARISH_REACTION_CANDLE")

    call_score = min(call_score, 8)
    put_score = min(put_score, 8)

    # No forced tie-break: if both sides are equally (un)supported, that
    # is genuinely "no edge", not a coin flip resolved in CALL's favor.
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

    signal_threshold = getattr(cfg, "SIGNAL_THRESHOLD_CALL_PUT", 8)

    direction = "NO_TRADE"
    score_reason = "+".join((call_reasons if direction_candidate == "CALL" else put_reasons)[:3]) or "WAITING_SETUP"
    gate_reason = None

    if direction_candidate in ("CALL", "PUT") and final_score >= signal_threshold:
        # Optional mandatory gates. Only ever applied when the caller
        # supplied genuinely usable higher-timeframe / volatility data;
        # garbage or missing input is a no-op (see module docstring).
        bias_5m = _safe_5m_bias(df_5m)
        adx_5m = _safe_adx(external_context)
        min_adx = getattr(cfg, "MIN_ADX_5M", 20.0)

        if bias_5m is not None and bias_5m != "NEUTRAL":
            wanted = "BULLISH" if direction_candidate == "CALL" else "BEARISH"
            if bias_5m != wanted:
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
        "trend_5m": _safe_5m_bias(df_5m) or "UNAVAILABLE",
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
