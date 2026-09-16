import math
import logging
from typing import Tuple, Dict, Any, Optional
import pandas as pd
import numpy as np
import config as cfg

logger = logging.getLogger("QuotexSignalBoard.Strategy")

def prepare_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes historical dataframe and calculates fundamental indicators
    such as EMAs, RSI, and ATR.
    """
    if df.empty or len(df) < 15:
        return df

    df = df.copy()
    
    # Ensure numeric columns
    for col in ["open", "high", "low", "close"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 1. EMA Calculations
    df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()
    df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()

    # 2. RSI Calculation (Period 14)
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0.0)).rolling(window=14, min_periods=14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=14, min_periods=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))
    df["rsi"] = df["rsi"].fillna(50.0)

    # 3. ATR Calculation (Period 14)
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["atr"] = tr.rolling(window=14, min_periods=1).mean()

    return df


def detect_market_structure(df: pd.DataFrame) -> Tuple[str, float, float]:
    """
    Determines market structure (HH_HL, LH_LL, or MIXED)
    and extracts recent swing highs/lows.
    """
    if len(df) < 10:
        return "MIXED", 0.0, 0.0

    recent = df.tail(10)
    swing_high = recent["high"].max()
    swing_low = recent["low"].min()

    # Basic structure check over last 5 candles
    last_5 = df.tail(5)
    highs = last_5["high"].values
    lows = last_5["low"].values

    is_bullish = (highs[-1] >= highs[0]) and (lows[-1] >= lows[0])
    is_bearish = (highs[-1] <= highs[0]) and (lows[-1] <= lows[0])

    if is_bullish and not is_bearish:
        structure = "HH_HL"
    elif is_bearish and not is_bullish:
        structure = "LH_LL"
    else:
        structure = "MIXED"

    return structure, swing_high, swing_low


def detect_m5_trend(df: pd.DataFrame) -> str:
    """
    Estimates 5-minute context trend using EMA slope or relationship.
    """
    if len(df) < 25:
        return "NEUTRAL"

    curr_close = df.iloc[-1]["close"]
    ema_21 = df.iloc[-1]["ema_21"]
    ema_50 = df.iloc[-1].get("ema_50", ema_21)

    if curr_close > ema_21 and ema_21 >= ema_50:
        return "BULLISH"
    elif curr_close < ema_21 and ema_21 <= ema_50:
        return "BEARISH"
    return "NEUTRAL"


def evaluate_strategy(df: pd.DataFrame, df5: Optional[pd.DataFrame] = None,
                      df15: Optional[pd.DataFrame] = None) -> Tuple[str, int, str, Dict[str, Any]]:
    """
    Evaluates full strategy with granular cumulative scoring.
    Does not early-exit so scores like 1, 2, 3, 4, 5, 6 are preserved and visible.
    """
    if df.empty or len(df) < 15:
        return "NO_TRADE", 0, "WAIT", {
            "score_reason": "INSUFFICIENT_DATA",
            "structure": "MIXED",
            "trend_5m": "UNAVAILABLE",
            "call_score": 0,
            "put_score": 0,
            "atr": 0.0001,
            "setup_id": "NONE",
            "rank_strength": 0,
            "pa": "NEUTRAL"
        }

    last_row = df.iloc[-1]
    prev_row = df.iloc[-2]

    curr_close = float(last_row["close"])
    curr_open = float(last_row["open"])
    curr_high = float(last_row["high"])
    curr_low = float(last_row["low"])
    curr_rsi = float(last_row.get("rsi", 50.0))
    atr = float(last_row.get("atr", 0.0001))
    if not math.isfinite(atr) or atr <= 0:
        atr = 0.0001

    structure, swing_high, swing_low = detect_market_structure(df)
    # These must be genuine higher-timeframe dataframes. Never label 1M data as 5M.
    context5 = prepare_history(df5) if df5 is not None and not df5.empty else pd.DataFrame()
    context15 = prepare_history(df15) if df15 is not None and not df15.empty else pd.DataFrame()
    trend_5m = detect_m5_trend(context5) if len(context5) >= 25 else "UNAVAILABLE"
    trend_15m = detect_m5_trend(context15) if len(context15) >= 25 else "UNAVAILABLE"

    call_score = 0
    put_score = 0
    reasons = []

    # -------------------------------------------------------------
    # 1. Market Structure Scoring (Up to +2 Points)
    # -------------------------------------------------------------
    if structure == "HH_HL":
        call_score += 2
        reasons.append("BULLISH_STRUCTURE")
    elif structure == "LH_LL":
        put_score += 2
        reasons.append("BEARISH_STRUCTURE")
    else:
        reasons.append("STRUCTURE_MIXED")

    # -------------------------------------------------------------
    # 2. Higher Timeframe Context (5M Trend) (Up to +2 Points)
    # -------------------------------------------------------------
    if trend_5m == "BULLISH":
        call_score += 1
        reasons.append("M5_BULLISH")
    elif trend_5m == "BEARISH":
        put_score += 1
        reasons.append("M5_BEARISH")

    if trend_15m == "BULLISH":
        call_score += 1
        reasons.append("M15_BULLISH")
    elif trend_15m == "BEARISH":
        put_score += 1
        reasons.append("M15_BEARISH")

    # -------------------------------------------------------------
    # 3. EMA Dynamic Trend & Alignment (Up to +2 Points)
    # -------------------------------------------------------------
    ema_9 = float(last_row.get("ema_9", curr_close))
    ema_21 = float(last_row.get("ema_21", curr_close))

    if curr_close > ema_9 > ema_21:
        call_score += 2
        reasons.append("EMA_BULLISH_STACK")
    elif curr_close < ema_9 < ema_21:
        put_score += 2
        reasons.append("EMA_BEARISH_STACK")
    elif curr_close > ema_9:
        call_score += 1
    elif curr_close < ema_9:
        put_score += 1

    # -------------------------------------------------------------
    # 4. Momentum / RSI Filter (Up to +2 Points)
    # -------------------------------------------------------------
    if getattr(cfg, "RSI_CALL_MIN", 55) < curr_rsi < getattr(cfg, "RSI_CALL_MAX", 70):
        call_score += 2
        reasons.append("RSI_BULLISH_MOMENTUM")
    elif getattr(cfg, "RSI_PUT_MIN", 30) < curr_rsi < getattr(cfg, "RSI_PUT_MAX", 45):
        put_score += 2
        reasons.append("RSI_BEARISH_MOMENTUM")
    # Oversold/overbought reversal is intentionally not mixed into a trend score.

    # -------------------------------------------------------------
    # 5. Price Action & Candlestick Confirmation (Up to +2 Points)
    # -------------------------------------------------------------
    candle_body = abs(curr_close - curr_open)
    upper_wick = curr_high - max(curr_close, curr_open)
    lower_wick = min(curr_close, curr_open) - curr_low

    is_bullish_candle = curr_close > curr_open
    is_bearish_candle = curr_close < curr_open

    # Rejection wick / Strong candle body
    candle_range = max(curr_high - curr_low, 1e-12)
    body_ratio = candle_body / candle_range
    if is_bullish_candle:
        if lower_wick >= candle_body * 1.25 and lower_wick / candle_range >= 0.30:
            call_score += 3
            reasons.append("BULLISH_REJECTION_WICK")
        elif body_ratio >= 0.55:
            call_score += 3
            reasons.append("BULLISH_MOMENTUM")
    elif is_bearish_candle:
        if upper_wick >= candle_body * 1.25 and upper_wick / candle_range >= 0.30:
            put_score += 3
            reasons.append("BEARISH_REJECTION_WICK")
        elif body_ratio >= 0.55:
            put_score += 3
            reasons.append("BEARISH_MOMENTUM")

    # -------------------------------------------------------------
    # Final Decision Calculation
    # -------------------------------------------------------------
    call_score = min(call_score, 10)
    put_score = min(put_score, 10)

    # Determine highest score side
    if call_score == put_score:
        final_score = call_score
        direction_candidate = "NONE"
    elif call_score > put_score:
        final_score = call_score
        direction_candidate = "CALL"
    else:
        final_score = put_score
        direction_candidate = "PUT"

    # Quality categorization based on total score
    if final_score >= 8:
        quality = "A_PLUS"
    elif final_score >= 6:
        quality = "STANDARD"
    elif final_score >= 4:
        quality = "MODERATE"
    else:
        quality = "WAIT"

    # Trade Trigger Threshold: Requires score >= 7 and matching 5M/Structure alignment
    SIGNAL_THRESHOLD = getattr(cfg, "SIGNAL_THRESHOLD_CALL_PUT", 8)
    score_margin = abs(call_score - put_score)
    if direction_candidate != "NONE" and final_score >= SIGNAL_THRESHOLD and score_margin >= 2:
        direction = direction_candidate
        score_reason = "+".join(reasons[-3:]) if reasons else "SETUP_CONFIRMED"
    else:
        direction = "NO_TRADE"
        score_reason = "+".join(reasons[:2]) if reasons else "WAITING_SETUP"

    details = {
        "score_reason": score_reason,
        "structure": structure,
        "trend_5m": trend_5m,
        "trend_15m": trend_15m,
        "call_score": call_score,
        "put_score": put_score,
        "atr": atr,
        "setup_id": f"{direction_candidate}_{int(last_row['time'])}_{final_score}",
        "rank_strength": final_score * 10 + (2 if quality == "A_PLUS" else 0),
        "pa": "BULLISH" if direction_candidate == "CALL" else "BEARISH",
        "activity_status": "ACTIVE",
        "relative_activity": round(float(last_row.get("tickscount", 10)) / 10.0, 2)
    }

    return direction, final_score, quality, details
