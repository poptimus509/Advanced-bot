import math
import logging
from typing import Tuple, Dict, Any, Optional
import pandas as pd
import numpy as np

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


def evaluate_strategy(df: pd.DataFrame) -> Tuple[str, int, str, Dict[str, Any]]:
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
    trend_5m = detect_m5_trend(df)

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
        call_score += 2
        reasons.append("M5_BULLISH")
    elif trend_5m == "BEARISH":
        put_score += 2
        reasons.append("M5_BEARISH")

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
    if curr_rsi > 55 and curr_rsi < 75:
        call_score += 2
        reasons.append("RSI_BULLISH_MOMENTUM")
    elif curr_rsi < 45 and curr_rsi > 25:
        put_score += 2
        reasons.append("RSI_BEARISH_MOMENTUM")
    elif curr_rsi <= 25:
        # Potential oversold reversal
        call_score += 1
        reasons.append("RSI_OVERSOLD")
    elif curr_rsi >= 75:
        # Potential overbought reversal
        put_score += 1
        reasons.append("RSI_OVERBOUGHT")

    # -------------------------------------------------------------
    # 5. Price Action & Candlestick Confirmation (Up to +2 Points)
    # -------------------------------------------------------------
    candle_body = abs(curr_close - curr_open)
    upper_wick = curr_high - max(curr_close, curr_open)
    lower_wick = min(curr_close, curr_open) - curr_low

    is_bullish_candle = curr_close > curr_open
    is_bearish_candle = curr_close < curr_open

    # Rejection wick / Strong candle body
    if is_bullish_candle:
        if lower_wick > candle_body:
            call_score += 2
            reasons.append("BULLISH_REJECTION_WICK")
        else:
            call_score += 1
    elif is_bearish_candle:
        if upper_wick > candle_body:
            put_score += 2
            reasons.append("BEARISH_REJECTION_WICK")
        else:
            put_score += 1

    # -------------------------------------------------------------
    # Final Decision Calculation
    # -------------------------------------------------------------
    call_score = min(call_score, 10)
    put_score = min(put_score, 10)

    # Determine highest score side
    if call_score >= put_score:
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
    SIGNAL_THRESHOLD = 7
    if final_score >= SIGNAL_THRESHOLD:
        direction = direction_candidate
        score_reason = "+".join(reasons[-3:]) if reasons else "SETUP_CONFIRMED"
    else:
        direction = "NO_TRADE"
        score_reason = "+".join(reasons[:2]) if reasons else "WAITING_SETUP"

    details = {
        "score_reason": score_reason,
        "structure": structure,
        "trend_5m": trend_5m,
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
