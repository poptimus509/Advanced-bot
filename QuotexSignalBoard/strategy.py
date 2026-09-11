import pandas as pd
from indicators import (
    calculate_ema, calculate_rsi, calculate_macd, 
    calculate_adx, detect_market_structure, detect_candlestick_patterns
)
from config import SIGNAL_THRESHOLD_CALL_PUT, SIGNAL_THRESHOLD_WATCH

def evaluate_strategy(arg1, arg2=None, arg3=None):
    """
    Evaluates 1M, 5M, and 15M data for high-selectivity binary options signals.
    Supports backward compatibility and startup graceful fallback.
    """
    df_1m = None
    df_5m = None
    df_15m = None

    if arg3 is not None:
        df_1m, df_5m, df_15m = arg1, arg2, arg3
    elif arg2 is not None:
        df_5m, df_15m = arg1, arg2
        df_1m = df_5m
    else:
        df_5m = arg1

    if df_5m is None or len(df_5m) < 10:
        return "NO_TRADE", 0, "D", {"trend": "NEUTRAL", "bias": "NEUTRAL", "structure": "NONE", "pa": "NONE"}

    # 15M Bias evaluation with startup fallback (min 10 candles instead of 50)
    bias_15m = "NEUTRAL"
    if df_15m is not None and len(df_15m) >= 10:
        span_val = min(20, len(df_15m) // 2)
        ema_fast = calculate_ema(df_15m['Close'], span_val).iloc[-1]
        ema_slow = calculate_ema(df_15m['Close'], len(df_15m)).iloc[-1] if len(df_15m) >= 30 else ema_fast
        htf_close = df_15m['Close'].iloc[-1]
        
        if htf_close >= ema_fast:
            bias_15m = "BULLISH"
        else:
            bias_15m = "BEARISH"
    elif df_5m is not None and len(df_5m) >= 15:
        ema_fast = calculate_ema(df_5m['Close'], 10).iloc[-1]
        ema_slow = calculate_ema(df_5m['Close'], 20).iloc[-1] if len(df_5m) >= 20 else ema_fast
        bias_15m = "BULLISH" if ema_fast >= ema_slow else "BEARISH"
    else:
        bias_15m = "BULLISH" # Default safe bias during very early startup

    m_struct, bos = detect_market_structure(df_5m)
    adx_series = calculate_adx(df_5m, 14)
    adx_val = adx_series.iloc[-1] if not adx_series.empty else 25.0 # Default fallback if calculating
    
    ema_20_5m = calculate_ema(df_5m['Close'], min(10, len(df_5m))).iloc[-1]
    ema_50_5m = calculate_ema(df_5m['Close'], min(25, len(df_5m))).iloc[-1]
    trend_5m = "BULLISH" if ema_20_5m >= ema_50_5m else "BEARISH"

    source_1m = df_1m if df_1m is not None and len(df_1m) >= 10 else df_5m
    ema_9 = calculate_ema(source_1m['Close'], min(5, len(source_1m))).iloc[-1]
    ema_21 = calculate_ema(source_1m['Close'], min(15, len(source_1m))).iloc[-1]
    
    pa_pattern = detect_candlestick_patterns(source_1m)
    
    rsi = calculate_rsi(source_1m['Close'], 14).iloc[-1] if len(source_1m) >= 14 else 50.0
    _, _, macd_hist = calculate_macd(source_1m['Close'])
    macd_hist_val = macd_hist.iloc[-1] if not macd_hist.empty else 0.0

    score = 0
    bullish_score = 0
    bearish_score = 0

    if bias_15m == "BULLISH":
        if ema_9 >= ema_21:
            bullish_score += 2
        if pa_pattern in ["BULLISH_ENGULFING", "BULLISH_REJECTION"]:
            bullish_score += 3
        elif pa_pattern == "BEARISH_ENGULFING":
            bullish_score -= 3
            
        if 40 <= rsi <= 80:
            bullish_score += 1
        if macd_hist_val >= 0:
            bullish_score += 1
        if bos or trend_5m == "BULLISH":
            bullish_score += 2
            
        score = bullish_score
        direction = "CALL" if score >= SIGNAL_THRESHOLD_CALL_PUT else "NO_TRADE"

    elif bias_15m == "BEARISH":
        if ema_9 <= ema_21:
            bearish_score += 2
        if pa_pattern in ["BEARISH_ENGULFING", "BEARISH_REJECTION"]:
            bearish_score += 3
        elif pa_pattern == "BULLISH_ENGULFING":
            bearish_score -= 3
            
        if 20 <= rsi <= 60:
            bearish_score += 1
        if macd_hist_val <= 0:
            bearish_score += 1
        if bos or trend_5m == "BEARISH":
            bearish_score += 2
            
        score = bearish_score
        direction = "PUT" if score >= SIGNAL_THRESHOLD_CALL_PUT else "NO_TRADE"
    else:
        direction = "NO_TRADE"

    if direction == "NO_TRADE" or score < SIGNAL_THRESHOLD_CALL_PUT:
        direction = "NO_TRADE"

    quality = "A+" if score >= 8 else ("A" if score >= 6 else "B")
    details = {
        "trend": trend_5m,
        "bias": bias_15m,
        "structure": m_struct,
        "pa": pa_pattern if pa_pattern != "NONE" else "Trend/Pullback"
    }

    return direction, score, quality, details
