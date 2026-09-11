import pandas as pd
from indicators import (
    calculate_ema, calculate_rsi, calculate_macd, 
    calculate_adx, detect_market_structure, detect_candlestick_patterns
)
from config import SIGNAL_THRESHOLD_CALL_PUT, SIGNAL_THRESHOLD_WATCH

def evaluate_strategy(arg1, arg2=None, arg3=None):
    """
    Evaluates 1M, 5M, and 15M data for high-selectivity binary options signals.
    Supports backward compatibility:
    - evaluate_strategy(df_1m, df_5m, df_15m)
    - evaluate_strategy(df_5m, df_15m)
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

    if df_5m is None or len(df_5m) < 30:
        return "NO_TRADE", 0, "D", {"trend": "NEUTRAL", "bias": "NEUTRAL", "structure": "NONE", "pa": "NONE"}

    bias_15m = "NEUTRAL"
    if df_15m is not None and len(df_15m) >= 50:
        ema_50_15m = calculate_ema(df_15m['Close'], 50).iloc[-1]
        ema_200_15m = calculate_ema(df_15m['Close'], 200) if len(df_15m) >= 200 else calculate_ema(df_15m['Close'], len(df_15m)//2)
        ema_200_val = ema_200_15m.iloc[-1]
        htf_close = df_15m['Close'].iloc[-1]
        
        if ema_50_15m > ema_200_val and htf_close > ema_50_15m:
            bias_15m = "BULLISH"
        elif ema_50_15m < ema_200_val and htf_close < ema_50_15m:
            bias_15m = "BEARISH"
    elif df_5m is not None and len(df_5m) >= 50:
        ema_20_5m = calculate_ema(df_5m['Close'], 20).iloc[-1]
        ema_50_5m = calculate_ema(df_5m['Close'], 50).iloc[-1]
        bias_15m = "BULLISH" if ema_20_5m > ema_50_5m else "BEARISH"

    if bias_15m == "NEUTRAL":
        return "NO_TRADE", 0, "D", {"trend": "NEUTRAL", "bias": "NEUTRAL", "structure": "NONE", "pa": "NONE"}

    m_struct, bos = detect_market_structure(df_5m)
    adx_series = calculate_adx(df_5m, 14)
    adx_val = adx_series.iloc[-1] if not adx_series.empty else 0
    
    ema_20_5m = calculate_ema(df_5m['Close'], 20).iloc[-1]
    ema_50_5m = calculate_ema(df_5m['Close'], 50).iloc[-1]
    trend_5m = "BULLISH" if ema_20_5m > ema_50_5m else "BEARISH"

    if trend_5m != bias_15m or m_struct != bias_15m or adx_val < 20:
        return "NO_TRADE", 0, "D", {"trend": trend_5m, "bias": bias_15m, "structure": m_struct, "pa": "NO_ALIGNMENT"}

    source_1m = df_1m if df_1m is not None and len(df_1m) >= 20 else df_5m
    ema_9 = calculate_ema(source_1m['Close'], 9).iloc[-1]
    ema_21 = calculate_ema(source_1m['Close'], 21).iloc[-1]
    
    pa_pattern = detect_candlestick_patterns(source_1m)
    
    rsi = calculate_rsi(source_1m['Close'], 14).iloc[-1]
    _, _, macd_hist = calculate_macd(source_1m['Close'])
    macd_hist_val = macd_hist.iloc[-1] if not macd_hist.empty else 0

    score = 0
    bullish_score = 0
    bearish_score = 0

    if bias_15m == "BULLISH":
        if ema_9 > ema_21:
            bullish_score += 2
        if pa_pattern in ["BULLISH_ENGULFING", "BULLISH_REJECTION"]:
            bullish_score += 3
        elif pa_pattern == "BEARISH_ENGULFING":
            bullish_score -= 3
            
        if 45 <= rsi <= 75:
            bullish_score += 1
        if macd_hist_val > 0:
            bullish_score += 1
        if bos:
            bullish_score += 1
            
        score = bullish_score
        direction = "CALL" if score >= SIGNAL_THRESHOLD_CALL_PUT and ema_9 > ema_21 and pa_pattern != "BEARISH_ENGULFING" else "NO_TRADE"

    elif bias_15m == "BEARISH":
        if ema_9 < ema_21:
            bearish_score += 2
        if pa_pattern in ["BEARISH_ENGULFING", "BEARISH_REJECTION"]:
            bearish_score += 3
        elif pa_pattern == "BULLISH_ENGULFING":
            bearish_score -= 3
            
        if 25 <= rsi <= 55:
            bearish_score += 1
        if macd_hist_val < 0:
            bearish_score += 1
        if bos:
            bearish_score += 1
            
        score = bearish_score
        direction = "PUT" if score >= SIGNAL_THRESHOLD_CALL_PUT and ema_9 < ema_21 and pa_pattern != "BULLISH_ENGULFING" else "NO_TRADE"
    else:
        direction = "NO_TRADE"

    if direction == "NO_TRADE" or score < SIGNAL_THRESHOLD_CALL_PUT:
        direction = "NO_TRADE"

    quality = "A+" if score >= 9 else ("A" if score >= 7 else "B")
    details = {
        "trend": trend_5m,
        "bias": bias_15m,
        "structure": m_struct,
        "pa": pa_pattern if pa_pattern != "NONE" else "Trend/Pullback"
    }

    return direction, score, quality, details
