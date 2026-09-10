import pandas as pd
from indicators import calculate_indicators
from config import SIGNAL_THRESHOLD_CALL_PUT, SIGNAL_THRESHOLD_WATCH

def evaluate_strategy(df_5m: pd.DataFrame, df_15m: pd.DataFrame):
    if df_5m.empty or len(df_5m) < 30 or df_15m.empty or len(df_15m) < 20:
        return "NO_TRADE", 0, "A", {"trend": "NEUTRAL", "bias": "NEUTRAL", "structure": "NONE", "pa": "NONE"}

    df_5m = calculate_indicators(df_5m)
    df_15m = calculate_indicators(df_15m)

    curr_close = df_5m["Close"].iloc[-1]
    ema20 = df_5m["EMA20"].iloc[-1]
    ema50 = df_5m["EMA50"].iloc[-1]
    rsi = df_5m["RSI"].iloc[-1]
    macd_hist = df_5m["MACD_Hist"].iloc[-1]
    adx = df_5m["ADX"].iloc[-1]

    htf_close = df_15m["Close"].iloc[-1]
    htf_ema20 = df_15m["EMA20"].iloc[-1]
    bias_15m = "BULLISH" if htf_close > htf_ema20 else "BEARISH"

    bullish_score = 0
    bearish_score = 0

    if ema20 > ema50:
        bullish_score += 2
    elif ema20 < ema50:
        bearish_score += 2

    if bias_15m == "BULLISH":
        bullish_score += 2
    else:
        bearish_score += 2

    if rsi > 55 and macd_hist > 0:
        bullish_score += 2
    elif rsi < 45 and macd_hist < 0:
        bearish_score += 2

    if adx > 25:
        bullish_score += 1
        bearish_score += 1

    prev_open = df_5m["Open"].iloc[-2]
    prev_close = df_5m["Close"].iloc[-2]
    curr_open = df_5m["Open"].iloc[-1]
    
    body = abs(curr_close - curr_open)
    lower_min = min(curr_open, curr_close) - df_5m["Low"].iloc[-1]
    upper_max = df_5m["High"].iloc[-1] - max(curr_open, curr_close)

    bullish_engulfing = (prev_close < prev_open) and (curr_close > curr_open) and (curr_close > prev_open) and (curr_open < prev_close)
    bearish_engulfing = (prev_close > prev_open) and (curr_close < curr_open) and (curr_close < prev_open) and (curr_open > prev_close)
    bullish_pinbar = (lower_min > (2 * body)) and (upper_max < (body * 0.5))
    bearish_pinbar = (upper_max > (2 * body)) and (lower_min < (body * 0.5))

    if bullish_engulfing or bullish_pinbar:
        bullish_score += 2
    if bearish_engulfing or bearish_pinbar:
        bearish_score += 2

    recent_high = df_5m["High"].iloc[-5:-1].max()
    recent_low = df_5m["Low"].iloc[-5:-1].min()
    structure = "BOS" if curr_close > recent_high or curr_close < recent_low else "RANGE"
    if structure == "BOS":
        if curr_close > recent_high:
            bullish_score += 1
        else:
            bearish_score += 1

    final_score = max(bullish_score, bearish_score)
    
    if final_score >= SIGNAL_THRESHOLD_CALL_PUT:
        direction = "CALL" if bullish_score > bearish_score else "PUT"
    elif final_score >= SIGNAL_THRESHOLD_WATCH:
        direction = "WATCH"
    else:
        direction = "NO_TRADE"

    quality = "A+" if final_score >= 10 else ("A" if final_score >= 9 else "B")
    details = {
        "trend": "Bullish" if ema20 > ema50 else "Bearish",
        "bias": bias_15m,
        "structure": structure,
        "pa": "Engulfing/PinBar" if (bullish_engulfing or bearish_engulfing or bullish_pinbar or bearish_pinbar) else "None"
    }

    return direction, final_score, quality, details