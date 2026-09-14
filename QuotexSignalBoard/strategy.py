import numpy as np
import pandas as pd

# ============================================================
# INDICATORS
# ============================================================

def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    rsi = rsi.where(avg_loss != 0, 100)
    rsi = rsi.where(avg_gain != 0, 0)
    return rsi

def calculate_macd(series, fast=12, slow=26, signal=9):
    exp_fast = series.ewm(span=fast, adjust=False).mean()
    exp_slow = series.ewm(span=slow, adjust=False).mean()

    macd_line = exp_fast - exp_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_adx(df, period=14):
    if df is None or len(df) < period + 1:
        return pd.Series(25.0, index=df.index if df is not None else [])

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index)

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()

    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = true_range.rolling(window=period, min_periods=period).mean()

    plus_di_series = 100 * plus_dm.rolling(period, min_periods=period).mean() / atr.replace(0, np.nan)
    minus_di_series = 100 * minus_dm.rolling(period, min_periods=period).mean() / atr.replace(0, np.nan)

    di_sum = (plus_di_series + minus_di_series).replace(0, np.nan)
    dx = 100 * (plus_di_series - minus_di_series).abs() / di_sum

    adx = dx.rolling(window=period, min_periods=period).mean()
    return adx.fillna(25.0)

def calculate_directional_strength(df, period=14):
    if df is None or len(df) < period + 1:
        return 0.0, 0.0, "NEUTRAL"

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index)

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()

    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = true_range.rolling(window=period, min_periods=period).mean()

    plus_di_series = 100 * plus_dm.rolling(period, min_periods=period).mean() / atr.replace(0, np.nan)
    minus_di_series = 100 * minus_dm.rolling(period, min_periods=period).mean() / atr.replace(0, np.nan)

    plus_di = float(plus_di_series.iloc[-1]) if not pd.isna(plus_di_series.iloc[-1]) else 0.0
    minus_di = float(minus_di_series.iloc[-1]) if not pd.isna(minus_di_series.iloc[-1]) else 0.0

    if plus_di > minus_di:
        direction = "BULLISH"
    elif minus_di > plus_di:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"

    return plus_di, minus_di, direction

# ============================================================
# MARKET STRUCTURE & CANDLESTICK
# ============================================================

def detect_bos(df, lookback=5):
    if df is None or len(df) < lookback + 1:
        return "NEUTRAL"

    recent_high = df["high"].iloc[-lookback - 1:-1].max()
    recent_low = df["low"].iloc[-lookback - 1:-1].min()
    current_close = df["close"].iloc[-1]

    if current_close > recent_high:
        return "BULLISH_BOS"
    if current_close < recent_low:
        return "BEARISH_BOS"
    return "NEUTRAL"

def detect_candlestick_pattern(df):
    if df is None or len(df) < 2:
        return "NEUTRAL"

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    open_price = float(curr["open"])
    high = float(curr["high"])
    low = float(curr["low"])
    close = float(curr["close"])

    candle_range = high - low
    if candle_range <= 0:
        return "NEUTRAL"

    body = abs(close - open_price)
    upper_wick = high - max(open_price, close)
    lower_wick = min(open_price, close) - low
    body_ratio = body / candle_range

    bullish = close > open_price
    bearish = close < open_price

    # Solid breakout momentum
    if bullish and body_ratio >= 0.50 and close >= float(prev["high"]):
        return "BULLISH"
    if bearish and body_ratio >= 0.50 and close <= float(prev["low"]):
        return "BEARISH"

    # Rejection Pinbars
    if bullish and lower_wick >= body * 1.3 and lower_wick > upper_wick:
        return "BULLISH"
    if bearish and upper_wick >= body * 1.3 and upper_wick > lower_wick:
        return "BEARISH"

    # Engulfing pattern
    prev_open = float(prev["open"])
    prev_close = float(prev["close"])
    if bullish and prev_close < prev_open and close >= prev_open:
        return "BULLISH"
    if bearish and prev_close > prev_open and close <= prev_open:
        return "BEARISH"

    return "NEUTRAL"

def _get_epoch(index_value):
    try:
        if hasattr(index_value, "timestamp"):
            return int(index_value.timestamp())
        return int(index_value)
    except Exception:
        return 0

# ============================================================
# MAIN STRATEGY
# ============================================================

def evaluate_strategy(df_1m, df_5m, df_15m):
    details = {
        "bias": "NEUTRAL",
        "trend": "NEUTRAL",
        "structure": "NEUTRAL",
        "pa": "NEUTRAL",
        "score_reason": "",
        "signal_candle_epoch": None,
        "threshold_used": 7,
        "timeframe_alignment": "NEUTRAL",
        "adx_5m": 0.0,
        "rsi_1m": 50.0,
        "macd_momentum": "NEUTRAL",
        "ema_alignment": "NEUTRAL"
    }

    if df_1m is None or df_5m is None or len(df_1m) < 25 or len(df_5m) < 20:
        return ("NO_TRADE", 0, "INSUFFICIENT_DATA", details)

    df_1m.columns = [str(col).lower() for col in df_1m.columns]
    df_5m.columns = [str(col).lower() for col in df_5m.columns]
    if df_15m is not None:
        df_15m.columns = [str(col).lower() for col in df_15m.columns]

    latest_1m = df_1m.iloc[-1]
    details["signal_candle_epoch"] = _get_epoch(latest_1m.name)

    # 1. 15M BIAS (+1)
    bias_15m = "NEUTRAL"
    if df_15m is not None and len(df_15m) >= 15:
        close_15m = float(df_15m["close"].iloc[-1])
        ema20_15m = float(calculate_ema(df_15m["close"], 20).iloc[-1])
        if ema20_15m != 0:
            if close_15m > ema20_15m:
                bias_15m = "BULLISH"
            elif close_15m < ema20_15m:
                bias_15m = "BEARISH"
    details["bias"] = bias_15m

    # 2. 5M DIRECTION (+2)
    ema20_5m = float(calculate_ema(df_5m["close"], 20).iloc[-1])
    ema50_5m = float(calculate_ema(df_5m["close"], 50).iloc[-1])
    structure_5m = detect_bos(df_5m, lookback=5)
    details["structure"] = structure_5m

    adx_series = calculate_adx(df_5m, period=14)
    adx_5m = float(adx_series.iloc[-1]) if not pd.isna(adx_series.iloc[-1]) else 25.0
    plus_di, minus_di, di_direction = calculate_directional_strength(df_5m, period=14)
    details["adx_5m"] = round(adx_5m, 2)

    if ema20_5m > ema50_5m:
        trend_5m = "BULLISH"
    elif ema20_5m < ema50_5m:
        trend_5m = "BEARISH"
    else:
        trend_5m = "NEUTRAL"
    details["trend"] = trend_5m

    # 3. 1M ENTRY INDICATORS
    ema9_1m = float(calculate_ema(df_1m["close"], 9).iloc[-1])
    ema21_1m = float(calculate_ema(df_1m["close"], 21).iloc[-1])
    rsi_series = calculate_rsi(df_1m["close"], 14)
    rsi_14_1m = float(rsi_series.iloc[-1]) if not pd.isna(rsi_series.iloc[-1]) else 50.0
    details["rsi_1m"] = round(rsi_14_1m, 1)

    _, _, macd_hist = calculate_macd(df_1m["close"])
    curr_hist = float(macd_hist.iloc[-1])
    prev_hist = float(macd_hist.iloc[-2])
    pa_1m = detect_candlestick_pattern(df_1m)
    details["pa"] = pa_1m

    # 1M EMA (+2)
    ema_direction = "BULLISH" if ema9_1m > ema21_1m else ("BEARISH" if ema9_1m < ema21_1m else "NEUTRAL")
    details["ema_alignment"] = ema_direction

    # MACD (+1)
    if curr_hist > 0 and curr_hist >= prev_hist:
        macd_direction = "BULLISH"
    elif curr_hist < 0 and curr_hist <= prev_hist:
        macd_direction = "BEARISH"
    else:
        macd_direction = "NEUTRAL"
    details["macd_momentum"] = macd_direction

    # 4. SCORING
    call_score, put_score = 0, 0
    call_reasons, put_reasons = [], []

    # 15M (+1)
    if bias_15m == "BULLISH":
        call_score += 1
        call_reasons.append("15M Bullish")
    elif bias_15m == "BEARISH":
        put_score += 1
        put_reasons.append("15M Bearish")

    # 5M Trend (+2)
    if trend_5m == "BULLISH":
        call_score += 2
        call_reasons.append("5M Trend Bullish")
    elif trend_5m == "BEARISH":
        put_score += 2
        put_reasons.append("5M Trend Bearish")

    # 5M ADX/DI (+1)
    if adx_5m >= 18:
        if di_direction == "BULLISH":
            call_score += 1
            call_reasons.append("5M DI+ Bullish")
        elif di_direction == "BEARISH":
            put_score += 1
            put_reasons.append("5M DI- Bearish")

    # 1M EMA (+2)
    if ema_direction == "BULLISH":
        call_score += 2
        call_reasons.append("1M EMA9>21")
    elif ema_direction == "BEARISH":
        put_score += 2
        put_reasons.append("1M EMA9<21")

    # 1M RSI (+1)
    if 48 <= rsi_14_1m <= 75:
        call_score += 1
        call_reasons.append(f"RSI Bullish ({rsi_14_1m:.0f})")
    elif 25 <= rsi_14_1m <= 52:
        put_score += 1
        put_reasons.append(f"RSI Bearish ({rsi_14_1m:.0f})")

    # 1M MACD (+1)
    if macd_direction == "BULLISH":
        call_score += 1
        call_reasons.append("MACD Bullish")
    elif macd_direction == "BEARISH":
        put_score += 1
        put_reasons.append("MACD Bearish")

    # 1M Price Action (+2)
    if pa_1m == "BULLISH":
        call_score += 2
        call_reasons.append("1M PA Bullish")
    elif pa_1m == "BEARISH":
        put_score += 2
        put_reasons.append("1M PA Bearish")

    # 5. DETERMINE DIRECTION & THRESHOLD
    if call_score > put_score:
        direction = "CALL"
        final_score = call_score
        reasons = call_reasons
    elif put_score > call_score:
        direction = "PUT"
        final_score = put_score
        reasons = put_reasons
    else:
        details["score_reason"] = f"Equal: CALL={call_score}, PUT={put_score}"
        return ("NO_TRADE", max(call_score, put_score), "NO_TRADE", details)

    # হাই-অ্যাকিউরেসি ফিল্টারিংয়ের জন্য থ্রেশহোল্ড ৭
    threshold = 7
    details["threshold_used"] = threshold

    if final_score < threshold:
        details["score_reason"] = f"{direction} score {final_score}/10 below threshold {threshold}"
        return ("NO_TRADE", final_score, "LOW_SCORE", details)

    quality = "A+" if final_score >= 9 else ("A" if final_score >= 8 else "B+")
    details["score_reason"] = f"{direction} {final_score}/10 | " + ", ".join(reasons)

    return (direction, final_score, quality, details)
