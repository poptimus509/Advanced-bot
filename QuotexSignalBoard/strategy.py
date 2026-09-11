import pandas as pd
import numpy as np

def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_macd(series, fast=12, slow=26, signal=9):
    exp1 = series.ewm(span=fast, adjust=False).mean()
    exp2 = series.ewm(span=slow, adjust=False).mean()
    macd_line = exp1 - exp2
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_adx(df, period=14):
    if len(df) < period + 1:
        return pd.Series(25, index=df.index) # Default neutral ADX if insufficient data
    high = df['high']
    low = df['low']
    close = df['close']
    
    plus_dm = high.diff()
    minus_dm = low.diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm < 0] = 0
    
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    atr = tr.rolling(window=period).mean()
    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)
    
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.rolling(window=period).mean().fillna(25)
    return adx

def detect_bos(df):
    """Detects basic Market Structure / Break of Structure (BOS)"""
    if len(df) < 5:
        return "NEUTRAL"
    recent_high = df['high'].iloc[-5:-1].max()
    recent_low = df['low'].iloc[-5:-1].min()
    current_close = df['close'].iloc[-1]
    
    if current_close > recent_high:
        return "BULLISH_BOS"
    elif current_close < recent_low:
        return "BEARISH_BOS"
    return "NEUTRAL"

def detect_candlestick_pattern(df):
    """Detects basic reliable candlestick momentum patterns on the latest closed candle"""
    if len(df) < 2:
        return "NEUTRAL"
    
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    
    curr_body = abs(curr['close'] - curr['open'])
    curr_range = curr['high'] - curr['low']
    
    if curr_range == 0:
        return "NEUTRAL"
        
    is_bullish_candle = curr['close'] > curr['open']
    is_bearish_candle = curr['close'] < curr['open']
    
    # Strong body check (body is > 50% of total range)
    strong_body = (curr_body / curr_range) > 0.5
    
    # Bullish Engulfing or Strong Bullish bar
    if is_bullish_candle and strong_body:
        if curr['close'] > prev['high']:
            return "BULLISH"
            
    # Bearish Engulfing or Strong Bearish bar
    if is_bearish_candle and strong_body:
        if curr['close'] < prev['low']:
            return "BEARISH"
            
    return "NEUTRAL"

def evaluate_strategy(df_1m, df_5m, df_15m):
    """
    Evaluates 1-minute trading strategy based on 1M, 5M, and 15M timeframes.
    15M acts as a Soft Bias, 5M as Main Trend/Structure, 1M as Entry Trigger.
    """
    details = {
        "bias": "NEUTRAL",
        "trend": "NEUTRAL",
        "structure": "NEUTRAL",
        "pa": "NEUTRAL",
        "score_reason": "",
        "signal_candle_epoch": None,
        "threshold_used": 7,
        "timeframe_alignment": "NEUTRAL"
    }
    
    # Check data sufficiency
    if df_1m is None or len(df_1m) < 30 or df_5m is None or len(df_5m) < 30:
        return "NO_TRADE", 0, "INSUFFICIENT_DATA", details
        
    # Set signal candle epoch (using the latest closed 1M candle index/timestamp if available)
    latest_1m = df_1m.iloc[-1]
    details["signal_candle_epoch"] = int(latest_1m.name.timestamp()) if hasattr(latest_1m.name, 'timestamp') else 0

    # 1. 15M SOFT MARKET BIAS EVALUATION
    bias_15m = "NEUTRAL"
    if df_15m is not None and len(df_15m) >= 20:
        close_15m = df_15m['close'].iloc[-1]
        ema20_15m = calculate_ema(df_15m['close'], 20).iloc[-1]
        
        # Buffer to check if close is very close to EMA20 (Neutral zone)
        diff_pct = abs(close_15m - ema20_15m) / ema20_15m
        if diff_pct < 0.0005:  # Very close proximity
            bias_15m = "NEUTRAL"
        elif close_15m > ema20_15m:
            bias_15m = "BULLISH"
        elif close_15m < ema20_15m:
            bias_15m = "BEARISH"
    details["bias"] = bias_15m

    # 2. 5M MAIN TREND + STRUCTURE CONFIRMATION
    ema20_5m = calculate_ema(df_5m['close'], 20).iloc[-1]
    ema50_5m = calculate_ema(df_5m['close'], 50).iloc[-1]
    adx_5m = calculate_adx(df_5m).iloc[-1]
    structure_5m = detect_bos(df_5m)
    details["structure"] = structure_5m

    is_5m_bullish = (ema20_5m >= ema50_5m) or (structure_5m == "BULLISH_BOS")
    is_5m_bearish = (ema20_5m <= ema50_5m) or (structure_5m == "BEARISH_BOS")
    
    # Conflict check between 5M and 1M setup requirements
    # We evaluate 1M triggers first to check alignment
    ema9_1m = calculate_ema(df_1m['close'], 9).iloc[-1]
    ema21_1m = calculate_ema(df_1m['close'], 21).iloc[-1]
    rsi_14_1m = calculate_rsi(df_1m['close'], 14).iloc[-1]
    _, _, macd_hist_1m = calculate_macd(df_1m['close'])
    curr_hist = macd_hist_1m.iloc[-1]
    prev_hist = macd_hist_1m.iloc[-2] if len(macd_hist_1m) > 1 else 0
    pa_1m = detect_candlestick_pattern(df_1m)
    details["pa"] = pa_1m

    is_1m_bullish = (ema9_1m > ema21_1m)
    is_1m_bearish = (ema9_1m < ema21_1m)

    # Hard Conflict Rule: 5M bullish & 1M bearish OR 5M bearish & 1M bullish -> NO_TRADE
    if (is_5m_bullish and not is_5m_bearish) and is_1m_bearish and not is_1m_bullish:
        details["score_reason"] = "Conflict between 5M bullish and 1M bearish."
        return "NO_TRADE", 0, "CONFLICT", details
    if (is_5m_bearish and not is_5m_bullish) and is_1m_bullish and not is_1m_bearish:
        details["score_reason"] = "Conflict between 5M bearish and 1M bullish."
        return "NO_TRADE", 0, "CONFLICT", details

    # 3. SCORING SYSTEM (Total Max = 10)
    call_score = 0
    put_score = 0

    # --- CALL SCORING ---
    # 15M supportive bias (+1 if bullish)
    if bias_15m == "BULLISH":
        call_score += 1
    # 5M bullish trend/structure (+2)
    if is_5m_bullish:
        call_score += 2
    # 5M ADX / strength confirmation (+1 if ADX >= 20)
    if adx_5m >= 20:
        call_score += 1
    # 1M EMA9 > EMA21 (+2)
    if ema9_1m > ema21_1m:
        call_score += 2
    # 1M RSI bullish momentum (RSI between 50 and 75, avoiding extreme overbought > 75)
    if 50 < rsi_14_1m < 75:
        call_score += 1
    # 1M MACD bullish histogram & improving momentum
    if curr_hist > 0 and curr_hist > prev_hist:
        call_score += 1
    # Bullish candlestick pattern (+2)
    if pa_1m == "BULLISH":
        call_score += 2

    # --- PUT SCORING ---
    # 15M supportive bias (+1 if bearish)
    if bias_15m == "BEARISH":
        put_score += 1
    # 5M bearish trend/structure (+2)
    if is_5m_bearish:
        put_score += 2
    # 5M ADX / strength confirmation (+1 if ADX >= 20)
    if adx_5m >= 20:
        put_score += 1
    # 1M EMA9 < EMA21 (+2)
    if ema9_1m < ema21_1m:
        put_score += 2
    # 1M RSI bearish momentum (RSI between 25 and 50, avoiding extreme oversold < 25)
    if 25 < rsi_14_1m < 50:
        put_score += 1
    # 1M MACD bearish histogram & improving downward momentum
    if curr_hist < 0 and curr_hist < prev_hist:
        put_score += 1
    # Bearish candlestick pattern (+2)
    if pa_1m == "BEARISH":
        put_score += 2

    # 4. DETERMINE THRESHOLD BASED ON 15M BIAS ALIGNMENT
    # If 15M is opposite to trade direction, minimum score required is 8 (Counter-bias trade)
    # If 15M is aligned or neutral, minimum score required is 7 (Normal aligned trade)
    
    # Check Call Setup
    if call_score > put_score and call_score >= 7:
        if bias_15m == "BEARISH":
            # Counter-15M-bias trade requires higher threshold (8/10)
            threshold = 8
            details["timeframe_alignment"] = "COUNTER_BIAS"
        else:
            threshold = 7
            details["timeframe_alignment"] = "ALIGNED_OR_NEUTRAL"
            
        details["threshold_used"] = threshold
        if call_score >= threshold:
            quality = "A+" if call_score >= 9 else ("A" if call_score == 8 else "B+")
            details["score_reason"] = f"CALL setup met with score {call_score}/10 (Threshold: {threshold})"
            return "CALL", call_score, quality, details
        else:
            details["score_reason"] = f"CALL score {call_score} below counter-bias threshold {threshold}."
            return "NO_TRADE", call_score, "LOW_SCORE", details

    # Check Put Setup
    elif put_score > call_score and put_score >= 7:
        if bias_15m == "BULLISH":
            # Counter-15M-bias trade requires higher threshold (8/10)
            threshold = 8
            details["timeframe_alignment"] = "COUNTER_BIAS"
        else:
            threshold = 7
            details["timeframe_alignment"] = "ALIGNED_OR_NEUTRAL"
            
        details["threshold_used"] = threshold
        if put_score >= threshold:
            quality = "A+" if put_score >= 9 else ("A" if put_score == 8 else "B+")
            details["score_reason"] = f"PUT setup met with score {put_score}/10 (Threshold: {threshold})"
            return "PUT", put_score, quality, details
        else:
            details["score_reason"] = f"PUT score {put_score} below counter-bias threshold {threshold}."
            return "NO_TRADE", put_score, "LOW_SCORE", details

    # Default No Trade
    max_score = max(call_score, put_score)
    details["score_reason"] = f"Score too low or mixed signals. Max score: {max_score}"
    return "NO_TRADE", max_score, "NO_TRADE", details
