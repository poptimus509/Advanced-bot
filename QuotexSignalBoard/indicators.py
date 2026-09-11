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
    exp1 = calculate_ema(series, fast)
    exp2 = calculate_ema(series, slow)
    macd = exp1 - exp2
    signal_line = calculate_ema(macd, signal)
    hist = macd - signal_line
    return macd, signal_line, hist

def calculate_adx(df, period=14):
    if df is None or len(df) < period + 1:
        return pd.Series([0.0] * len(df) if df is not None else [])
    
    high = df['High']
    low = df['Low']
    close = df['Close']
    
    plus_dm = high.diff()
    minus_dm = low.diff()
    plus_dm = np.where((plus_dm > minus_dm) & (plus_dm > 0), plus_dm, 0.0)
    minus_dm = np.where((minus_dm > plus_dm) & (minus_dm > 0), minus_dm, 0.0)
    
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    atr = tr.rolling(window=period).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).rolling(window=period).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=df.index).rolling(window=period).mean() / atr
    
    denom = plus_di + minus_di
    denom = denom.replace(0, np.nan)
    dx = (100 * (plus_di - minus_di).abs() / denom).abs().fillna(0)
    adx = dx.rolling(window=period).mean().fillna(0)
    return adx

def detect_market_structure(df):
    """Detects Higher High / Higher Low (Bullish) or Lower High / Lower Low (Bearish) and BOS."""
    if df is None or len(df) < 5:
        return "NEUTRAL", False
    
    recent_highs = df['High'].rolling(window=3).max()
    recent_lows = df['Low'].rolling(window=3).min()
    
    is_hh_hl = (df['High'].iloc[-1] > df['High'].iloc[-3]) and (df['Low'].iloc[-1] > df['Low'].iloc[-3])
    is_lh_ll = (df['High'].iloc[-1] < df['High'].iloc[-3]) and (df['Low'].iloc[-1] < df['Low'].iloc[-3])
    
    bos_bullish = df['Close'].iloc[-1] > recent_highs.iloc[-4] if len(df) >= 4 else False
    bos_bearish = df['Close'].iloc[-1] < recent_lows.iloc[-4] if len(df) >= 4 else False
    
    if is_hh_hl or bos_bullish:
        return "BULLISH", bool(bos_bullish)
    elif is_lh_ll or bos_bearish:
        return "BEARISH", bool(bos_bearish)
    
    return "NEUTRAL", False

def detect_candlestick_patterns(df):
    """Detects Bullish/Bearish Engulfing and Rejection candles."""
    if df is None or len(df) < 2:
        return "NONE"
    
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    
    curr_body = abs(curr['Close'] - curr['Open'])
    total_range = curr['High'] - curr['Low']
    
    if total_range > 0:
        upper_wick = curr['High'] - max(curr['Open'], curr['Close'])
        lower_wick = min(curr['Open'], curr['Close']) - curr['Low']
        
        if lower_wick > (2 * curr_body) and upper_wick < curr_body and curr_body > 0:
            return "BULLISH_REJECTION"
        if upper_wick > (2 * curr_body) and lower_wick < curr_body and curr_body > 0:
            return "BEARISH_REJECTION"

    if curr['Close'] > curr['Open'] and prev['Close'] < prev['Open'] and curr['Close'] >= prev['Open'] and curr['Open'] <= prev['Close']:
        return "BULLISH_ENGULFING"
    
    if curr['Close'] < curr['Open'] and prev['Close'] > prev['Open'] and curr['Close'] <= prev['Open'] and curr['Close'] >= prev['Close']:
        return "BEARISH_ENGULFING"
        
    return "NONE"

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Backward compatibility wrapper for legacy code expecting calculate_indicators."""
    if df is None or df.empty or len(df) < 10:
        return df
    
    df = df.copy()
    df["EMA20"] = calculate_ema(df["Close"], 20)
    df["EMA50"] = calculate_ema(df["Close"], 50)
    df["RSI"] = calculate_rsi(df["Close"], 14)
    macd, signal, hist = calculate_macd(df["Close"])
    df["MACD"] = macd
    df["MACD_Signal"] = signal
    df["MACD_Hist"] = hist
    df["ADX"] = calculate_adx(df, 14)
    return df
