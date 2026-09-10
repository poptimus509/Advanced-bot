import pandas as pd
import numpy as np

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 30:
        return df
    
    df["EMA20"] = df["Close"].ewm(span=20, adjust=False).mean()
    df["EMA50"] = df["Close"].ewm(span=50, adjust=False).mean()
    
    delta = df["Close"].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=13, adjust=False).mean()
    ema_down = down.ewm(com=13, adjust=False).mean()
    rs = ema_up / ema_down
    df["RSI"] = 100 - (100 / (1 + rs))
    
    exp12 = df["Close"].ewm(span=12, adjust=False).mean()
    exp26 = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = exp12 - exp26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]
    
    high_diff = df["High"].diff()
    low_diff = -df["Low"].diff()
    df["+DM"] = np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0.0)
    df["-DM"] = np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0.0)
    
    tr1 = df["High"] - df["Low"]
    tr2 = abs(df["High"] - df["Close"].shift(1))
    tr3 = abs(df["Low"] - df["Close"].shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()
    
    atr14 = df["ATR"]
    plus_di = 100 * (df["+DM"].ewm(alpha=1/14, adjust=False).mean() / atr14)
    minus_di = 100 * (df["-DM"].ewm(alpha=1/14, adjust=False).mean() / atr14)
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    df["ADX"] = dx.ewm(alpha=1/14, adjust=False).mean()
    
    return df