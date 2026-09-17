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
    """
    Accepts a dataframe with lowercase OHLC columns ('open', 'high',
    'low', 'close'), matching the rest of the codebase (candle_manager
    and strategy.py both use lowercase). Falls back to capitalized
    column names for backward compatibility with any external caller
    still using them.
    """
    if df is None or len(df) < period + 1:
        return pd.Series([0.0] * len(df) if df is not None else [])

    def col(name_lower, name_upper):
        if name_lower in df.columns:
            return df[name_lower]
        return df[name_upper]

    high = col('high', 'High')
    low = col('low', 'Low')
    close = col('close', 'Close')
    
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

# NOTE: this module previously also contained detect_market_structure()
# and detect_candlestick_patterns() - a second, capitalized-column
# implementation of exactly what strategy.py's confirmed_swings() and
# candle_reaction() do (in lowercase columns, and covered by unit
# tests). Two parallel implementations of the same logic is how the
# original bot ended up with a bugged "5M trend" that was silently
# derived from 1M data: nobody could tell which function actually ran
# in production. Those two functions and the unused calculate_indicators()
# wrapper have been removed; strategy.py is now the single source of
# truth for structure/pattern detection. calculate_adx() remains here
# because bot.py uses it directly for the 5M ADX gate.
