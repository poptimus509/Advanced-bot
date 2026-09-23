"""Shared price indicators; unavailable warm-up values remain NaN."""
import numpy as np
import pandas as pd


def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def calculate_rsi(series, period=14):
    delta = pd.to_numeric(series, errors="coerce").diff()
    gain = delta.clip(lower=0).rolling(period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).rolling(period, min_periods=period).mean()
    result = 100 - 100 / (1 + gain / loss.replace(0, np.nan))
    result = result.mask((loss == 0) & (gain > 0), 100.0)
    result = result.mask((gain == 0) & (loss > 0), 0.0)
    return result.mask((gain == 0) & (loss == 0), 50.0)


def calculate_macd(series, fast=12, slow=26, signal=9):
    macd = calculate_ema(series, fast) - calculate_ema(series, slow)
    signal_line = calculate_ema(macd, signal)
    return macd, signal_line, macd - signal_line


def calculate_adx(df, period=14):
    if df is None:
        return pd.Series(dtype=float)
    high = df["high" if "high" in df else "High"]
    low = df["low" if "low" in df else "Low"]
    close = df["close" if "close" in df else "Close"]
    up = high.diff()
    down = -low.diff()
    plus = up.where((up > down) & (up > 0), 0.0)
    minus = down.where((down > up) & (down > 0), 0.0)
    tr = pd.concat([high-low, (high-close.shift()).abs(),
                    (low-close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(period, min_periods=period).mean()
    plus_di = 100 * plus.rolling(period).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus.rolling(period).mean() / atr.replace(0, np.nan)
    denominator = plus_di + minus_di
    dx = 100 * (plus_di-minus_di).abs() / denominator.replace(0, np.nan)
    dx = dx.mask((denominator == 0) | (atr == 0), 0.0)
    return dx.rolling(period, min_periods=period).mean()
