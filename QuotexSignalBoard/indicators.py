import numpy as np
import pandas as pd


def calculate_ema(series, period):
    return pd.to_numeric(series, errors="coerce").ewm(span=period, adjust=False).mean()


def _wilder_rma(series: pd.Series, period: int) -> pd.Series:
    """Wilder's smoothing (RMA/SMMA), equivalent to alpha=1/period."""
    s = pd.to_numeric(series, errors="coerce").astype(float)
    return s.ewm(alpha=1.0 / float(period), adjust=False, min_periods=period).mean()


def calculate_rsi(series, period=14):
    """Standard Wilder RSI with correct zero-gain/zero-loss edge handling."""
    close = pd.to_numeric(series, errors="coerce").astype(float)
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    avg_gain = _wilder_rma(gain, period)
    avg_loss = _wilder_rma(loss, period)

    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))

    # Correct mathematical edge cases.
    both_zero = (avg_gain == 0.0) & (avg_loss == 0.0)
    only_loss_zero = (avg_loss == 0.0) & (avg_gain > 0.0)
    only_gain_zero = (avg_gain == 0.0) & (avg_loss > 0.0)

    rsi = rsi.mask(only_loss_zero, 100.0)
    rsi = rsi.mask(only_gain_zero, 0.0)
    rsi = rsi.mask(both_zero, 50.0)
    return rsi


def calculate_macd(series, fast=12, slow=26, signal=9):
    exp1 = calculate_ema(series, fast)
    exp2 = calculate_ema(series, slow)
    macd = exp1 - exp2
    signal_line = calculate_ema(macd, signal)
    hist = macd - signal_line
    return macd, signal_line, hist


def calculate_true_range(df: pd.DataFrame) -> pd.Series:
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    prev_close = close.shift(1)
    return pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def calculate_atr(df: pd.DataFrame, period=14) -> pd.Series:
    """Wilder ATR."""
    if df is None or len(df) == 0:
        return pd.Series(dtype=float)
    return _wilder_rma(calculate_true_range(df), period)


def calculate_adx(df, period=14):
    """
    Wilder DMI/ADX implementation.

    The previous version calculated -DM from ``low.diff()`` and then
    compared it against an already-mutated +DM array. That made clean
    downtrends report an ADX near zero. This implementation follows the
    standard directional-movement definitions:

      up_move   = current_high - previous_high
      down_move = previous_low - current_low

    and applies Wilder smoothing to TR, +DM, -DM, and DX.
    """
    if df is None or len(df) == 0:
        return pd.Series(dtype=float)
    if len(df) < period + 1:
        return pd.Series(np.nan, index=df.index, dtype=float)

    def col(name_lower, name_upper):
        if name_lower in df.columns:
            return pd.to_numeric(df[name_lower], errors="coerce").astype(float)
        return pd.to_numeric(df[name_upper], errors="coerce").astype(float)

    high = col("high", "High")
    low = col("low", "Low")
    close = col("close", "Close")

    up_move = high.diff()
    down_move = -low.diff()  # previous low - current low

    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0),
        index=df.index,
        dtype=float,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0),
        index=df.index,
        dtype=float,
    )

    tr = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = _wilder_rma(tr, period)
    smooth_plus_dm = _wilder_rma(plus_dm, period)
    smooth_minus_dm = _wilder_rma(minus_dm, period)

    plus_di = 100.0 * smooth_plus_dm / atr.replace(0.0, np.nan)
    minus_di = 100.0 * smooth_minus_dm / atr.replace(0.0, np.nan)
    denom = plus_di + minus_di

    dx = 100.0 * (plus_di - minus_di).abs() / denom.replace(0.0, np.nan)
    adx = _wilder_rma(dx, period)
    return adx
