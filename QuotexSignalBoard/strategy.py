import pandas as pd
import numpy as np


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

    # Handle pure bullish / bearish sequences safely
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
    """
    Directional ADX calculation.

    Returns ADX only.
    Direction (+DI / -DI) is calculated separately in
    calculate_directional_strength().
    """

    if df is None or len(df) < period + 1:
        return pd.Series(25.0, index=df.index if df is not None else [])

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = pd.Series(
        np.where(
            (up_move > down_move) & (up_move > 0),
            up_move,
            0.0
        ),
        index=df.index
    )

    minus_dm = pd.Series(
        np.where(
            (down_move > up_move) & (down_move > 0),
            down_move,
            0.0
        ),
        index=df.index
    )

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()

    true_range = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    atr = true_range.rolling(
        window=period,
        min_periods=period
    ).mean()

    plus_di_series = (
        100
        * plus_dm.rolling(period, min_periods=period).mean()
        / atr.replace(0, np.nan)
    )

    minus_di_series = (
        100
        * minus_dm.rolling(period, min_periods=period).mean()
        / atr.replace(0, np.nan)
    )

    di_sum = (plus_di_series + minus_di_series).replace(0, np.nan)

    dx = (
        100
        * (plus_di_series - minus_di_series).abs()
        / di_sum
    )

    adx = dx.rolling(
        window=period,
        min_periods=period
    ).mean()

    return adx.fillna(25.0)


def calculate_directional_strength(df, period=14):
    """
    Returns:
        plus_di
        minus_di
        direction

    direction:
        BULLISH
        BEARISH
        NEUTRAL
    """

    if df is None or len(df) < period + 1:
        return 0.0, 0.0, "NEUTRAL"

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = pd.Series(
        np.where(
            (up_move > down_move) & (up_move > 0),
            up_move,
            0.0
        ),
        index=df.index
    )

    minus_dm = pd.Series(
        np.where(
            (down_move > up_move) & (down_move > 0),
            down_move,
            0.0
        ),
        index=df.index
    )

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()

    true_range = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    atr = true_range.rolling(
        window=period,
        min_periods=period
    ).mean()

    plus_di_series = (
        100
        * plus_dm.rolling(period, min_periods=period).mean()
        / atr.replace(0, np.nan)
    )

    minus_di_series = (
        100
        * minus_dm.rolling(period, min_periods=period).mean()
        / atr.replace(0, np.nan)
    )

    plus_di = float(
        plus_di_series.iloc[-1]
        if not pd.isna(plus_di_series.iloc[-1])
        else 0
    )

    minus_di = float(
        minus_di_series.iloc[-1]
        if not pd.isna(minus_di_series.iloc[-1])
        else 0
    )

    if plus_di > minus_di:
        direction = "BULLISH"
    elif minus_di > plus_di:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"

    return plus_di, minus_di, direction


# ============================================================
# MARKET STRUCTURE
# ============================================================

def detect_bos(df, lookback=5):
    """
    Detect recent Break of Structure.

    Returns:
        BULLISH_BOS
        BEARISH_BOS
        NEUTRAL
    """

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


# ============================================================
# CANDLE / PRICE ACTION
# ============================================================

def detect_candlestick_pattern(df):
    """
    Detects useful momentum / rejection patterns
    on the latest CLOSED candle.

    Returns:
        BULLISH
        BEARISH
        NEUTRAL
    """

    if df is None or len(df) < 2:
        return "NEUTRAL"

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    open_price = float(curr["open"])
    high = float(curr["high"])
    low = float(curr["low"])
    close = float(curr["close"])

    prev_high = float(prev["high"])
    prev_low = float(prev["low"])

    candle_range = high - low

    if candle_range <= 0:
        return "NEUTRAL"

    body = abs(close - open_price)

    upper_wick = high - max(open_price, close)
    lower_wick = min(open_price, close) - low

    body_ratio = body / candle_range

    bullish = close > open_price
    bearish = close < open_price

    # --------------------------------------------------------
    # Strong bullish momentum candle
    # --------------------------------------------------------

    if bullish and body_ratio >= 0.55:
        if close >= prev_high:
            return "BULLISH"

    # --------------------------------------------------------
    # Strong bearish momentum candle
    # --------------------------------------------------------

    if bearish and body_ratio >= 0.55:
        if close <= prev_low:
            return "BEARISH"

    # --------------------------------------------------------
    # Bullish rejection / hammer
    # --------------------------------------------------------

    if bullish:
        if (
            lower_wick >= body * 1.5
            and lower_wick > upper_wick
            and close > (low + candle_range * 0.55)
        ):
            return "BULLISH"

    # --------------------------------------------------------
    # Bearish rejection / shooting-star type
    # --------------------------------------------------------

    if bearish:
        if (
            upper_wick >= body * 1.5
            and upper_wick > lower_wick
            and close < (low + candle_range * 0.45)
        ):
            return "BEARISH"

    # --------------------------------------------------------
    # Engulfing-style momentum
    # --------------------------------------------------------

    prev_open = float(prev["open"])
    prev_close = float(prev["close"])

    if bullish and prev_close < prev_open:
        if open_price <= prev_close and close >= prev_open:
            return "BULLISH"

    if bearish and prev_close > prev_open:
        if open_price >= prev_close and close <= prev_open:
            return "BEARISH"

    return "NEUTRAL"


# ============================================================
# SAFE TIMESTAMP
# ============================================================

def _get_epoch(index_value):
    """
    Safely convert candle index/timestamp to epoch.
    """

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
    """
    Multi-timeframe 1-minute strategy.

    Architecture:

        15M = SOFT CONTEXT ONLY
        5M  = MAIN DIRECTION + STRUCTURE
        1M  = ENTRY MOMENTUM + TRIGGER

    Important:
        15M NEVER blocks a signal.

    Maximum score = 10.

    Components:

        15M context              +1
        5M trend/structure       +2
        5M ADX directional       +1
        1M EMA alignment         +2
        1M RSI momentum          +1
        1M MACD momentum         +1
        1M candle/price action   +2

        TOTAL                    10
    """

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

    # ========================================================
    # DATA VALIDATION
    # ========================================================

    if (
        df_1m is None
        or df_5m is None
        or len(df_1m) < 30
        or len(df_5m) < 30
    ):
        return (
            "NO_TRADE",
            0,
            "INSUFFICIENT_DATA",
            details
        )

    required_columns = {
        "open",
        "high",
        "low",
        "close"
    }

    if not required_columns.issubset(df_1m.columns):
        return (
            "NO_TRADE",
            0,
            "INVALID_1M_DATA",
            details
        )

    if not required_columns.issubset(df_5m.columns):
        return (
            "NO_TRADE",
            0,
            "INVALID_5M_DATA",
            details
        )

    # ========================================================
    # SIGNAL CANDLE
    # ========================================================

    latest_1m = df_1m.iloc[-1]

    details["signal_candle_epoch"] = _get_epoch(
        latest_1m.name
    )

    # ========================================================
    # 1. 15M SOFT BIAS
    # ========================================================

    bias_15m = "NEUTRAL"

    if df_15m is not None and len(df_15m) >= 20:

        close_15m = float(
            df_15m["close"].iloc[-1]
        )

        ema20_15m = float(
            calculate_ema(
                df_15m["close"],
                20
            ).iloc[-1]
        )

        if ema20_15m != 0:

            diff_pct = abs(
                close_15m - ema20_15m
            ) / abs(ema20_15m)

            # Very small distance = neutral context
            if diff_pct < 0.0005:
                bias_15m = "NEUTRAL"

            elif close_15m > ema20_15m:
                bias_15m = "BULLISH"

            else:
                bias_15m = "BEARISH"

    details["bias"] = bias_15m

    # ========================================================
    # 2. 5M MAIN DIRECTION
    # ========================================================

    ema20_5m = float(
        calculate_ema(
            df_5m["close"],
            20
        ).iloc[-1]
    )

    ema50_5m = float(
        calculate_ema(
            df_5m["close"],
            50
        ).iloc[-1]
    )

    structure_5m = detect_bos(
        df_5m,
        lookback=5
    )

    details["structure"] = structure_5m

    # Directional ADX
    adx_series = calculate_adx(
        df_5m,
        period=14
    )

    adx_5m = float(
        adx_series.iloc[-1]
        if not pd.isna(adx_series.iloc[-1])
        else 25.0
    )

    plus_di, minus_di, di_direction = (
        calculate_directional_strength(
            df_5m,
            period=14
        )
    )

    details["adx_5m"] = round(
        adx_5m,
        2
    )

    # --------------------------------------------------------
    # Determine 5M direction WITHOUT allowing both sides
    # --------------------------------------------------------

    ema_bullish = ema20_5m > ema50_5m
    ema_bearish = ema20_5m < ema50_5m

    bullish_structure = (
        structure_5m == "BULLISH_BOS"
    )

    bearish_structure = (
        structure_5m == "BEARISH_BOS"
    )

    # Strong directional agreement
    if (
        ema_bullish
        and (
            bullish_structure
            or di_direction == "BULLISH"
        )
    ):
        trend_5m = "BULLISH"

    elif (
        ema_bearish
        and (
            bearish_structure
            or di_direction == "BEARISH"
        )
    ):
        trend_5m = "BEARISH"

    # EMA + structure agreement
    elif ema_bullish and bullish_structure:
        trend_5m = "BULLISH"

    elif ema_bearish and bearish_structure:
        trend_5m = "BEARISH"

    # If only EMA gives direction, keep it but mark as weaker
    elif ema_bullish and not ema_bearish:
        trend_5m = "BULLISH"

    elif ema_bearish and not ema_bullish:
        trend_5m = "BEARISH"

    else:
        trend_5m = "NEUTRAL"

    details["trend"] = trend_5m

    # ========================================================
    # 3. 1M ENTRY INDICATORS
    # ========================================================

    ema9_1m = float(
        calculate_ema(
            df_1m["close"],
            9
        ).iloc[-1]
    )

    ema21_1m = float(
        calculate_ema(
            df_1m["close"],
            21
        ).iloc[-1]
    )

    rsi_series = calculate_rsi(
        df_1m["close"],
        14
    )

    rsi_14_1m = float(
        rsi_series.iloc[-1]
        if not pd.isna(rsi_series.iloc[-1])
        else 50.0
    )

    macd_line, signal_line, macd_hist = (
        calculate_macd(
            df_1m["close"]
        )
    )

    curr_hist = float(
        macd_hist.iloc[-1]
    )

    prev_hist = float(
        macd_hist.iloc[-2]
    )

    pa_1m = detect_candlestick_pattern(
        df_1m
    )

    details["pa"] = pa_1m

    # ========================================================
    # 1M EMA DIRECTION
    # ========================================================

    if ema9_1m > ema21_1m:
        ema_direction = "BULLISH"

    elif ema9_1m < ema21_1m:
        ema_direction = "BEARISH"

    else:
        ema_direction = "NEUTRAL"

    details["ema_alignment"] = ema_direction

    # ========================================================
    # MACD MOMENTUM
    # ========================================================

    if (
        curr_hist > 0
        and curr_hist > prev_hist
    ):
        macd_direction = "BULLISH"

    elif (
        curr_hist < 0
        and curr_hist < prev_hist
    ):
        macd_direction = "BEARISH"

    elif curr_hist > 0:
        macd_direction = "BULLISH_WEAK"

    elif curr_hist < 0:
        macd_direction = "BEARISH_WEAK"

    else:
        macd_direction = "NEUTRAL"

    details["macd_momentum"] = macd_direction

    # ========================================================
    # 4. SCORE
    # ========================================================

    call_score = 0
    put_score = 0

    call_reasons = []
    put_reasons = []

    # --------------------------------------------------------
    # 15M SOFT CONTEXT
    #
    # IMPORTANT:
    # Opposite 15M NEVER removes points.
    # It simply gives no bonus.
    # --------------------------------------------------------

    if bias_15m == "BULLISH":
        call_score += 1
        call_reasons.append("15M bullish context")

    elif bias_15m == "BEARISH":
        put_score += 1
        put_reasons.append("15M bearish context")

    # --------------------------------------------------------
    # 5M MAIN DIRECTION / STRUCTURE
    # --------------------------------------------------------

    if trend_5m == "BULLISH":
        call_score += 2
        call_reasons.append("5M bullish trend")

    elif trend_5m == "BEARISH":
        put_score += 2
        put_reasons.append("5M bearish trend")

    # --------------------------------------------------------
    # 5M ADX + DI DIRECTION
    # --------------------------------------------------------

    if adx_5m >= 20:

        if di_direction == "BULLISH":
            call_score += 1
            call_reasons.append(
                f"5M bullish strength ADX {adx_5m:.1f}"
            )

        elif di_direction == "BEARISH":
            put_score += 1
            put_reasons.append(
                f"5M bearish strength ADX {adx_5m:.1f}"
            )

    # --------------------------------------------------------
    # 1M EMA
    # --------------------------------------------------------

    if ema_direction == "BULLISH":
        call_score += 2
        call_reasons.append("1M EMA9>EMA21")

    elif ema_direction == "BEARISH":
        put_score += 2
        call_reasons.append("1M EMA9<EMA21")

    # --------------------------------------------------------
    # 1M RSI
    #
    # Avoid buying extreme overbought.
    # Avoid selling extreme oversold.
    # --------------------------------------------------------

    if 50 < rsi_14_1m < 72:

        call_score += 1
        call_reasons.append(
            f"1M RSI bullish {rsi_14_1m:.1f}"
        )

    elif 28 < rsi_14_1m < 50:

        put_score += 1
        put_reasons.append(
            f"1M RSI bearish {rsi_14_1m:.1f}"
        )

    # --------------------------------------------------------
    # 1M MACD
    # --------------------------------------------------------

    if macd_direction == "BULLISH":
        call_score += 1
        call_reasons.append("1M MACD rising")

    elif macd_direction == "BEARISH":
        put_score += 1
        put_reasons.append("1M MACD falling")

    # --------------------------------------------------------
    # 1M PRICE ACTION
    # --------------------------------------------------------

    if pa_1m == "BULLISH":
        call_score += 2
        call_reasons.append("1M bullish price action")

    elif pa_1m == "BEARISH":
        put_score += 2
        put_reasons.append("1M bearish price action")

    # ========================================================
    # 5. TIMEFRAME ALIGNMENT
    # ========================================================

    # --------------------------------------------------------
    # If 5M has a clear direction and 1M is directly opposite,
    # don't immediately reject it.
    #
    # Instead, require stronger evidence.
    # --------------------------------------------------------

    counter_trend = False

    if trend_5m == "BULLISH" and ema_direction == "BEARISH":
        counter_trend = True
        details["timeframe_alignment"] = (
            "1M_COUNTER_5M_BULLISH"
        )

    elif trend_5m == "BEARISH" and ema_direction == "BULLISH":
        counter_trend = True
        details["timeframe_alignment"] = (
            "1M_COUNTER_5M_BEARISH"
        )

    else:
        if (
            trend_5m == ema_direction
            and trend_5m != "NEUTRAL"
        ):
            details["timeframe_alignment"] = "ALIGNED"

        elif (
            trend_5m == "NEUTRAL"
            or ema_direction == "NEUTRAL"
        ):
            details["timeframe_alignment"] = "NEUTRAL"

        else:
            details["timeframe_alignment"] = "MIXED"

    # ========================================================
    # 6. DETERMINE WINNING DIRECTION
    # ========================================================

    if call_score > put_score:
        direction = "CALL"
        final_score = call_score
        reasons = call_reasons

    elif put_score > call_score:
        direction = "PUT"
        final_score = put_score
        reasons = put_reasons

    else:
        details["score_reason"] = (
            f"Balanced setup. CALL={call_score}, "
            f"PUT={put_score}"
        )

        return (
            "NO_TRADE",
            max(call_score, put_score),
            "NO_TRADE",
            details
        )

    # ========================================================
    # 7. THRESHOLD
    # ========================================================

    # Normal strong setup
    threshold = 7

    # If 1M is directly counter to 5M,
    # require one additional point.
    if counter_trend:

        threshold = 8

        details["threshold_used"] = 8

        # Counter-trend requires strong 1M evidence:
        # candle + MACD or RSI must support direction.
        if direction == "CALL":

            strong_1m_confirmation = (
                pa_1m == "BULLISH"
                and (
                    macd_direction == "BULLISH"
                    or 50 < rsi_14_1m < 72
                )
            )

        else:

            strong_1m_confirmation = (
                pa_1m == "BEARISH"
                and (
                    macd_direction == "BEARISH"
                    or 28 < rsi_14_1m < 50
                )
            )

        if not strong_1m_confirmation:

            details["score_reason"] = (
                f"{direction} rejected: "
                f"1M counter to 5M without strong "
                f"reversal confirmation. "
                f"Score={final_score}/10"
            )

            return (
                "NO_TRADE",
                final_score,
                "REVERSAL_NOT_CONFIRMED",
                details
            )

    else:
        details["threshold_used"] = 7

    # ========================================================
    # 8. FINAL SIGNAL
    # ========================================================

    if final_score < threshold:

        details["score_reason"] = (
            f"{direction} score {final_score}/10 "
            f"below threshold {threshold}"
        )

        return (
            "NO_TRADE",
            final_score,
            "LOW_SCORE",
            details
        )

    # ========================================================
    # QUALITY
    # ========================================================

    if final_score >= 9:
        quality = "A+"

    elif final_score == 8:
        quality = "A"

    elif final_score == 7:
        quality = "B+"

    else:
        quality = "B"

    details["score_reason"] = (
        f"{direction} {final_score}/10 | "
        + ", ".join(reasons)
    )

    return (
        direction,
        final_score,
        quality,
        details
    )
