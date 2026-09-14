import numpy as np
import pandas as pd

from config import (
    MIN_1M_HISTORY,
    ATR_PERIOD,
    SWING_REVERSAL_ATR,
    ZONE_WIDTH_ATR,
    MIN_CLEARANCE_ATR,
    MAX_REACTION_RANGE_ATR,
    ACTIVITY_BASELINE_CANDLES,
    SIGNAL_THRESHOLD_CALL_PUT,
    CONTEXT_5M_MIN_CANDLES,
    CONTEXT_5M_RANK_WEIGHT,
)


def prepare_history(frame):
    """Validate closed M1 history without modifying the caller."""
    if frame is None:
        raise ValueError("MISSING_HISTORY")

    df = frame.copy()
    df.columns = [str(column).lower() for column in df.columns]

    if df.columns.duplicated().any():
        raise ValueError("DUPLICATE_COLUMNS")

    required = ["time", "open", "high", "low", "close"]

    if any(column not in df.columns for column in required):
        raise ValueError("MISSING_OHLC_OR_TIME")

    for column in required:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    values = df[required].to_numpy(dtype=float)

    if not np.isfinite(values).all():
        raise ValueError("NONFINITE_DATA")

    if (df["time"] % 60 != 0).any():
        raise ValueError("INVALID_M1_TIMESTAMP")

    df = (
        df.sort_values("time", kind="stable")
        .drop_duplicates("time", keep="last")
        .reset_index(drop=True)
    )

    if df.empty:
        raise ValueError("EMPTY_HISTORY")

    invalid = (
        (df["low"] <= 0)
        | (df["high"] < df["low"])
        | (df["high"] < df[["open", "close"]].max(axis=1))
        | (df["low"] > df[["open", "close"]].min(axis=1))
    )

    if invalid.any():
        raise ValueError("INVALID_OHLC")

    # Do not bridge missing market data with invented candles.
    intervals = df["time"].diff().fillna(60).to_numpy()
    gaps = np.flatnonzero(intervals != 60)

    if len(gaps):
        df = df.iloc[int(gaps[-1]):].reset_index(drop=True)

    df["time"] = df["time"].astype("int64")
    return df


def calculate_atr(df, period=ATR_PERIOD):
    previous_close = df["close"].shift(1)

    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - previous_close).abs(),
            (df["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return true_range.rolling(
        period,
        min_periods=period,
    ).mean()


def confirmed_swings(df):
    """
    Detect price-reversal pivots without a fixed swing duration.

    An ATR-sized close reversal confirms the preceding pivot.
    Unfinished swings are not returned as confirmed structure.
    """
    if df is None or len(df) <= ATR_PERIOD:
        return []

    atr = calculate_atr(df).to_numpy()
    closes = df["close"].to_numpy(dtype=float)

    pivots = []
    direction = 0
    high_index = ATR_PERIOD - 1
    low_index = ATR_PERIOD - 1

    def record(kind, index, confirmed_index):
        column = "high" if kind == "HIGH" else "low"

        pivots.append({
            "kind": kind,
            "price": float(df.iloc[index][column]),
            "time": int(df.iloc[index]["time"]),
            "index": int(index),
            "confirmed_index": int(confirmed_index),
        })

    for index in range(ATR_PERIOD, len(df)):
        threshold = (
            float(atr[index - 1]) * SWING_REVERSAL_ATR
        )

        if not np.isfinite(threshold) or threshold <= 0:
            continue

        if direction == 0:
            if closes[index] > closes[high_index]:
                high_index = index

            if closes[index] < closes[low_index]:
                low_index = index

            if closes[index] - closes[low_index] >= threshold:
                record("LOW", low_index, index)
                direction = 1
                high_index = index

            elif closes[high_index] - closes[index] >= threshold:
                record("HIGH", high_index, index)
                direction = -1
                low_index = index

        elif direction == 1:
            if closes[index] > closes[high_index]:
                high_index = index

            elif closes[high_index] - closes[index] >= threshold:
                record("HIGH", high_index, index)
                direction = -1
                low_index = index

        else:
            if closes[index] < closes[low_index]:
                low_index = index

            elif closes[index] - closes[low_index] >= threshold:
                record("LOW", low_index, index)
                direction = 1
                high_index = index

    return pivots


def structure_state(pivots, tolerance):
    highs = [
        pivot for pivot in pivots
        if pivot["kind"] == "HIGH"
    ]
    lows = [
        pivot for pivot in pivots
        if pivot["kind"] == "LOW"
    ]

    if len(highs) < 2 or len(lows) < 2:
        return "UNCONFIRMED", highs, lows

    higher_high = (
        highs[-1]["price"] > highs[-2]["price"] + tolerance
    )
    higher_low = (
        lows[-1]["price"] > lows[-2]["price"] + tolerance
    )
    lower_high = (
        highs[-1]["price"] < highs[-2]["price"] - tolerance
    )
    lower_low = (
        lows[-1]["price"] < lows[-2]["price"] - tolerance
    )

    if higher_high and higher_low:
        return "HH_HL", highs, lows

    if lower_high and lower_low:
        return "LH_LL", highs, lows

    return "MIXED", highs, lows


def active_zones(context, pivots, kind, width):
    """Build zones from pivots and discard subsequently broken levels."""
    zones = []

    for pivot in pivots:
        if pivot["kind"] != kind:
            continue

        level = pivot["price"]
        later_closes = context["close"].iloc[
            pivot["index"] + 1:
        ]

        if kind == "LOW":
            if (later_closes < level - width).any():
                continue
        else:
            if (later_closes > level + width).any():
                continue

        matching = next(
            (
                zone for zone in zones
                if abs(zone["level"] - level) <= width
            ),
            None,
        )

        if matching is None:
            zones.append({
                "level": level,
                "first_time": pivot["time"],
                "last_time": pivot["time"],
                "touches": 1,
            })
        else:
            count = matching["touches"]

            matching["level"] = (
                matching["level"] * count + level
            ) / (count + 1)

            matching["last_time"] = pivot["time"]
            matching["touches"] += 1

    return zones


def candle_reaction(current, previous, bullish):
    opening, high, low, close = (
        float(current[column])
        for column in ("open", "high", "low", "close")
    )

    span = high - low
    if span <= 0:
        return None

    body = abs(close - opening)
    body_ratio = body / span
    close_location = (close - low) / span

    upper_wick = high - max(opening, close)
    lower_wick = min(opening, close) - low

    previous_open = float(previous["open"])
    previous_close = float(previous["close"])

    if bullish:
        if close <= opening or close_location < 0.65:
            return None

        engulfing = (
            previous_close < previous_open
            and opening <= previous_close
            and close >= previous_open
            and body_ratio >= 0.45
        )

        rejection = (
            lower_wick >= body * 1.3
            and lower_wick >= span * 0.35
            and body_ratio >= 0.15
        )

        continuation = (
            body_ratio >= 0.60
            and close > float(previous["high"])
        )

    else:
        if close >= opening or close_location > 0.35:
            return None

        engulfing = (
            previous_close > previous_open
            and opening >= previous_close
            and close <= previous_open
            and body_ratio >= 0.45
        )

        rejection = (
            upper_wick >= body * 1.3
            and upper_wick >= span * 0.35
            and body_ratio >= 0.15
        )

        continuation = (
            body_ratio >= 0.60
            and close < float(previous["low"])
        )

    if not (engulfing or rejection or continuation):
        return None

    if engulfing:
        name = "ENGULFING"
    elif rejection:
        name = "REJECTION"
    else:
        name = "CONTINUATION"

    return {
        "name": name,
        "body_ratio": body_ratio,
        "close_location": close_location,
    }


def activity_pressure(df):
    """
    Estimate M1 price pressure weighted by live quote-update activity.

    This is not traded volume, order flow, or buyer/seller percentages.
    Unverified historical tickscount values are not used.
    """
    result = {
        "activity_status": "UNAVAILABLE",
        "relative_activity": None,
        "pressure": None,
    }

    needed = ACTIVITY_BASELINE_CANDLES + 1

    if "verified_ticks" not in df.columns or len(df) < needed:
        return result

    window = df.tail(needed)
    ticks = pd.to_numeric(
        window["verified_ticks"],
        errors="coerce",
    )

    tick_values = ticks.to_numpy(dtype=float)

    if not np.isfinite(tick_values).all():
        return result

    if (ticks <= 0).any():
        return result

    baseline = float(ticks.iloc[:-1].median())
    if baseline <= 0:
        return result

    current = window.iloc[-1]
    span = float(current["high"] - current["low"])

    if span <= 0:
        return result

    relative_activity = float(ticks.iloc[-1]) / baseline

    close_location = (
        2 * float(current["close"])
        - float(current["high"])
        - float(current["low"])
    ) / span

    body_efficiency = (
        abs(float(current["close"] - current["open"])) / span
    )

    pressure = (
        close_location
        * body_efficiency
        * min(relative_activity, 3.0)
    )

    result.update({
        "activity_status": "LIVE_TICK_PROXY",
        "relative_activity": relative_activity,
        "pressure": pressure,
    })

    return result


def get_5m_context(df):
    """
    Aggregate complete M5 candles from validated closed M1 history.

    M5 supplies context only. It cannot create or veto an M1 signal.
    """
    result = {
        "trend_5m": "UNAVAILABLE",
        "context_5m_reason": "Insufficient complete M5 history",
    }

    if df is None or df.empty:
        return result

    work = df[
        ["time", "open", "high", "low", "close"]
    ].copy()

    work["bucket"] = (work["time"] // 300) * 300

    candles = work.groupby("bucket", sort=True).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        count=("time", "size"),
        first_time=("time", "min"),
        last_time=("time", "max"),
    )

    latest_closed_epoch = int(work["time"].max()) + 60

    complete = (
        (candles["count"] == 5)
        & (candles["first_time"] == candles.index)
        & (candles["last_time"] == candles.index + 240)
        & (candles.index + 300 <= latest_closed_epoch)
    )

    candles = candles.loc[complete]

    if len(candles) < CONTEXT_5M_MIN_CANDLES:
        return result

    closes = candles["close"].astype(float)
    fast = closes.ewm(span=5, adjust=False).mean()
    slow = closes.ewm(span=10, adjust=False).mean()

    rising = fast.iloc[-1] > fast.iloc[-2]
    falling = fast.iloc[-1] < fast.iloc[-2]

    if fast.iloc[-1] > slow.iloc[-1] and rising:
        trend = "BULLISH"
        reason = "M5 EMA5 above EMA10 and rising"

    elif fast.iloc[-1] < slow.iloc[-1] and falling:
        trend = "BEARISH"
        reason = "M5 EMA5 below EMA10 and falling"

    else:
        trend = "NEUTRAL"
        reason = "M5 trend is mixed or flattening"

    return {
        "trend_5m": trend,
        "context_5m_reason": reason,
    }


def evaluate_strategy(df_1m, df_5m=None, df_15m=None):
    """
    M1 drives structure, zones, reaction and activity pressure.

    Optional dataframe arguments preserve the old call signature.
    M5 context is derived from complete M1 history.
    M15 is not used.
    """
    details = {
        "bias": "NOT_USED",
        "trend": "NEUTRAL",
        "structure": "UNCONFIRMED",
        "pa": "NEUTRAL",
        "score_reason": "",
        "signal_candle_epoch": None,
        "call_score": 0,
        "put_score": 0,
        "alignment_score": 0,
        "rank_strength": 0.0,
        "activity_status": "UNAVAILABLE",
        "relative_activity": None,
        "pressure": None,
        "trend_5m": "UNAVAILABLE",
        "context_5m_reason": "",
        "setup_id": None,
        "threshold_used": SIGNAL_THRESHOLD_CALL_PUT,
    }

    def reject(reason):
        details["score_reason"] = reason
        return "NO_TRADE", 0, reason, details

    try:
        df = prepare_history(df_1m)
    except (ValueError, TypeError, KeyError) as exc:
        return reject(str(exc))

    if len(df) < MIN_1M_HISTORY:
        return reject("INSUFFICIENT_CONTIGUOUS_HISTORY")

    current = df.iloc[-1]
    previous = df.iloc[-2]

    details["signal_candle_epoch"] = int(current["time"])
    details.update(get_5m_context(df))

    # Structure and zones must exist before the reaction candle.
    context = df.iloc[:-1].reset_index(drop=True)
    atr = float(calculate_atr(context).iloc[-1])

    if not np.isfinite(atr) or atr <= 0:
        return reject("INVALID_ATR")

    details["atr"] = atr
    width = atr * ZONE_WIDTH_ATR

    pivots = confirmed_swings(context)
    state, highs, lows = structure_state(
        pivots,
        tolerance=width * 0.5,
    )

    details["structure"] = state

    if state not in ("HH_HL", "LH_LL"):
        return reject("STRUCTURE_UNCONFIRMED_OR_MIXED")

    bullish = state == "HH_HL"
    direction = "CALL" if bullish else "PUT"

    details["trend"] = "BULLISH" if bullish else "BEARISH"

    close = float(current["close"])

    if bullish and close < lows[-1]["price"] - width:
        return reject("BULLISH_STRUCTURE_BROKEN")

    if not bullish and close > highs[-1]["price"] + width:
        return reject("BEARISH_STRUCTURE_BROKEN")

    span = float(current["high"] - current["low"])

    if span > MAX_REACTION_RANGE_ATR * atr:
        return reject("REACTION_CANDLE_OVEREXTENDED")

    reaction = candle_reaction(current, previous, bullish)

    if reaction is None:
        return reject("NO_CONFIRMED_REACTION")

    supports = active_zones(
        context, pivots, "LOW", width
    )
    resistances = active_zones(
        context, pivots, "HIGH", width
    )

    entry_zones = supports if bullish else resistances
    candidates = []

    for zone in entry_zones:
        level = zone["level"]

        touches = (
            float(current["low"]) <= level + width
            and float(current["high"]) >= level - width
        )

        holds = close > level if bullish else close < level
        near = abs(close - level) <= atr

        if touches and holds and near:
            candidates.append(zone)

    if not candidates:
        return reject("NO_SUPPORT_RESISTANCE_REACTION")

    zone = min(
        candidates,
        key=lambda item: abs(close - item["level"]),
    )

    opposite_zones = resistances if bullish else supports

    distances = [
        (
            opposite["level"] - width - close
            if bullish
            else close - opposite["level"] - width
        )
        for opposite in opposite_zones
        if (
            opposite["level"] >= close
            if bullish
            else opposite["level"] <= close
        )
    ]

    clearance = min(distances) / atr if distances else None
    details["clearance_atr"] = clearance

    if clearance is not None and clearance < MIN_CLEARANCE_ATR:
        return reject("OPPOSITE_ZONE_TOO_CLOSE")

    pressure_data = activity_pressure(df)
    details.update(pressure_data)

    directional_pressure = pressure_data["pressure"]

    if directional_pressure is not None:
        directional_pressure *= 1 if bullish else -1

        if directional_pressure < 0:
            return reject("OPPOSING_TICK_PRESSURE")

    # Heuristic setup score, not an estimated success probability.
    score = 8

    if zone["touches"] >= 2:
        score += 1

    if (
        directional_pressure is not None
        and directional_pressure >= 0.3
        and pressure_data["relative_activity"] >= 1.0
    ):
        score += 1

    anchor = (
        f"{highs[-1]['time']}:"
        f"{lows[-1]['time']}:"
        f"{zone['first_time']}"
    )

    clearance_component = (
        min(clearance, 3.0) * 0.1
        if clearance is not None
        else 0.0
    )

    pressure_component = (
        max(directional_pressure, 0.0) * 0.1
        if directional_pressure is not None
        else 0.0
    )

    rank_strength = (
        reaction["body_ratio"]
        + min(zone["touches"], 3) * 0.1
        + clearance_component
        + pressure_component
    )

    # M5 changes only the ranking slightly between equal-score setups.
    expected_5m = "BULLISH" if bullish else "BEARISH"
    opposite_5m = "BEARISH" if bullish else "BULLISH"
    trend_5m = details["trend_5m"]

    if trend_5m == expected_5m:
        rank_strength += CONTEXT_5M_RANK_WEIGHT

    elif trend_5m == opposite_5m:
        rank_strength -= CONTEXT_5M_RANK_WEIGHT

    details.update({
        "setup_id": f"{direction}:{anchor}",
        "zone": float(zone["level"]),
        "zone_touches": zone["touches"],
        "pa": reaction["name"],
        "alignment_score": 3,
        "rank_strength": rank_strength,
        "call_score": score if bullish else 0,
        "put_score": score if not bullish else 0,
    })

    relative_activity = pressure_data["relative_activity"]

    if relative_activity is not None:
        activity_text = (
            f"M1 tick activity {relative_activity:.2f}x"
        )
    else:
        activity_text = "M1 tick activity unavailable"

    zone_name = "support" if bullish else "resistance"

    details["score_reason"] = (
        f"M1 {state}; "
        f"{reaction['name']}; "
        f"{zone_name}={zone['level']:.6f}; "
        f"{activity_text}; "
        f"M5 context={trend_5m}"
    )

    if score < SIGNAL_THRESHOLD_CALL_PUT:
        return reject("BELOW_SCORE_THRESHOLD")

    return direction, score, "SETUP_CONFIRMED", details
