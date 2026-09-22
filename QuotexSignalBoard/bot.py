"""
Main Trading Bot Coordinator for Quotex Signal Board (1M Precision Optimized).
Includes:
  1. High-frequency scan worker (0.25s sleep, 1.0s interval).
  2. Per-pair cooldown protection to prevent signal spamming.
  3. Session filter to reject quiet Asian session hours.
  4. Mandatory 1M + 5M REST history seeding.
  5. Actionable Telegram alert message reminding traders to enter within 10-15s.
"""

import datetime
import logging
import math
import os
import sqlite3
import threading
import time

import pusher
import pytz
import requests
import pandas as pd
from flask import Flask, jsonify, render_template

import config as cfg

from core.events import EventDispatcher
from data.candle_manager import CandleManager
from data.deriv_client import DerivClient
from database import get_db_connection, init_db
from indicators import calculate_adx
from monitor import get_today_performance
from strategy import evaluate_strategy, prepare_history


# ============================================================
# LOGGING AND APPLICATION SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger("QuotexSignalBoard")

app = Flask(__name__)
init_db()


# ============================================================
# SHARED STATE
# ============================================================

latest_evaluations = {}

evaluation_lock = threading.Lock()
state_lock = threading.RLock()
history_lock = threading.Lock()

event_dispatcher = EventDispatcher()
deriv_client = DerivClient()

candle_managers = {}
live_state = {}
tick_windows = {}

ready = threading.Event()

_threads_started = False
_threads_lock = threading.Lock()

EXPIRY_SECONDS = int(getattr(cfg, "EXPIRY_SECONDS", 60))
PAYOUT_PERCENT = float(getattr(cfg, "PAYOUT_PERCENT", 85.0))

auto_disabled_symbols = set()
auto_disabled_lock = threading.Lock()


# ============================================================
# PUSHER
# ============================================================

pusher_client = None

if (
    getattr(cfg, "PUSHER_ENABLED", False)
    and getattr(cfg, "PUSHER_APP_ID", None)
    and getattr(cfg, "PUSHER_KEY", None)
    and getattr(cfg, "PUSHER_SECRET", None)
):
    try:
        pusher_client = pusher.Pusher(
            app_id=cfg.PUSHER_APP_ID,
            key=cfg.PUSHER_KEY,
            secret=cfg.PUSHER_SECRET,
            cluster=getattr(cfg, "PUSHER_CLUSTER", "ap2"),
            ssl=True,
        )
    except Exception:
        logger.error("Pusher initialization failed.")


# ============================================================
# PERSISTENT DELIVERY DEDUPLICATION & COOLDOWN
# ============================================================

def init_dispatch_ledger():
    conn = get_db_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dispatch_ledger_v2 (
                target_epoch INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                setup_id TEXT NOT NULL,
                status TEXT NOT NULL,
                UNIQUE(symbol, setup_id)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


init_dispatch_ledger()


def symbol_in_cooldown(symbol: str, cooldown_minutes: int) -> bool:
    """Checks if pair dispatched an alert within cooldown minutes."""
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT candle_epoch FROM signal_history WHERE symbol = ? ORDER BY candle_epoch DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        if not row or not row["candle_epoch"]:
            return False
        server_now = deriv_client.get_server_time()
        return (server_now - int(row["candle_epoch"])) < (cooldown_minutes * 60)
    finally:
        conn.close()


def used_setup(symbol: str, setup_id: str) -> bool:
    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT 1 FROM dispatch_ledger_v2
            WHERE symbol = ? AND setup_id = ?
            """,
            (symbol, setup_id),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def minute_has_dispatch_attempt(target_epoch: int) -> bool:
    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT 1 FROM dispatch_ledger_v2
            WHERE target_epoch = ?
            """,
            (target_epoch,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def reserve_dispatch(symbol: str, setup_id: str, target_epoch: int) -> bool:
    conn = get_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO dispatch_ledger_v2 (
                target_epoch, symbol, setup_id, status
            )
            VALUES (?, ?, ?, 'ATTEMPTING')
            """,
            (target_epoch, symbol, setup_id),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def set_dispatch_status(target_epoch: int, status: str):
    conn = get_db_connection()
    try:
        conn.execute(
            """
            UPDATE dispatch_ledger_v2
            SET status = ?
            WHERE target_epoch = ?
            """,
            (status, target_epoch),
        )
        conn.commit()
    finally:
        conn.close()


# ============================================================
# PER-PAIR AUTO-FILTER
# ============================================================

def refresh_auto_disabled_pairs():
    min_trades = int(getattr(cfg, "AUTO_FILTER_MIN_TRADES", 30))
    min_wr = float(getattr(cfg, "AUTO_FILTER_MIN_WIN_RATE", 0.50))

    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT symbol,
                   COUNT(*) AS total,
                   SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END) AS losses
            FROM signal_history
            WHERE result IN ('WIN', 'LOSS')
            GROUP BY symbol
            """
        ).fetchall()
    finally:
        conn.close()

    disabled = set()
    for symbol, total, wins, losses in rows:
        settled = (wins or 0) + (losses or 0)
        if settled < min_trades:
            continue
        wr = (wins or 0) / float(settled)
        if wr < min_wr:
            disabled.add(symbol)
            logger.warning(
                "Auto-filter DISABLED %s: %s trades, WR=%.1f%% (min %.0f%%)",
                symbol, settled, wr * 100.0, min_wr * 100.0,
            )

    with auto_disabled_lock:
        auto_disabled_symbols.clear()
        auto_disabled_symbols.update(disabled)


def is_auto_disabled(symbol: str) -> bool:
    with auto_disabled_lock:
        return symbol in auto_disabled_symbols


# ============================================================
# SYMBOL HELPER & TICK PROCESSING
# ============================================================

def get_symbol_variants(symbol: str):
    clean = symbol.replace("frx", "").replace("/", "").upper()
    variants = {
        symbol,
        clean,
        f"frx{clean}",
        f"{clean[:3]}/{clean[3:]}" if len(clean) == 6 else clean
    }
    return list(variants)


def make_tick_handler(symbol, manager):
    def handle(epoch, price, receipt_time):
        try:
            epoch = int(epoch)
            price = float(price)

            if not math.isfinite(price) or price <= 0:
                return []

            monotonic_now = time.monotonic()
            boundary = (epoch // 60) * 60
            variants = get_symbol_variants(symbol)

            with state_lock:
                for target_key in variants:
                    previous = live_state.get(target_key)
                    if previous and epoch < previous["epoch"]:
                        continue

                    windows = tick_windows.setdefault(target_key, {})
                    window = windows.get(boundary)

                    if window is None:
                        window = {
                            "count": 0,
                            "first": epoch,
                            "last": epoch,
                            "last_receipt": monotonic_now,
                            "complete": True,
                        }
                        windows[boundary] = window
                    else:
                        window["count"] += 1
                        window["last"] = epoch
                        window["last_receipt"] = monotonic_now

                    live_state[target_key] = {
                        "epoch": epoch,
                        "price": price,
                        "receipt": monotonic_now,
                    }

                    oldest_allowed = boundary - getattr(cfg, "CANDLE_HISTORY_LIMIT", 200) * 60
                    for old_epoch in list(windows):
                        if old_epoch < oldest_allowed:
                            del windows[old_epoch]

            return manager.process_tick(epoch, price, receipt_time)

        except Exception:
            logger.exception("Tick processing failed for %s", symbol)
            return []

    return handle


for symbol in cfg.FOREX_PAIRS:
    manager = CandleManager(
        symbol=symbol,
        event_dispatcher=event_dispatcher,
        max_history=getattr(cfg, "CANDLE_HISTORY_LIMIT", 200),
    )
    candle_managers[symbol] = manager
    handler = make_tick_handler(symbol, manager)

    for var in get_symbol_variants(symbol):
        tick_windows[var] = {}
        deriv_client.register_tick_handler(var, handler)


def live_quote(symbol):
    variants = get_symbol_variants(symbol)
    stale_after = getattr(cfg, "STALE_TICK_THRESHOLD_SEC", 25.0)

    with state_lock:
        quote = None
        for v in variants:
            if v in live_state:
                quote = live_state[v]
                break

        if quote is None:
            manager = candle_managers.get(symbol)
            if manager:
                last_c = manager.get_latest_closed_candle("1M")
                if last_c:
                    quote = {
                        "epoch": int(last_c.close_epoch),
                        "price": float(last_c.close),
                        "receipt": time.monotonic(),
                    }

        if quote is None:
            return None
        quote = dict(quote)

    now_epoch = deriv_client.get_server_time()
    if now_epoch - quote["epoch"] > stale_after:
        return None

    return quote


def history_snapshot(symbol, timeframe="1M"):
    if timeframe != "1M":
        manager = candle_managers.get(symbol)
        if not manager:
            return pd.DataFrame()
        with state_lock:
            return manager.get_closed_history(timeframe)

    with state_lock:
        manager = candle_managers.get(symbol)
        if not manager:
            return pd.DataFrame()
        df = manager.get_closed_history("1M").copy()

        variants = get_symbol_variants(symbol)
        symbol_windows = {}
        for v in variants:
            if v in tick_windows and tick_windows[v]:
                symbol_windows = tick_windows[v]
                break

        windows = {epoch: dict(value) for epoch, value in symbol_windows.items()}

    if df.empty or "time" not in df.columns:
        return df

    def verified_count(epoch):
        epoch = int(epoch)
        window = windows.get(epoch)
        if window is None or not window.get("complete", False):
            return float("nan")
        return float(window["count"])

    df["verified_ticks"] = df["time"].map(verified_count)
    return df


def feed_diagnostics(symbol):
    variants = get_symbol_variants(symbol)

    with state_lock:
        quote = None
        for v in variants:
            if v in live_state:
                quote = live_state[v]
                break

        manager = candle_managers.get(symbol)
        candle = manager.get_latest_closed_candle("1M") if manager else None
        last_close = int(candle.close_epoch) if candle else None

        if quote is None and candle:
            quote = {
                "epoch": last_close,
                "price": float(candle.close),
                "receipt": time.monotonic(),
            }

        quote = dict(quote) if quote else None

    now = deriv_client.get_server_time()

    return {
        "last_tick_epoch": quote["epoch"] if quote else None,
        "tick_age_seconds": round(now - quote["epoch"], 2) if quote else None,
        "receipt_age_seconds": round(time.monotonic() - quote["receipt"], 2) if quote else None,
        "last_candle_close_epoch": last_close,
    }


# ============================================================
# TELEGRAM DELIVERY WITH TIMING WARNING
# ============================================================

def send_telegram_alert(candidate, target_epoch):
    if not getattr(cfg, "TELEGRAM_ENABLED", False):
        return "DISABLED"

    if not cfg.TELEGRAM_BOT_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        logger.error("Telegram configuration missing.")
        return "CONFIG_ERROR"

    tz = pytz.timezone(getattr(cfg, "TIMEZONE_NAME", "Asia/Dhaka"))
    expiry = datetime.datetime.fromtimestamp(target_epoch + EXPIRY_SECONDS, tz)
    details = candidate["details"]

    trend_5m = details.get("trend_5m", "UNAVAILABLE")
    trend_line = (
        f"📈 5M Trend: {trend_5m}\n"
        if trend_5m != "UNAVAILABLE"
        else "📈 5M Trend: unavailable\n"
    )

    message = (
        "⚡️ <b>QUOTEX 1M HIGH-SPEED SIGNAL</b> ⚡️\n\n"
        f"📊 <b>Pair:</b> {candidate['display_name']}\n"
        f"🎯 <b>Action:</b> <b>{candidate['direction']}</b>\n"
        f"⭐️ <b>Score:</b> {candidate['score']}/{getattr(cfg, 'SIGNAL_THRESHOLD_CALL_PUT', 7)} ({candidate['quality']})\n"
        f"⏰ <b>Expiry:</b> {expiry.strftime('%H:%M:%S')} {getattr(cfg, 'TIMEZONE_NAME', 'Asia/Dhaka')} (1M)\n"
        f"💰 <b>Min Payout:</b> {PAYOUT_PERCENT:.0f}%\n"
        f"{trend_line}"
        f"💡 <b>Reason:</b> {details.get('score_reason', 'N/A')}\n\n"
        "⚠️ <b>Enter within first 10–15 seconds of the new candle.</b>"
    )

    url = f"https://api.telegram.org/bot{cfg.TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        response = requests.post(
            url,
            json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=(2, 3),
        )
        body = response.json()
        if response.status_code == 200 and body.get("ok") is True:
            logger.info("Telegram SENT for %s %s", candidate["display_name"], candidate["direction"])
            return "SENT"
        return "REJECTED"
    except Exception:
        return "UNKNOWN"


# ============================================================
# SIGNAL RECORDING & DISPATCH (PAYOUT & EXPECTANCY TRACKING)
# ============================================================

def save_signal(candidate, target_epoch, entry_price):
    tz = pytz.timezone(getattr(cfg, "TIMEZONE_NAME", "Asia/Dhaka"))
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    local_now = utc_now.astimezone(tz)
    signal_id = f"{candidate['symbol']}_1M_{target_epoch}"

    conn = get_db_connection()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO signal_history (
                signal_id, symbol, display_pair, timeframe, candle_epoch,
                signal_timestamp_utc, signal_timestamp_bdt, direction, score, quality,
                bias_15m, entry_reference_price, entry_reference_timestamp, created_at, result,
                payout_percent, expiry_seconds
            )
            VALUES (?, ?, ?, '1M', ?, ?, ?, ?, ?, ?, 'NOT_USED', ?, ?, ?, 'PENDING', ?, ?)
            """,
            (
                signal_id,
                candidate["symbol"],
                candidate["display_name"],
                target_epoch,
                utc_now.isoformat(),
                local_now.isoformat(),
                candidate["direction"],
                candidate["score"],
                candidate["quality"],
                entry_price,
                utc_now.isoformat(),
                local_now.isoformat(),
                PAYOUT_PERCENT,
                EXPIRY_SECONDS,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def dispatch_best_signal(candidate, target_epoch):
    quote = live_quote(candidate["symbol"])
    if quote is None:
        quote = {"price": candidate["analysis_close"], "epoch": target_epoch}

    if not getattr(cfg, "TELEGRAM_ENABLED", False):
        return "DISABLED"

    if not reserve_dispatch(candidate["symbol"], candidate["details"]["setup_id"], target_epoch):
        return "DUPLICATE"

    status = send_telegram_alert(candidate, target_epoch)
    set_dispatch_status(target_epoch, status)

    if status == "SENT":
        try:
            save_signal(candidate, target_epoch, quote["price"])
        except Exception:
            logger.exception("save_signal failed for %s", candidate["symbol"])

        if pusher_client:
            try:
                pusher_client.trigger(
                    "trading-signals",
                    "new-signal",
                    {
                        "pair": candidate["display_name"],
                        "direction": candidate["direction"],
                        "score": candidate["score"],
                        "quality": candidate["quality"],
                        "trend_5m": candidate["details"].get("trend_5m", "UNAVAILABLE"),
                        "timeframe": "1M",
                        "target_candle_epoch": target_epoch,
                        "expiry_epoch": target_epoch + EXPIRY_SECONDS,
                        "payout_percent": PAYOUT_PERCENT,
                    },
                )
            except Exception:
                logger.exception("Pusher trigger failed for %s", candidate["symbol"])

    return status


# ============================================================
# EVALUATION & DISPATCH WORKER (SESSION & COOLDOWN GATED)
# ============================================================

def evaluate_and_dispatch_all(target_epoch):
    if not evaluation_lock.acquire(blocking=False):
        return

    try:
        # 1. Asian Session Dead Hours Filter (UTC)
        utc_hour = datetime.datetime.now(datetime.timezone.utc).hour
        start, end = getattr(cfg, "SIGNAL_HOURS_UTC", (0, 24))
        if not (start <= utc_hour < end):
            logger.info("Trading hours filter active: UTC %s is outside window %s-%s.", utc_hour, start, end)
            return

        refresh_auto_disabled_pairs()

        candidates = []
        reports = {}
        timestamp = datetime.datetime.now(pytz.timezone(getattr(cfg, "TIMEZONE_NAME", "Asia/Dhaka"))).isoformat()
        cooldown_mins = getattr(cfg, "PAIR_COOLDOWN_MINUTES", 10)

        for symbol, display in cfg.FOREX_PAIRS.items():
            report = {
                "timestamp": timestamp,
                "direction": "NO_TRADE",
                "score": 0,
                "quality": "WAIT",
                "bias": "NOT_USED",
                "target_candle_epoch": target_epoch,
                "expected_close_epoch": target_epoch + EXPIRY_SECONDS,
                "delivery": "NOT_SENT",
            }

            try:
                if is_auto_disabled(symbol):
                    report["score_reason"] = "PAIR_AUTO_DISABLED"
                    reports[display] = report
                    continue

                # 2. Per-Pair Cooldown Check
                if symbol_in_cooldown(symbol, cooldown_mins):
                    report["delivery"] = "PAIR_COOLDOWN"
                    report["score_reason"] = f"COOLDOWN_{cooldown_mins}M"
                    reports[display] = report
                    continue

                report.update(feed_diagnostics(symbol))

                manager = candle_managers.get(symbol)
                latest_closed = manager.get_latest_closed_candle("1M") if manager else None
                if latest_closed is None or int(latest_closed.close_epoch) != int(target_epoch):
                    report["score_reason"] = "FORMING_CANDLE_NOT_CLOSED"
                    reports[display] = report
                    continue

                raw_df = history_snapshot(symbol)
                df = prepare_history(raw_df) if not raw_df.empty else raw_df

                if not df.empty and "time" in df.columns:
                    df = df[df["time"] <= target_epoch].reset_index(drop=True)

                min_history = getattr(cfg, "MIN_1M_HISTORY", 20)
                if df.empty or len(df) < min_history:
                    report["score_reason"] = "LATEST_CLOSED_CANDLE_MISSING"
                    reports[display] = report
                    continue

                df_5m = history_snapshot(symbol, "5M")
                adx_5m = None
                min_5m = getattr(cfg, "CONTEXT_5M_MIN_CANDLES", 10)
                if not df_5m.empty and len(df_5m) >= min_5m:
                    try:
                        adx_series = calculate_adx(df_5m, period=14)
                        if len(adx_series) > 0:
                            last_adx = float(adx_series.iloc[-1])
                            if math.isfinite(last_adx):
                                adx_5m = last_adx
                    except Exception:
                        adx_5m = None

                direction, score, quality, details = evaluate_strategy(
                    df, df_5m, {"adx_5m": adx_5m}
                )

                report.update({
                    "direction": direction,
                    "score": score,
                    "quality": quality,
                    "details": details.get("pa"),
                    "score_reason": details.get("score_reason", "EVALUATED"),
                    "structure": details.get("structure", "N/A"),
                    "trend_5m": details.get("trend_5m", "UNAVAILABLE"),
                    "call_score": details.get("call_score", 0),
                    "put_score": details.get("put_score", 0),
                    "activity_status": details.get("activity_status", "ACTIVE"),
                    "relative_activity": details.get("relative_activity", 1.0),
                    "analysis_candle_epoch": int(df.iloc[-1]["time"]),
                })

                if direction in ("CALL", "PUT"):
                    if used_setup(symbol, details.get("setup_id", "")):
                        report["delivery"] = "SETUP_ALREADY_ATTEMPTED"
                    else:
                        candidates.append({
                            "symbol": symbol,
                            "display_name": display,
                            "direction": direction,
                            "score": score,
                            "quality": quality,
                            "details": details,
                            "analysis_close": float(df.iloc[-1]["close"]),
                        })

            except Exception as exc:
                report["score_reason"] = f"DATA_ERROR:{type(exc).__name__}"

            reports[display] = report

        candidates.sort(
            key=lambda c: (
                -c["score"],
                -c["details"].get("rank_strength", 0),
                c["symbol"],
            )
        )

        if candidates and cfg.SIGNALS_ENABLED:
            best_candidate = candidates[0]
            status = dispatch_best_signal(best_candidate, target_epoch)
            reports[best_candidate["display_name"]]["delivery"] = status

        with state_lock:
            latest_evaluations.clear()
            latest_evaluations.update(reports)

    finally:
        evaluation_lock.release()


# ============================================================
# HISTORICAL DATA REFRESH (1M + 5M REST SEEDING)
# ============================================================

def resync_historical_candles():
    if not history_lock.acquire(blocking=False):
        return

    try:
        jobs = []
        for symbol in cfg.FOREX_PAIRS:
            jobs.append({
                "key": f"{symbol}:1M",
                "symbol": symbol,
                "count": 300,
                "granularity": 60,
            })
            # 5M History Seeding
            jobs.append({
                "key": f"{symbol}:5M",
                "symbol": symbol,
                "count": 100,
                "granularity": 300,
            })

        histories = deriv_client.fetch_historical_candles_batch_sync(jobs)
        server_epoch = int(deriv_client.fetch_server_epoch_sync())
        updated = 0

        for symbol, manager in candle_managers.items():
            raw_1m = histories.get(f"{symbol}:1M", [])
            raw_5m = histories.get(f"{symbol}:5M", [])
            if not raw_1m and not raw_5m:
                continue

            with state_lock:
                if raw_1m:
                    manager.seed_historical_candles("1M", raw_1m, server_epoch)
                    last_c = raw_1m[-1]
                    clean = symbol.replace("frx", "").replace("/", "").upper()
                    ep = int(last_c.get("epoch") or last_c.get("time") or server_epoch)
                    cl = float(last_c.get("close", 0))
                    for k in [symbol, clean, f"frx{clean}"]:
                        live_state[k] = {"epoch": ep, "price": cl, "receipt": time.monotonic()}
                if raw_5m:
                    manager.seed_historical_candles("5M", raw_5m, server_epoch)
            updated += 1

        logger.info("Seeded 1M & 5M candles for %s/%s pairs.", updated, len(cfg.FOREX_PAIRS))

    except Exception:
        logger.exception("History refresh failed.")
    finally:
        history_lock.release()


def run_history_worker():
    ready.wait()
    last_minute = None

    while True:
        try:
            now = deriv_client.get_server_time()
            minute = int(now // 60) * 60

            if 10 <= (now % 60) < 30 and minute != last_minute:
                last_minute = minute
                resync_historical_candles()

            time.sleep(1.0)
        except Exception:
            logger.exception("History worker failed.")
            time.sleep(2)


# ============================================================
# SCAN WORKER — HIGH SPEED (0.25s sleep, 1.0s attempt throttle)
# ============================================================

def run_scan_worker():
    ready.wait()
    active_minute = None
    finished_minute = None
    last_attempt = 0.0

    while True:
        try:
            now = deriv_client.get_server_time()
            minute = int(now // 60) * 60
            second = now - minute

            if minute != active_minute:
                active_minute = minute
                last_attempt = 0.0

            if minute == finished_minute:
                time.sleep(0.1)
                continue

            scan_delay = float(getattr(cfg, "SCAN_DELAY_SECONDS", 1.0))
            max_delay = float(getattr(cfg, "MAX_ENTRY_DELAY_SECONDS", 5.0))

            if second > max_delay:
                finished_minute = minute
                time.sleep(0.1)
                continue

            if second < scan_delay:
                time.sleep(0.05)
                continue

            if minute_has_dispatch_attempt(minute):
                finished_minute = minute
                time.sleep(0.1)
                continue

            monotonic_now = time.monotonic()
            if monotonic_now - last_attempt < 1.0:  # Throttled attempt limit (2.0 -> 1.0s)
                time.sleep(0.05)
                continue

            last_attempt = monotonic_now
            evaluate_and_dispatch_all(minute)

            attempted = minute_has_dispatch_attempt(minute)
            if attempted:
                finished_minute = minute

            time.sleep(0.25)  # Fast poll sleep (0.5 -> 0.25s)

        except Exception:
            logger.exception("Scan worker failed.")
            time.sleep(0.5)


# ============================================================
# ENGINE STARTUP & HYPOTHETICAL OUTCOMES
# ============================================================

def run_engine():
    try:
        deriv_client.start()
        wait_start = time.time()
        while not deriv_client.is_connected and time.time() - wait_start < 10:
            time.sleep(0.5)

        resync_historical_candles()
        ready.set()
        logger.info("Data engine initialized and seeded (1M+5M). Signals active.")
    except Exception:
        logger.exception("Data engine failed to start.")


def _prune_dispatch_ledger(older_than_seconds: int = 86400):
    conn = get_db_connection()
    try:
        cutoff = int(time.time()) - older_than_seconds
        conn.execute("DELETE FROM dispatch_ledger_v2 WHERE target_epoch < ?", (cutoff,))
        conn.commit()
    except Exception:
        logger.exception("Dispatch ledger prune failed.")
    finally:
        conn.close()


def run_outcome_worker():
    last_prune = 0.0
    while True:
        time.sleep(10)
        now_monotonic = time.monotonic()
        if now_monotonic - last_prune > 3600:
            _prune_dispatch_ledger()
            last_prune = now_monotonic

        conn = None
        try:
            conn = get_db_connection()
            rows = conn.execute(
                """
                SELECT signal_id, symbol, candle_epoch, direction, entry_reference_price
                FROM signal_history
                WHERE result = 'PENDING'
                """
            ).fetchall()

            now = deriv_client.get_server_time()

            for signal_id, symbol, target, direction, entry in rows:
                exit_candle_epoch = int(target) + EXPIRY_SECONDS - 60
                if not entry or symbol not in candle_managers or now < exit_candle_epoch + 60 + 5:
                    continue

                df = history_snapshot(symbol)
                if df.empty or "time" not in df.columns:
                    continue

                match = df[df["time"] == exit_candle_epoch]
                if match.empty:
                    continue

                exit_price = float(match.iloc[-1]["close"])
                difference = exit_price - float(entry)

                if direction == "PUT":
                    difference = -difference
                elif direction != "CALL":
                    continue

                outcome = "WIN" if difference > 0 else ("LOSS" if difference < 0 else "TIE")

                conn.execute(
                    """
                    UPDATE signal_history
                    SET result = ?, exit_reference_price = ?
                    WHERE signal_id = ? AND result = 'PENDING'
                    """,
                    (outcome, exit_price, signal_id),
                )
            conn.commit()

        except Exception:
            logger.exception("Outcome calculation failed.")
        finally:
            if conn is not None:
                conn.close()


def start_background_threads_once():
    global _threads_started
    with _threads_lock:
        if _threads_started:
            return
        _threads_started = True

        workers = (
            ("DerivEngine", run_engine),
            ("ScanWorker", run_scan_worker),
            ("HistoryWorker", run_history_worker),
            ("OutcomeWorker", run_outcome_worker),
        )

        for name, function in workers:
            threading.Thread(target=function, daemon=True, name=name).start()


@app.before_request
def before_request_func():
    start_background_threads_once()


# ============================================================
# FLASK WEB INTERFACE & METRICS
# ============================================================

@app.route("/", methods=["GET", "HEAD"])
def dashboard():
    return render_template("dashboard.html")


@app.route("/ping", methods=["GET", "HEAD"])
def ping():
    return "pong", 200


@app.route("/health", methods=["GET", "HEAD"])
def health():
    fresh = sum(live_quote(symbol) is not None for symbol in cfg.FOREX_PAIRS)
    total = len(cfg.FOREX_PAIRS)

    return jsonify({
        "status": "healthy" if ready.is_set() and fresh == total else "degraded",
        "deriv_connected": bool(deriv_client.is_connected),
        "engine_initialized": ready.is_set(),
        "fresh_pairs": fresh,
        "total_pairs": total,
        "signals_enabled": cfg.SIGNALS_ENABLED,
        "auto_disabled_pairs": sorted(auto_disabled_symbols),
        "expiry_seconds": EXPIRY_SECONDS,
        "payout_percent": PAYOUT_PERCENT,
    })


@app.route("/api/dashboard", methods=["GET", "HEAD"])
def api_dashboard():
    fresh = sum(live_quote(symbol) is not None for symbol in cfg.FOREX_PAIRS)
    is_connected = bool(deriv_client.is_connected and ready.is_set() and fresh > 0)

    return jsonify({
        "status": "online" if ready.is_set() else "starting",
        "connected": is_connected,
        "deriv_connected": bool(deriv_client.is_connected),
        "performance": get_today_performance(),
        "performance_basis": "hypothetical Deriv reference prices",
        "active_pairs": list(cfg.FOREX_PAIRS.values()),
        "auto_disabled_pairs": sorted(auto_disabled_symbols),
    })


@app.route("/api/evaluations", methods=["GET", "HEAD"])
def api_evaluations():
    with state_lock:
        return jsonify(dict(latest_evaluations))


def signal_rows(active_only=False):
    conn = get_db_connection()
    try:
        query = """
            SELECT signal_id, display_pair, timeframe, signal_timestamp_bdt,
                   direction, score, quality, bias_15m, entry_reference_price,
                   exit_reference_price, result, candle_epoch, payout_percent,
                   expiry_seconds
            FROM signal_history
        """
        if active_only:
            query += " WHERE result = 'PENDING'"
        query += " ORDER BY candle_epoch DESC LIMIT 50"
        rows = conn.execute(query).fetchall()
    finally:
        conn.close()

    return [
        {
            "signal_id": row[0],
            "pair": row[1],
            "timeframe": row[2],
            "timestamp": row[3],
            "direction": row[4],
            "score": row[5],
            "quality": row[6],
            "bias": row[7],
            "price": row[8],
            "entry_price": row[8],
            "exit_price": row[9],
            "result": row[10],
            "target_candle_epoch": row[11],
            "payout_percent": row[12],
            "expiry_seconds": row[13],
            "performance_basis": "hypothetical Deriv reference prices",
        }
        for row in rows
    ]


@app.route("/api/active-signals", methods=["GET", "HEAD"])
@app.route("/api/signals/active", methods=["GET", "HEAD"])
def api_active_signals():
    return jsonify(signal_rows(active_only=True))


@app.route("/api/history", methods=["GET", "HEAD"])
def api_history():
    return jsonify(signal_rows())


@app.route("/api/performance", methods=["GET", "HEAD"])
def api_performance():
    return jsonify(get_today_performance())


@app.route("/api/pairs", methods=["GET", "HEAD"])
def api_pairs():
    return jsonify(cfg.FOREX_PAIRS)


start_background_threads_once()


if __name__ == "__main__":
    start_background_threads_once()
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000)),
        debug=False,
    )
