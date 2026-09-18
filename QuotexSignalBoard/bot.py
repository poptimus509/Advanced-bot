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
# LOGGING AND APPLICATION (EXPOSED GLOBALLY FOR GUNICORN)
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger("QuotexSignalBoard")

# This must be defined globally at module level for 'bot:app'
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
history_diagnostic_seen = set()

ready = threading.Event()

_threads_started = False
_threads_lock = threading.Lock()


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
            cluster=getattr(cfg, "PUSHER_CLUSTER", "mt1"),
            ssl=True,
        )
    except Exception:
        logger.error("Pusher initialization failed.")


# ============================================================
# PERSISTENT DELIVERY DEDUPLICATION
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


def used_setup(symbol, setup_id):
    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT 1
            FROM dispatch_ledger_v2
            WHERE symbol = ? AND setup_id = ?
            """,
            (symbol, setup_id),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def minute_has_dispatch_attempt(target_epoch):
    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT 1
            FROM dispatch_ledger_v2
            WHERE target_epoch = ?
            """,
            (target_epoch,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def reserve_dispatch(symbol, setup_id, target_epoch):
    conn = get_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO dispatch_ledger_v2 (
                target_epoch,
                symbol,
                setup_id,
                status
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


def set_dispatch_status(target_epoch, status):
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
# SYMBOL HELPER & NORMALIZER
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


# ============================================================
# LIVE TICK COLLECTION
# ============================================================

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

                    # Mark older windows complete only when we actually saw
                    # the next minute arrive. A window that started midway
                    # through a minute remains partial instead of being
                    # mislabeled as fully observed.
                    for old_boundary, old_window in list(windows.items()):
                        if old_boundary < boundary and not old_window.get("complete", False):
                            old_window["complete"] = bool(
                                old_window.get("first", old_boundary + 999) <= old_boundary + 2
                            )

                    window = windows.get(boundary)
                    if window is None:
                        window = {
                            "count": 1,
                            "first": epoch,
                            "last": epoch,
                            "last_receipt": monotonic_now,
                            "complete": False,
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
    """Return only a genuinely recent live tick; never a historical close."""
    variants = get_symbol_variants(symbol)
    stale_after = getattr(cfg, "STALE_TICK_THRESHOLD_SEC", 25.0)

    with state_lock:
        quote = None
        for variant in variants:
            if variant in live_state:
                quote = dict(live_state[variant])
                break

    if quote is None:
        return None

    now_epoch = deriv_client.get_server_time()
    if now_epoch - quote["epoch"] > stale_after:
        return None
    if time.monotonic() - quote["receipt"] > stale_after:
        return None
    return quote

def history_snapshot(symbol, timeframe="1M"):
    if timeframe != "1M":
        # 5M/15M candles carry no live tick-verification window; just
        # hand back the closed history as CandleManager built it.
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
        for variant in variants:
            if variant in live_state:
                quote = dict(live_state[variant])
                break

        manager = candle_managers.get(symbol)
        candle = manager.get_latest_closed_candle("1M") if manager else None
        last_close = int(candle.close_epoch) if candle else None
        manager_diag = manager.diagnostics("1M") if manager else {}

    now = deriv_client.get_server_time()
    return {
        "last_tick_epoch": quote["epoch"] if quote else None,
        "tick_age_seconds": round(now - quote["epoch"], 2) if quote else None,
        "receipt_age_seconds": round(time.monotonic() - quote["receipt"], 2) if quote else None,
        "last_candle_close_epoch": last_close,
        "history_count_1m": manager_diag.get("count", 0),
        "history_last_epoch_1m": manager_diag.get("last_epoch"),
        "history_gap_count_1m": manager_diag.get("abnormal_gap_count", 0),
    }



# ============================================================
# TELEGRAM DELIVERY
# ============================================================

def send_telegram_alert(candidate, target_epoch):
    if not getattr(cfg, "TELEGRAM_ENABLED", False):
        return "DISABLED"

    if not cfg.TELEGRAM_BOT_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        logger.error("Telegram configuration missing: check token and chat/channel ID.")
        return "CONFIG_ERROR"

    tz = pytz.timezone(getattr(cfg, "TIMEZONE_NAME", "Asia/Dhaka"))
    expiry = datetime.datetime.fromtimestamp(target_epoch + 60, tz)
    details = candidate["details"]

    trend_5m = details.get("trend_5m", "UNAVAILABLE")
    trend_line = (
        f"📈 5M Trend: {trend_5m}\n"
        if trend_5m != "UNAVAILABLE"
        else "📈 5M Trend: not available (skipped - insufficient 5M history)\n"
    )

    # This bot analyzes the Deriv (frx) feed. Quotex, especially its OTC
    # instruments, generates its own candles that can and do diverge from
    # Deriv's. Every message says so explicitly instead of implying the
    # analysis is on the exact feed being traded.
    message = (
        "⚡️ MARKET ANALYSIS SIGNAL ⚡️\n\n"
        f"📊 Pair: {candidate['display_name']}\n"
        f"🎯 Action: {candidate['direction']}\n"
        f"⭐️ Score: {candidate['score']}/8 ({candidate['quality']})\n"
        f"⏰ Expiry: {expiry.strftime('%H:%M:%S')} {getattr(cfg, 'TIMEZONE_NAME', 'Asia/Dhaka')}\n"
        f"{trend_line}"
        f"💡 Reason: {details.get('score_reason', 'N/A')}\n\n"
        "⚠️ Based on the Deriv quotation feed, not Quotex's own candles.\n"
        "This is not financial advice; no signal here guarantees a profitable trade."
    )

    url = f"https://api.telegram.org/bot{cfg.TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        response = requests.post(
            url,
            json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": message},
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
# SIGNAL RECORDING AND DISPATCH
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
                bias_15m, entry_reference_price, entry_reference_timestamp, created_at, result
            )
            VALUES (?, ?, ?, '1M', ?, ?, ?, ?, ?, ?, 'NOT_USED', ?, ?, ?, 'PENDING')
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
            ),
        )
        conn.commit()
    finally:
        conn.close()


def dispatch_best_signal(candidate, target_epoch):
    quote = live_quote(candidate["symbol"])
    if quote is None:
        logger.warning(
            "Dispatch blocked for %s: no fresh live quote within %.1fs",
            candidate["symbol"],
            float(getattr(cfg, "STALE_TICK_THRESHOLD_SEC", 25.0)),
        )
        return "NO_FRESH_QUOTE"

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
            # A Telegram message already went out, but the DB record
            # failed - this used to fail silently, meaning the signal
            # was invisible to /api/history and win-rate stats forever.
            # It's still not retried here (that's a bigger change), but
            # it is now at least logged so the gap is diagnosable.
            logger.exception(
                "save_signal failed after SENT Telegram alert for %s %s",
                candidate["symbol"], candidate["direction"],
            )

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
                        "expiry_epoch": target_epoch + 60,
                    },
                )
            except Exception:
                logger.exception("Pusher trigger failed for %s", candidate["symbol"])

    return status


# ============================================================
# HISTORY DIAGNOSTICS
# ============================================================

def _history_counts(df):
    if df is None or getattr(df, "empty", True) or "time" not in df.columns:
        return 0, None, None, 0
    times = [int(x) for x in df["time"].tolist()]
    gaps = [times[i] - times[i - 1] for i in range(1, len(times))]
    abnormal = [gap for gap in gaps if gap != 60]
    return len(times), times[0], times[-1], (abnormal[-1] if abnormal else 0)


def _log_history_problem_once(symbol, target_epoch, reason, raw_df, prepared_df, expected_last_epoch):
    if not getattr(cfg, "HISTORY_DIAGNOSTICS", True):
        return
    key = (symbol, int(target_epoch), reason)
    with state_lock:
        if key in history_diagnostic_seen:
            return
        history_diagnostic_seen.add(key)
        # Bound memory usage to roughly the latest few minutes.
        if len(history_diagnostic_seen) > 500:
            cutoff = int(target_epoch) - 600
            history_diagnostic_seen_copy = {x for x in history_diagnostic_seen if x[1] >= cutoff}
            history_diagnostic_seen.clear()
            history_diagnostic_seen.update(history_diagnostic_seen_copy)

    raw_count, raw_first, raw_last, raw_gap = _history_counts(raw_df)
    prep_count, prep_first, prep_last, prep_gap = _history_counts(prepared_df)
    logger.warning(
        "HISTORY_DIAG %s reason=%s expected_last=%s raw_count=%s raw_first=%s raw_last=%s "
        "raw_latest_gap=%s prepared_count=%s prepared_first=%s prepared_last=%s prepared_latest_gap=%s",
        symbol, reason, expected_last_epoch, raw_count, raw_first, raw_last, raw_gap,
        prep_count, prep_first, prep_last, prep_gap,
    )


# ============================================================
# ALL-PAIR EVALUATION AND RANKING
# ============================================================

def evaluate_and_dispatch_all(target_epoch):
    if not evaluation_lock.acquire(blocking=False):
        return

    try:
        candidates = []
        reports = {}
        timestamp = datetime.datetime.now(pytz.timezone(getattr(cfg, "TIMEZONE_NAME", "Asia/Dhaka"))).isoformat()

        for symbol, display in cfg.FOREX_PAIRS.items():
            report = {
                "timestamp": timestamp,
                "direction": "NO_TRADE",
                "score": 0,
                "quality": "WAIT",
                "bias": "NOT_USED",
                "target_candle_epoch": target_epoch,
                "expected_close_epoch": target_epoch,
                "delivery": "NOT_SENT",
            }

            try:
                report.update(feed_diagnostics(symbol))

                raw_df = history_snapshot(symbol)
                expected_last_epoch = int(target_epoch) - 60

                if raw_df.empty or "time" not in raw_df.columns:
                    report["score_reason"] = "NO_1M_HISTORY"
                    _log_history_problem_once(
                        symbol, target_epoch, report["score_reason"], raw_df, raw_df, expected_last_epoch
                    )
                    reports[display] = report
                    continue

                # Never analyze the currently-forming minute. The decision
                # for target_epoch must be based on the candle that closed
                # exactly at target_epoch.
                raw_df = raw_df[raw_df["time"] <= expected_last_epoch].reset_index(drop=True)
                df = prepare_history(raw_df) if not raw_df.empty else raw_df

                min_history = getattr(cfg, "MIN_1M_HISTORY", 20)
                if len(raw_df) < min_history:
                    report["score_reason"] = "INSUFFICIENT_RAW_1M_HISTORY"
                    _log_history_problem_once(
                        symbol, target_epoch, report["score_reason"], raw_df, df, expected_last_epoch
                    )
                    reports[display] = report
                    continue

                if df.empty or len(df) < min_history:
                    report["score_reason"] = "INSUFFICIENT_CONTIGUOUS_1M_HISTORY"
                    _log_history_problem_once(
                        symbol, target_epoch, report["score_reason"], raw_df, df, expected_last_epoch
                    )
                    reports[display] = report
                    continue

                latest_analysis_epoch = int(df.iloc[-1]["time"])
                if latest_analysis_epoch != expected_last_epoch:
                    report["score_reason"] = "LATEST_CLOSED_CANDLE_MISSING"
                    _log_history_problem_once(
                        symbol, target_epoch, report["score_reason"], raw_df, df, expected_last_epoch
                    )
                    reports[display] = report
                    continue

                # Real 5M regime + ADX context. If either is unavailable
                # or too short, evaluate_strategy's own defensive checks
                # skip the corresponding gate rather than guessing - this
                # never raises and never blocks the 1M-only evaluation.
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

        history_problem_reasons = {
            "NO_1M_HISTORY",
            "INSUFFICIENT_RAW_1M_HISTORY",
            "INSUFFICIENT_CONTIGUOUS_1M_HISTORY",
            "LATEST_CLOSED_CANDLE_MISSING",
        }
        missing_count = sum(
            1 for report in reports.values()
            if report.get("score_reason") in history_problem_reasons
        )

        logger.info(
            "Scan: %s pairs, %s candidates, missing_candles=%s, signals_enabled=%s",
            len(reports),
            len(candidates),
            missing_count,
            cfg.SIGNALS_ENABLED,
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
# HISTORICAL DATA REFRESH
# ============================================================

def resync_historical_candles():
    if not history_lock.acquire(blocking=False):
        return

    try:
        jobs = [
            {
                "key": f"{symbol}:1M",
                "symbol": symbol,
                "count": getattr(cfg, "CANDLE_HISTORY_LIMIT", 100),
                "granularity": 60,
            }
            for symbol in cfg.FOREX_PAIRS
        ]

        histories = deriv_client.fetch_historical_candles_batch_sync(jobs)
        server_epoch = int(deriv_client.fetch_server_epoch_sync())
        updated = 0

        stored_ok = 0
        for symbol, manager in candle_managers.items():
            raw = histories.get(f"{symbol}:1M", [])
            if not raw:
                logger.warning("History refresh %s: API returned 0 candles", symbol)
                continue

            with state_lock:
                manager.seed_historical_candles("1M", raw, server_epoch)
                diag_1m = manager.diagnostics("1M")
                diag_5m = manager.diagnostics("5M")

            updated += 1
            if diag_1m.get("count", 0) >= getattr(cfg, "MIN_1M_HISTORY", 20):
                stored_ok += 1

            if getattr(cfg, "HISTORY_DIAGNOSTICS", True):
                logger.info(
                    "History %s: api=%s stored1m=%s last1m=%s gaps1m=%s stored5m=%s last5m=%s",
                    symbol,
                    len(raw),
                    diag_1m.get("count", 0),
                    diag_1m.get("last_epoch"),
                    diag_1m.get("abnormal_gap_count", 0),
                    diag_5m.get("count", 0),
                    diag_5m.get("last_epoch"),
                )

        logger.info(
            "M1 history refreshed for %s/%s pairs; %s/%s have >=%s stored 1M candles.",
            updated,
            len(cfg.FOREX_PAIRS),
            stored_ok,
            len(cfg.FOREX_PAIRS),
            getattr(cfg, "MIN_1M_HISTORY", 20),
        )

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
# SCAN RETRIES WITHIN THE ENTRY WINDOW
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
                time.sleep(0.5)
                continue

            # A signal dispatched at, say, second 40 of the minute is
            # entering ~40 seconds into that candle's move, not at its
            # open - yet the recorded entry_reference_price and the
            # Telegram message both implicitly assume a fresh entry. The
            # previous window (up to second 45) allowed exactly that.
            # Keeping the window tight to the start of the minute keeps
            # "entry price" honest relative to what was actually analyzed.
            scan_delay = getattr(cfg, "SCAN_DELAY_SECONDS", 2.0)
            max_delay = getattr(cfg, "MAX_ENTRY_DELAY_SECONDS", 10.0)

            if second > max_delay:
                finished_minute = minute
                time.sleep(0.5)
                continue

            if second < scan_delay:
                time.sleep(0.5)
                continue

            if minute_has_dispatch_attempt(minute):
                finished_minute = minute
                time.sleep(0.5)
                continue

            monotonic_now = time.monotonic()
            if monotonic_now - last_attempt < 2.0:
                time.sleep(0.3)
                continue

            last_attempt = monotonic_now
            evaluate_and_dispatch_all(minute)

            attempted = minute_has_dispatch_attempt(minute)
            if attempted:
                finished_minute = minute

            time.sleep(0.5)

        except Exception:
            logger.exception("Scan worker failed.")
            time.sleep(1)


# ============================================================
# ENGINE STARTUP
# ============================================================

def run_engine():
    try:
        deriv_client.start()
        wait_start = time.time()
        while not deriv_client.is_connected and time.time() - wait_start < 10:
            time.sleep(0.5)

        resync_historical_candles()
        ready.set()
        logger.info("Data engine initialized and history seeded. Signals are now active.")
    except Exception:
        logger.exception("Data engine failed to start.")


# ============================================================
# HYPOTHETICAL OUTCOMES
# ============================================================

def _prune_dispatch_ledger(older_than_seconds: int = 86400):
    # dispatch_ledger_v2 previously grew forever - one row per (symbol,
    # target_epoch) attempted, indefinitely. It only needs to remember
    # enough history to prevent same-minute duplicate dispatch, so a
    # rolling 24h window is more than sufficient.
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
                if not entry or symbol not in candle_managers or now < int(target) + 60:
                    continue

                df = history_snapshot(symbol)
                if df.empty or "time" not in df.columns:
                    continue

                match = df[df["time"] == int(target)]
                if match.empty:
                    continue

                exit_price = float(match.iloc[-1]["close"])
                difference = exit_price - float(entry)

                if direction == "PUT":
                    difference = -difference
                elif direction != "CALL":
                    continue

                if difference > 0:
                    outcome = "WIN"
                elif difference < 0:
                    outcome = "LOSS"
                else:
                    outcome = "TIE"

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
            logger.exception("Hypothetical outcome calculation failed.")
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
    # Kept as a safety net (in case the process-start call below ever
    # fails to run for some reason), but this should already be a no-op
    # in normal operation - see start_background_threads_once() call at
    # module load time, right after the Flask app and routes are wired.
    start_background_threads_once()


# ============================================================
# WEB ROUTES
# ============================================================

@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/health")
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
    })


@app.route("/api/dashboard")
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
    })


@app.route("/api/evaluations")
def api_evaluations():
    with state_lock:
        return jsonify(dict(latest_evaluations))


def signal_rows(active_only=False):
    conn = get_db_connection()
    try:
        query = """
            SELECT signal_id, display_pair, timeframe, signal_timestamp_bdt,
                   direction, score, quality, bias_15m, entry_reference_price,
                   exit_reference_price, result, candle_epoch
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
            "performance_basis": "hypothetical Deriv reference prices",
        }
        for row in rows
    ]


@app.route("/api/active-signals")
@app.route("/api/signals/active")
def api_active_signals():
    return jsonify(signal_rows(active_only=True))


@app.route("/api/history")
def api_history():
    return jsonify(signal_rows())


@app.route("/api/performance")
def api_performance():
    return jsonify(get_today_performance())


@app.route("/api/pairs")
def api_pairs():
    return jsonify(cfg.FOREX_PAIRS)


@app.route("/api/history-diagnostics")
def api_history_diagnostics():
    now = deriv_client.get_server_time()
    current_minute = int(now // 60) * 60
    expected_last = current_minute - 60
    output = {}

    for symbol, display in cfg.FOREX_PAIRS.items():
        manager = candle_managers.get(symbol)
        raw = history_snapshot(symbol) if manager else pd.DataFrame()
        filtered = raw[raw["time"] <= expected_last].reset_index(drop=True) if (not raw.empty and "time" in raw.columns) else raw
        prepared = prepare_history(filtered) if not filtered.empty else filtered
        output[display] = {
            "symbol": symbol,
            "expected_last_epoch": expected_last,
            "manager_1m": manager.diagnostics("1M") if manager else {},
            "manager_5m": manager.diagnostics("5M") if manager else {},
            "raw_count_before_prepare": int(len(filtered)) if filtered is not None else 0,
            "prepared_count": int(len(prepared)) if prepared is not None else 0,
            "prepared_last_epoch": (
                int(prepared.iloc[-1]["time"])
                if prepared is not None and not prepared.empty and "time" in prepared.columns
                else None
            ),
            "latest_candle_exact": bool(
                prepared is not None
                and not prepared.empty
                and "time" in prepared.columns
                and int(prepared.iloc[-1]["time"]) == expected_last
            ),
        }

    return jsonify({
        "server_epoch": now,
        "current_minute_epoch": current_minute,
        "expected_last_closed_epoch": expected_last,
        "pairs": output,
    })


# Start the worker threads (Deriv engine, scan/telegram, history resync,
# outcome tracking) as soon as the module is loaded - not on the first
# incoming HTTP request. The previous version only started them inside
# @app.before_request, which meant:
#   1. A deployed process with no traffic yet never scanned or sent
#      any signal, silently, until someone hit the URL.
#   2. Render/most PaaS health checks hit "/" or "/health" almost
#      immediately, which made this look like it worked in testing,
#      while still being fragile.
# start_background_threads_once() is idempotent (guarded by
# _threads_started + a lock), so this is safe even though
# @app.before_request above still also calls it as a fallback.
#
# IMPORTANT - this does NOT make it safe to run with multiple gunicorn
# worker processes. Each worker process is a separate Python process
# with its own copy of these threads, in-memory candle state, and
# dispatch dedup set, so N workers means N independent scanners each
# capable of sending its own Telegram message for the same signal.
# Deploy with a single worker (see the Render start command in
# CORRECTIONS.md) until the scan/dispatch logic is moved to a separate
# always-single-instance process.
start_background_threads_once()


if __name__ == "__main__":
    start_background_threads_once()
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000)),
        debug=False,
    )
