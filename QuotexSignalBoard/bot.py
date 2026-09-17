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

    with state_lock:
        quote = None
        for v in variants:
            if v in live_state:
                quote = live_state[v]
                break

        manager = candle_managers.get(symbol)
        if manager:
            last_c = manager.get_latest_closed_candle("1M")
            if last_c:
                now_epoch = deriv_client.get_server_time()
                if quote is None or (now_epoch - quote["epoch"] > 120):
                    quote = {
                        "epoch": int(last_c.close_epoch),
                        "price": float(last_c.close),
                        "receipt": time.monotonic()
                    }

        if quote is None:
            return None
        quote = dict(quote)

    return quote


def history_snapshot(symbol):
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
                "receipt": time.monotonic()
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

    message = (
        "⚡️ MARKET ANALYSIS SIGNAL ⚡️\n\n"
        f"📊 Pair: {candidate['display_name']}\n"
        f"🎯 Action: {candidate['direction']}\n"
        f"⭐️ Score: {candidate['score']}/10 ({candidate['quality']})\n"
        f"⏰ Expiry: {expiry.strftime('%H:%M:%S')} {getattr(cfg, 'TIMEZONE_NAME', 'Asia/Dhaka')}\n"
        f"📈 5M Trend: {details.get('trend_5m', 'UNAVAILABLE')}\n"
        f"💡 Reason: {details.get('score_reason', 'N/A')}\n\n"
        "Execution on Deriv quotation feed."
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
            pass

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
                pass

    return status


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
                manager = candle_managers.get(symbol)
                raw_df5 = manager.get_closed_history("5M") if manager else pd.DataFrame()
                raw_df15 = manager.get_closed_history("15M") if manager else pd.DataFrame()
                df = prepare_history(raw_df) if not raw_df.empty else raw_df
                df5 = prepare_history(raw_df5) if not raw_df5.empty else raw_df5
                df15 = prepare_history(raw_df15) if not raw_df15.empty else raw_df15

                if not df.empty and "time" in df.columns:
                    df = df[df["time"] <= target_epoch].reset_index(drop=True)

                if df.empty or len(df) < 15:
                    report["score_reason"] = "LATEST_CLOSED_CANDLE_MISSING"
                    reports[display] = report
                    continue

                direction, score, quality, details = evaluate_strategy(df, df5, df15)

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

        missing_count = sum(
            1 for report in reports.values()
            if report.get("score_reason") == "LATEST_CLOSED_CANDLE_MISSING"
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
        jobs = []
        for symbol in cfg.FOREX_PAIRS:
            jobs.extend([
                {"key": f"{symbol}:1M", "symbol": symbol, "count": getattr(cfg, "CANDLE_HISTORY_LIMIT", 300), "granularity": 60},
                {"key": f"{symbol}:5M", "symbol": symbol, "count": 100, "granularity": 300},
                {"key": f"{symbol}:15M", "symbol": symbol, "count": 100, "granularity": 900},
            ])

        histories = deriv_client.fetch_historical_candles_batch_sync(jobs)
        server_epoch = int(deriv_client.fetch_server_epoch_sync())
        updated = 0

        for symbol, manager in candle_managers.items():
            raw = histories.get(f"{symbol}:1M", [])
            if not raw:
                continue

            with state_lock:
                manager.seed_historical_candles("1M", raw, server_epoch)
                manager.seed_historical_candles("5M", histories.get(f"{symbol}:5M", []), server_epoch)
                manager.seed_historical_candles("15M", histories.get(f"{symbol}:15M", []), server_epoch)
                if raw:
                    last_c = raw[-1]
                    clean = symbol.replace("frx", "").replace("/", "").upper()
                    ep = int(last_c.get("epoch") or last_c.get("time") or server_epoch)
                    cl = float(last_c.get("close", 0))
                    for k in [symbol, clean, f"frx{clean}"]:
                        live_state[k] = {"epoch": ep, "price": cl, "receipt": time.monotonic()}
            updated += 1

        logger.info("M1 history refreshed for %s/%s pairs.", updated, len(cfg.FOREX_PAIRS))

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

            # Final entry signal is allowed only during the first ~2 seconds
            # of the new server-defined 1M candle.
            if second > 2.2:
                finished_minute = minute
                time.sleep(0.5)
                continue

            if second < 0.8:
                time.sleep(0.1)
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

        if not deriv_client.is_connected:
            logger.error("Deriv connection timeout; engine will continue in degraded mode.")

        # Do not block the signal worker on the initial 16-pair history fetch.
        # The scan worker can start immediately and history will be seeded in
        # the background as soon as the Deriv connection is available.
        ready.set()
        logger.info("Data engine initialized and history seeded. Signals are now active.")

        try:
            resync_historical_candles()
        except Exception:
            logger.exception("Initial history seed failed; retrying via history worker.")
    except Exception:
        logger.exception("Data engine failed to start.")


# ============================================================
# HYPOTHETICAL OUTCOMES
# ============================================================

def run_outcome_worker():
    while True:
        time.sleep(10)
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
    rows = []
    for symbol, display in cfg.FOREX_PAIRS.items():
        quote = live_quote(symbol)
        rows.append({
            "symbol": symbol,
            "display": display,
            "price": quote["price"] if quote else None,
            "epoch": quote["epoch"] if quote else None,
            "active": quote is not None,
        })
    return jsonify(rows)


if __name__ == "__main__":
    start_background_threads_once()
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000)),
        debug=False,
    )
