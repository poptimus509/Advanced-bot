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
from flask import Flask, jsonify, render_template

import config as cfg

from core.events import EventDispatcher
from data.candle_manager import CandleManager
from data.deriv_client import DerivClient
from database import get_db_connection, init_db
from monitor import get_today_performance
from strategy import evaluate_strategy, prepare_history


# ============================================================
# LOGGING AND APPLICATION
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


# ============================================================
# PUSHER
# ============================================================

pusher_client = None

if (
    cfg.PUSHER_ENABLED
    and cfg.PUSHER_APP_ID
    and cfg.PUSHER_KEY
    and cfg.PUSHER_SECRET
):
    try:
        pusher_client = pusher.Pusher(
            app_id=cfg.PUSHER_APP_ID,
            key=cfg.PUSHER_KEY,
            secret=cfg.PUSHER_SECRET,
            cluster=cfg.PUSHER_CLUSTER,
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
            boundary = epoch // 60 * 60

            with state_lock:
                previous = live_state.get(symbol)

                if previous and epoch < previous["epoch"]:
                    return []

                windows = tick_windows[symbol]
                window = windows.get(boundary)

                if window is None:
                    window = {
                        "count": 0,
                        "first": epoch,
                        "last": epoch,
                        "last_receipt": monotonic_now,
                        "complete": epoch - boundary <= 3,
                    }
                    windows[boundary] = window

                elif (
                    epoch - window["last"]
                    > cfg.STALE_TICK_THRESHOLD_SEC
                    or monotonic_now - window["last_receipt"]
                    > cfg.STALE_TICK_THRESHOLD_SEC
                ):
                    window["complete"] = False

                window["count"] += 1
                window["last"] = epoch
                window["last_receipt"] = monotonic_now

                live_state[symbol] = {
                    "epoch": epoch,
                    "price": price,
                    "receipt": monotonic_now,
                }

                oldest_allowed = (
                    boundary - cfg.CANDLE_HISTORY_LIMIT * 60
                )

                for old_epoch in list(windows):
                    if old_epoch < oldest_allowed:
                        del windows[old_epoch]

                return manager.process_tick(
                    epoch,
                    price,
                    receipt_time,
                )

        except Exception:
            logger.exception(
                "Tick processing failed for %s", symbol
            )
            return []

    return handle


for symbol in cfg.FOREX_PAIRS:
    manager = CandleManager(
        symbol=symbol,
        event_dispatcher=event_dispatcher,
        max_history=cfg.CANDLE_HISTORY_LIMIT,
    )

    candle_managers[symbol] = manager
    tick_windows[symbol] = {}

    deriv_client.register_tick_handler(
        symbol,
        make_tick_handler(symbol, manager),
    )


def live_quote(symbol):
    with state_lock:
        quote = live_state.get(symbol)

        if quote is None:
            return None

        quote = dict(quote)

    if not deriv_client.is_connected:
        return None

    server_age = (
        deriv_client.get_server_time() - quote["epoch"]
    )
    receipt_age = time.monotonic() - quote["receipt"]

    if server_age < -cfg.SERVER_SYNC_TOLERANCE_SEC:
        return None

    if server_age > cfg.STALE_TICK_THRESHOLD_SEC:
        return None

    if receipt_age > cfg.STALE_TICK_THRESHOLD_SEC:
        return None

    return quote


def history_snapshot(symbol):
    with state_lock:
        df = candle_managers[symbol].get_closed_history(
            "1M"
        ).copy()

        windows = {
            epoch: dict(value)
            for epoch, value in tick_windows[symbol].items()
        }

    if "time" not in df.columns:
        return df

    def verified_count(epoch):
        epoch = int(epoch)
        window = windows.get(epoch)

        if window is None or not window["complete"]:
            return float("nan")

        end_gap = epoch + 60 - window["last"]

        if end_gap > cfg.STALE_TICK_THRESHOLD_SEC:
            return float("nan")

        return float(window["count"])

    df["verified_ticks"] = df["time"].map(verified_count)

    return df


def feed_diagnostics(symbol):
    with state_lock:
        quote = live_state.get(symbol)
        quote = dict(quote) if quote else None

        candle = candle_managers[
            symbol
        ].get_latest_closed_candle("1M")

        last_close = int(candle.close_epoch) if candle else None

    now = deriv_client.get_server_time()

    return {
        "last_tick_epoch": quote["epoch"] if quote else None,
        "tick_age_seconds": (
            round(now - quote["epoch"], 2)
            if quote else None
        ),
        "receipt_age_seconds": (
            round(time.monotonic() - quote["receipt"], 2)
            if quote else None
        ),
        "last_candle_close_epoch": last_close,
    }


# ============================================================
# TELEGRAM DELIVERY
# ============================================================

def send_telegram_alert(candidate, target_epoch):
    if not cfg.TELEGRAM_ENABLED:
        return "DISABLED"

    if not cfg.TELEGRAM_BOT_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        logger.error(
            "Telegram configuration missing: check token "
            "and chat/channel ID."
        )
        return "CONFIG_ERROR"

    tz = pytz.timezone(cfg.TIMEZONE_NAME)
    expiry = datetime.datetime.fromtimestamp(
        target_epoch + 60, tz
    )
    details = candidate["details"]

    message = (
        "MARKET ANALYSIS SIGNAL\n\n"
        f"Pair: {candidate['display_name']}\n"
        f"Action: {candidate['direction']}\n"
        f"Setup score: {candidate['score']}/10 "
        "(not win probability)\n"
        f"Expiry: {expiry.strftime('%H:%M:%S')} "
        f"{cfg.TIMEZONE_NAME}\n"
        f"M1 Structure: {details['structure']}\n"
        f"M5 Context: {details.get('trend_5m', 'UNAVAILABLE')}\n"
        f"Reason: {details['score_reason']}\n\n"
        "Based on Deriv prices. Broker execution prices may differ."
    )

    url = (
        f"https://api.telegram.org/"
        f"bot{cfg.TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    try:
        response = requests.post(
            url,
            json={
                "chat_id": cfg.TELEGRAM_CHAT_ID,
                "text": message,
            },
            timeout=(2, 3),
        )

        try:
            body = response.json()
        except ValueError:
            logger.error(
                "Telegram returned unreadable response; HTTP %s",
                response.status_code,
            )
            return "UNKNOWN"

        if not isinstance(body, dict):
            logger.error("Telegram returned an unexpected response.")
            return "UNKNOWN"

        if response.status_code == 200 and body.get("ok") is True:
            logger.info(
                "Telegram accepted %s %s; message_id=%s",
                candidate["display_name"],
                candidate["direction"],
                body.get("result", {}).get("message_id"),
            )
            return "SENT"

        logger.error(
            "Telegram rejected message: HTTP %s, error_code=%s",
            response.status_code,
            body.get("error_code"),
        )
        return "REJECTED"

    except requests.RequestException as exc:
        logger.error(
            "Telegram delivery uncertain: %s",
            type(exc).__name__,
        )
        return "UNKNOWN"


# ============================================================
# SIGNAL RECORDING AND DISPATCH
# ============================================================

def save_signal(candidate, target_epoch, entry_price):
    tz = pytz.timezone(cfg.TIMEZONE_NAME)
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    local_now = utc_now.astimezone(tz)

    signal_id = (
        f"{candidate['symbol']}_1M_{target_epoch}"
    )

    conn = get_db_connection()

    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO signal_history (
                signal_id,
                symbol,
                display_pair,
                timeframe,
                candle_epoch,
                signal_timestamp_utc,
                signal_timestamp_bdt,
                direction,
                score,
                quality,
                bias_15m,
                entry_reference_price,
                entry_reference_timestamp,
                result,
                created_at
            )
            VALUES (
                ?, ?, ?, '1M', ?, ?, ?, ?, ?, ?, 'NOT_USED',
                ?, ?, 'PENDING', ?
            )
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
    now = deriv_client.get_server_time()

    if not 0 <= now - target_epoch <= cfg.MAX_ENTRY_DELAY_SECONDS:
        return "ENTRY_WINDOW_EXPIRED"

    quote = live_quote(candidate["symbol"])

    if quote is None:
        return "NO_FRESH_QUOTE"

    drift = abs(
        quote["price"] - candidate["analysis_close"]
    )

    max_drift = (
        candidate["details"]["atr"]
        * cfg.MAX_ENTRY_DRIFT_ATR
    )

    if drift > max_drift:
        return "PRICE_MOVED_FROM_SETUP"

    if not cfg.TELEGRAM_ENABLED:
        return "DISABLED"

    if not cfg.TELEGRAM_BOT_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        logger.error(
            "Cannot send: Telegram token or chat/channel ID missing."
        )
        return "CONFIG_ERROR"

    if not reserve_dispatch(
        candidate["symbol"],
        candidate["details"]["setup_id"],
        target_epoch,
    ):
        return "DUPLICATE"

    status = send_telegram_alert(candidate, target_epoch)
    set_dispatch_status(target_epoch, status)

    if status != "SENT":
        return status

    reference = live_quote(candidate["symbol"])

    if (
        reference is not None
        and deriv_client.get_server_time() < target_epoch + 60
    ):
        try:
            save_signal(
                candidate,
                target_epoch,
                reference["price"],
            )
        except Exception:
            logger.exception(
                "Telegram sent, but signal database save failed."
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
                    "bias": "NOT_USED",
                    "trend_5m": candidate["details"].get(
                        "trend_5m", "UNAVAILABLE"
                    ),
                    "timeframe": "1M",
                    "target_candle_epoch": target_epoch,
                    "expiry_epoch": target_epoch + 60,
                },
            )

        except Exception:
            logger.error("Pusher notification failed.")

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

        timestamp = datetime.datetime.now(
            pytz.timezone(cfg.TIMEZONE_NAME)
        ).isoformat()

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

                df = prepare_history(history_snapshot(symbol))
                df = df[
                    df["time"] < target_epoch
                ].reset_index(drop=True)

                if (
                    df.empty
                    or int(df.iloc[-1]["time"]) + 60 != target_epoch
                ):
                    report["score_reason"] = (
                        "LATEST_CLOSED_CANDLE_MISSING"
                    )
                    reports[display] = report
                    continue

                direction, score, quality, details = evaluate_strategy(
                    df
                )

                report.update({
                    "direction": direction,
                    "score": score,
                    "quality": quality,
                    "details": details.get("pa"),
                    "score_reason": details["score_reason"],
                    "structure": details["structure"],
                    "trend_5m": details.get(
                        "trend_5m", "UNAVAILABLE"
                    ),
                    "call_score": details["call_score"],
                    "put_score": details["put_score"],
                    "activity_status": details.get(
                        "activity_status"
                    ),
                    "relative_activity": details.get(
                        "relative_activity"
                    ),
                    "analysis_candle_epoch": int(
                        df.iloc[-1]["time"]
                    ),
                })

                if direction in ("CALL", "PUT"):
                    if used_setup(symbol, details["setup_id"]):
                        report["delivery"] = (
                            "SETUP_ALREADY_ATTEMPTED"
                        )

                    elif live_quote(symbol) is None:
                        report["delivery"] = "LIVE_FEED_STALE"

                    else:
                        candidates.append({
                            "symbol": symbol,
                            "display_name": display,
                            "direction": direction,
                            "score": score,
                            "quality": quality,
                            "details": details,
                            "analysis_close": float(
                                df.iloc[-1]["close"]
                            ),
                        })

            except Exception as exc:
                report["score_reason"] = (
                    f"DATA_ERROR:{type(exc).__name__}"
                )
                logger.warning(
                    "Evaluation failed for %s: %s",
                    display,
                    exc,
                )

            reports[display] = report

        candidates.sort(
            key=lambda candidate: (
                -candidate["score"],
                -candidate["details"]["rank_strength"],
                candidate["symbol"],
            )
        )

        missing_count = sum(
            report.get("score_reason")
            == "LATEST_CLOSED_CANDLE_MISSING"
            for report in reports.values()
        )

        logger.info(
            "Scan: %s pairs, %s candidates, missing_candles=%s, "
            "signals_enabled=%s",
            len(reports),
            len(candidates),
            missing_count,
            cfg.SIGNALS_ENABLED,
        )

        if candidates and cfg.SIGNALS_ENABLED:
            for candidate in candidates:
                status = dispatch_best_signal(
                    candidate,
                    target_epoch,
                )

                reports[
                    candidate["display_name"]
                ]["delivery"] = status

                if status not in (
                    "NO_FRESH_QUOTE",
                    "PRICE_MOVED_FROM_SETUP",
                ):
                    break

        elif candidates:
            reports[
                candidates[0]["display_name"]
            ]["delivery"] = "DEMO_REVIEW_ONLY"

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
                "count": cfg.CANDLE_HISTORY_LIMIT,
                "granularity": 60,
            }
            for symbol in cfg.FOREX_PAIRS
        ]

        histories = (
            deriv_client.fetch_historical_candles_batch_sync(jobs)
        )

        server_epoch = int(
            deriv_client.fetch_server_epoch_sync()
        )

        updated = 0

        for symbol, manager in candle_managers.items():
            raw = histories.get(f"{symbol}:1M", [])

            closed = [
                row for row in raw
                if int(row["epoch"]) + 60 <= server_epoch
            ]

            if not closed:
                continue

            closed.sort(key=lambda row: int(row["epoch"]))

            with state_lock:
                latest = manager.get_latest_closed_candle("1M")

                if (
                    latest is not None
                    and int(latest.epoch) > int(closed[-1]["epoch"])
                ):
                    continue

                manager.seed_historical_candles(
                    "1M",
                    closed,
                    server_epoch,
                )

            updated += 1

        logger.info(
            "M1 history refreshed for %s/%s pairs.",
            updated,
            len(cfg.FOREX_PAIRS),
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

            if (
                15 <= now % 60 < 25
                and minute != last_minute
            ):
                last_minute = minute
                resync_historical_candles()

            time.sleep(0.5)

        except Exception:
            logger.exception("History worker failed.")
            time.sleep(1)


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
                time.sleep(0.2)
                continue

            if second > cfg.MAX_ENTRY_DELAY_SECONDS:
                finished_minute = minute
                time.sleep(0.2)
                continue

            if second < cfg.SCAN_DELAY_SECONDS:
                time.sleep(0.2)
                continue

            if minute_has_dispatch_attempt(minute):
                finished_minute = minute
                time.sleep(0.2)
                continue

            with state_lock:
                latest_candles = [
                    manager.get_latest_closed_candle("1M")
                    for manager in candle_managers.values()
                ]

                histories_ready = all(
                    candle is not None
                    and int(candle.close_epoch) == minute
                    for candle in latest_candles
                )

            grace_deadline = min(
                cfg.SCAN_DELAY_SECONDS + 4,
                cfg.MAX_ENTRY_DELAY_SECONDS - 1,
            )

            if not histories_ready and second < grace_deadline:
                time.sleep(0.2)
                continue

            monotonic_now = time.monotonic()

            if monotonic_now - last_attempt < 1:
                time.sleep(0.2)
                continue

            last_attempt = monotonic_now

            evaluate_and_dispatch_all(minute)

            attempted = minute_has_dispatch_attempt(minute)

            with state_lock:
                reports = list(latest_evaluations.values())

            needs_retry = (
                not reports
                or any(
                    report.get("score_reason")
                    == "LATEST_CLOSED_CANDLE_MISSING"
                    or report.get("delivery") in (
                        "LIVE_FEED_STALE",
                        "NO_FRESH_QUOTE",
                    )
                    for report in reports
                )
            )

            if attempted or not needs_retry:
                finished_minute = minute

            time.sleep(0.2)

        except Exception:
            logger.exception("Scan worker failed.")
            time.sleep(1)


# ============================================================
# ENGINE STARTUP
# ============================================================

def run_engine():
    try:
        deriv_client.start()
        resync_historical_candles()
        ready.set()
        logger.info(
            "Data engine initialized. "
            "Fresh ticks and closed history are still required for signals."
        )
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
                SELECT
                    signal_id,
                    symbol,
                    candle_epoch,
                    direction,
                    entry_reference_price
                FROM signal_history
                WHERE result = 'PENDING'
                """
            ).fetchall()

            now = deriv_client.get_server_time()

            for (
                signal_id,
                symbol,
                target,
                direction,
                entry,
            ) in rows:
                if (
                    not entry
                    or symbol not in candle_managers
                    or now < int(target) + 60
                ):
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
            logger.exception(
                "Hypothetical outcome calculation failed."
            )

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
            threading.Thread(
                target=function,
                daemon=True,
                name=name,
            ).start()


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
    fresh = sum(
        live_quote(symbol) is not None
        for symbol in cfg.FOREX_PAIRS
    )

    total = len(cfg.FOREX_PAIRS)

    return jsonify({
        "status": (
            "healthy"
            if ready.is_set() and fresh == total
            else "degraded"
        ),
        "deriv_connected": bool(deriv_client.is_connected),
        "engine_initialized": ready.is_set(),
        "fresh_pairs": fresh,
        "total_pairs": total,
        "signals_enabled": cfg.SIGNALS_ENABLED,
    })


@app.route("/api/dashboard")
def api_dashboard():
    return jsonify({
        "status": (
            "online" if ready.is_set() else "starting"
        ),
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
            SELECT
                signal_id,
                display_pair,
                timeframe,
                signal_timestamp_bdt,
                direction,
                score,
                quality,
                bias_15m,
                entry_reference_price,
                exit_reference_price,
                result,
                candle_epoch
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
            "performance_basis": "hypothetical Deriv prices",
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


if __name__ == "__main__":
    start_background_threads_once()

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000)),
        debug=False,
    )
