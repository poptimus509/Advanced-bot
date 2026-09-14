import datetime
import logging
import math
import os
import sqlite3
import threading
import time

import pandas as pd
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("QuotexSignalBoard")

app = Flask(__name__)
init_db()

latest_evaluations = {}
evaluation_lock = threading.Lock()
state_lock = threading.RLock()
history_lock = threading.Lock()

event_dispatcher = EventDispatcher()
deriv_client = DerivClient()
candle_managers = {}

# Tick metadata is independent of historical OHLC seeding.
live_state = {}
tick_windows = {}

ready = threading.Event()
_threads_started = False
_threads_lock = threading.Lock()

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


def init_dispatch_ledger():
    conn = get_db_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dispatch_ledger_v2 (
                target_epoch INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                setup_id TEXT NOT NULL,
                status TEXT NOT NULL,
                UNIQUE(symbol, setup_id)
            )
        """)
        conn.commit()
    finally:
        conn.close()


init_dispatch_ledger()


def make_tick_handler(symbol, manager):
    def handle(epoch, price, receipt_time):
        try:
            epoch = int(epoch)
            price = float(price)
            if not math.isfinite(price) or price <= 0:
                return []

            now = time.monotonic()
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
                        "last_receipt": now,
                        "complete": epoch - boundary <= 3,
                    }
                    windows[boundary] = window
                elif (
                    epoch - window["last"] > cfg.STALE_TICK_THRESHOLD_SEC
                    or now - window["last_receipt"]
                    > cfg.STALE_TICK_THRESHOLD_SEC
                ):
                    window["complete"] = False

                window["count"] += 1
                window["last"] = epoch
                window["last_receipt"] = now

                live_state[symbol] = {
                    "epoch": epoch,
                    "price": price,
                    "receipt": now,
                }

                for old in list(windows):
                    if old < boundary - cfg.CANDLE_HISTORY_LIMIT * 60:
                        del windows[old]

                # Same lock is used for historical seeding.
                return manager.process_tick(epoch, price, receipt_time)

        except Exception:
            logger.exception("Tick processing failed for %s", symbol)
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
        symbol, make_tick_handler(symbol, manager)
    )


def live_quote(symbol):
    with state_lock:
        quote = live_state.get(symbol)
        if not quote:
            return None
        quote = dict(quote)

    age = deriv_client.get_server_time() - quote["epoch"]
    if age < -cfg.SERVER_SYNC_TOLERANCE_SEC:
        return None
    if age > cfg.STALE_TICK_THRESHOLD_SEC:
        return None
    if time.monotonic() - quote["receipt"] > cfg.STALE_TICK_THRESHOLD_SEC:
        return None
    if not deriv_client.is_connected:
        return None
    return quote


def history_snapshot(symbol):
    with state_lock:
        df = candle_managers[symbol].get_closed_history("1M").copy()
        windows = {
            epoch: dict(value)
            for epoch, value in tick_windows[symbol].items()
        }

    if "time" not in df:
        return df

    def verified_count(epoch):
        epoch = int(epoch)
        window = windows.get(epoch)
        if not window or not window["complete"]:
            return float("nan")
        if epoch + 60 - window["last"] > cfg.STALE_TICK_THRESHOLD_SEC:
            return float("nan")
        return float(window["count"])

    df["verified_ticks"] = df["time"].map(verified_count)
    return df


def used_setup(symbol, setup_id):
    conn = get_db_connection()
    try:
        return conn.execute(
            """
            SELECT 1 FROM dispatch_ledger_v2
            WHERE symbol = ? AND setup_id = ?
            """,
            (symbol, setup_id),
        ).fetchone() is not None
    finally:
        conn.close()


def reserve_dispatch(symbol, setup_id, target_epoch):
    conn = get_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO dispatch_ledger_v2
                (target_epoch, symbol, setup_id, status)
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
            "UPDATE dispatch_ledger_v2 SET status = ? WHERE target_epoch = ?",
            (status, target_epoch),
        )
        conn.commit()
    finally:
        conn.close()


def send_telegram_alert(candidate, target_epoch):
    if not cfg.TELEGRAM_ENABLED:
        return "DISABLED"
    if not cfg.TELEGRAM_BOT_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        logger.error(
            "Telegram configuration missing: check token and chat/channel ID."
        )
        return "CONFIG_ERROR"

    tz = pytz.timezone(cfg.TIMEZONE_NAME)
    expiry = datetime.datetime.fromtimestamp(target_epoch + 60, tz)
    details = candidate["details"]

    message = (
        "MARKET ANALYSIS SIGNAL\n\n"
        f"Pair: {candidate['display_name']}\n"
        f"Action: {candidate['direction']}\n"
        f"Setup score: {candidate['score']}/10 (not win probability)\n"
        f"Expiry: {expiry.strftime('%H:%M:%S')} {cfg.TIMEZONE_NAME}\n"
        f"Structure: {details['structure']}\n"
        f"Reason: {details['score_reason']}\n\n"
        "Based on Deriv prices. Broker execution prices may differ."
    )

    url = f"https://api.telegram.org/bot{cfg.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(
            url,
            json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": message},
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

        if response.status_code == 200 and body.get("ok") is True:
            logger.info(
                "Telegram accepted %s %s; message_id=%s",
                candidate["display_name"],
                candidate["direction"],
                body.get("result", {}).get("message_id"),
            )
            return "SENT"

        # Avoid logging a token-containing request URL.
        logger.error(
            "Telegram rejected message: HTTP %s, error_code=%s",
            response.status_code,
            body.get("error_code"),
        )
        return "REJECTED"

    except requests.RequestException as exc:
        # An ambiguous timeout must not produce an automatic duplicate send.
        logger.error(
            "Telegram delivery uncertain: %s", type(exc).__name__
        )
        return "UNKNOWN"


def save_signal(candidate, target_epoch, entry_price):
    tz = pytz.timezone(cfg.TIMEZONE_NAME)
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    local_now = utc_now.astimezone(tz)
    signal_id = f"{candidate['symbol']}_1M_{target_epoch}"

    conn = get_db_connection()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO signal_history (
                signal_id, symbol, display_pair, timeframe, candle_epoch,
                signal_timestamp_utc, signal_timestamp_bdt,
                direction, score, quality, bias_15m,
                entry_reference_price, entry_reference_timestamp,
                result, created_at
            )
            VALUES (?, ?, ?, '1M', ?, ?, ?, ?, ?, ?, 'NOT_USED',
                    ?, ?, 'PENDING', ?)
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
    if not quote:
        return "NO_FRESH_QUOTE"

    drift = abs(quote["price"] - candidate["analysis_close"])
    if drift > candidate["details"]["atr"] * cfg.MAX_ENTRY_DRIFT_ATR:
        return "PRICE_MOVED_FROM_SETUP"

    if not reserve_dispatch(
        candidate["symbol"], candidate["details"]["setup_id"], target_epoch
    ):
        return "DUPLICATE"

    status = send_telegram_alert(candidate, target_epoch)
    set_dispatch_status(target_epoch, status)

    if status != "SENT":
        # Keep the attempt recorded: do not blindly repeat an uncertain send.
        return status

    # This is a hypothetical feed reference, not the user's actual entry.
    reference = live_quote(candidate["symbol"])
    if reference and deriv_client.get_server_time() < target_epoch + 60:
        save_signal(candidate, target_epoch, reference["price"])

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
                    "timeframe": "1M",
                    "target_candle_epoch": target_epoch,
                    "expiry_epoch": target_epoch + 60,
                },
            )
        except Exception:
            logger.error("Pusher notification failed.")

    return status


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
                "delivery": "NOT_SENT",
            }

            try:
                df = prepare_history(history_snapshot(symbol))
                df = df[df["time"] < target_epoch].reset_index(drop=True)

                if (
                    df.empty
                    or int(df.iloc[-1]["time"]) + 60 != target_epoch
                ):
                    report["score_reason"] = "LATEST_CLOSED_CANDLE_MISSING"
                    reports[display] = report
                    continue

                direction, score, quality, details = evaluate_strategy(df)
                report.update({
                    "direction": direction,
                    "score": score,
                    "quality": quality,
                    "details": details.get("pa"),
                    "score_reason": details["score_reason"],
                    "structure": details["structure"],
                    "call_score": details["call_score"],
                    "put_score": details["put_score"],
                    "activity_status": details.get("activity_status"),
                    "relative_activity": details.get("relative_activity"),
                    "analysis_candle_epoch": int(df.iloc[-1]["time"]),
                })

                if direction in ("CALL", "PUT"):
                    if used_setup(symbol, details["setup_id"]):
                        report["delivery"] = "SETUP_ALREADY_ATTEMPTED"
                    elif not live_quote(symbol):
                        report["delivery"] = "LIVE_FEED_STALE"
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
                logger.warning("Evaluation failed for %s: %s", display, exc)

            reports[display] = report

        # Compare strength across every eligible pair.
        # Equal strength uses symbol for deterministic ordering.
        candidates.sort(
            key=lambda c: (
                -c["score"],
                -c["details"]["rank_strength"],
                c["symbol"],
            )
        )

        logger.info(
            "Scan: %s pairs, %s candidates, signals_enabled=%s",
            len(reports), len(candidates), cfg.SIGNALS_ENABLED,
        )

        if candidates and cfg.SIGNALS_ENABLED:
            for candidate in candidates:
                status = dispatch_best_signal(candidate, target_epoch)
                reports[candidate["display_name"]]["delivery"] = status

                # Only pre-send drift/staleness can fall through to next pair.
                if status not in ("NO_FRESH_QUOTE", "PRICE_MOVED_FROM_SETUP"):
                    break
        elif candidates:
            reports[candidates[0]["display_name"]]["delivery"] = "DEMO_REVIEW_ONLY"

        with state_lock:
            latest_evaluations.clear()
            latest_evaluations.update(reports)

    finally:
        evaluation_lock.release()


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
        histories = deriv_client.fetch_historical_candles_batch_sync(jobs)
        server_epoch = int(deriv_client.fetch_server_epoch_sync())

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
                # Do not overwrite newer live candles with an older response.
                latest = manager.get_latest_closed_candle("1M")
                if latest and int(latest.epoch) > int(closed[-1]["epoch"]):
                    continue

                # No forming historical candle is passed to the old manager.
                manager.seed_historical_candles("1M", closed, server_epoch)

            updated += 1

        logger.info("M1 history refreshed for %s pairs.", updated)

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

            # Resync away from the entry scan.
            if 15 <= now % 60 < 25 and minute != last_minute:
                last_minute = minute
                resync_historical_candles()

            time.sleep(0.5)
        except Exception:
            logger.exception("History worker failed.")
            time.sleep(1)


def run_scan_worker():
    ready.wait()
    last_minute = None

    while True:
        try:
            now = deriv_client.get_server_time()
            minute = int(now // 60) * 60
            second = now - minute

            if (
                cfg.SCAN_DELAY_SECONDS <= second
                <= cfg.MAX_ENTRY_DELAY_SECONDS
                and minute != last_minute
            ):
                last_minute = minute
                evaluate_and_dispatch_all(minute)

            time.sleep(0.2)
        except Exception:
            logger.exception("Scan worker failed.")
            time.sleep(1)


def run_engine():
    try:
        resync_historical_candles()
        deriv_client.start()
        ready.set()
    except Exception:
        logger.exception("Data engine failed to start.")


def run_outcome_worker():
    while True:
        time.sleep(10)
        conn = None

        try:
            conn = get_db_connection()
            rows = conn.execute(
                """
                SELECT signal_id, symbol, candle_epoch,
                       direction, entry_reference_price
                FROM signal_history
                WHERE result = 'PENDING'
                """
            ).fetchall()

            now = deriv_client.get_server_time()

            for signal_id, symbol, target, direction, entry in rows:
                if (
                    not entry
                    or symbol not in candle_managers
                    or now < int(target) + 60
                ):
                    continue

                df = history_snapshot(symbol)
                if df.empty or "time" not in df:
                    continue

                # Use the EXACT entry-minute candle; never a later candle.
                match = df[df["time"] == int(target)]
                if match.empty:
                    # Keep unresolved when exact evidence is unavailable.
                    continue

                exit_price = float(match.iloc[-1]["close"])
                difference = exit_price - float(entry)
                if direction == "PUT":
                    difference = -difference
                elif direction != "CALL":
                    continue

                outcome = (
                    "WIN" if difference > 0
                    else "LOSS" if difference < 0
                    else "TIE"
                )

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

        for name, function in (
            ("DerivEngine", run_engine),
            ("ScanWorker", run_scan_worker),
            ("HistoryWorker", run_history_worker),
            ("OutcomeWorker", run_outcome_worker),
        ):
            threading.Thread(
                target=function, daemon=True, name=name
            ).start()


@app.before_request
def before_request_func():
    start_background_threads_once()


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/health")
def health():
    fresh = sum(live_quote(symbol) is not None for symbol in cfg.FOREX_PAIRS)
    return jsonify({
        "status": "healthy" if fresh == len(cfg.FOREX_PAIRS) else "degraded",
        "deriv_connected": bool(deriv_client.is_connected),
        "fresh_pairs": fresh,
        "total_pairs": len(cfg.FOREX_PAIRS),
        "signals_enabled": cfg.SIGNALS_ENABLED,
    })


@app.route("/api/dashboard")
def api_dashboard():
    return jsonify({
        "status": "online" if ready.is_set() else "starting",
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
                   direction, score, quality, bias_15m,
                   entry_reference_price, exit_reference_price,
                   result, candle_epoch
            FROM signal_history
        """
        if active_only:
            query += " WHERE result = 'PENDING'"
        query += " ORDER BY candle_epoch DESC LIMIT 50"
        rows = conn.execute(query).fetchall()
    finally:
        conn.close()

    return [{
        "signal_id": r[0],
        "pair": r[1],
        "timeframe": r[2],
        "timestamp": r[3],
        "direction": r[4],
        "score": r[5],
        "quality": r[6],
        "bias": r[7],
        "price": r[8],
        "entry_price": r[8],
        "exit_price": r[9],
        "result": r[10],
        "target_candle_epoch": r[11],
        "performance_basis": "hypothetical Deriv prices",
    } for r in rows]


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
