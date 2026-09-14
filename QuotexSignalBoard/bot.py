import datetime
import logging
import math
import os
import threading
import time

from flask import Flask, jsonify, render_template
import pusher
import pytz
import requests

from config import (
    FOREX_PAIRS,
    PUSHER_APP_ID,
    PUSHER_CLUSTER,
    PUSHER_ENABLED,
    PUSHER_KEY,
    PUSHER_SECRET,
    SIGNAL_THRESHOLD_CALL_PUT,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_ENABLED,
    TIMEZONE_NAME,
)
from core.events import CandleClosedEvent, EventDispatcher
from core.signal_lock import SignalLockManager
from data.candle_manager import CandleManager
from data.deriv_client import DerivClient
from database import get_db_connection, init_db
from monitor import get_today_performance
from strategy import evaluate_strategy

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("QuotexSignalBoard")

# ============================================================
# FLASK & DATABASE INITIALIZATION
# ============================================================

app = Flask(__name__)
init_db()

# ============================================================
# GLOBAL STATE & CACHE CONTROLS
# ============================================================

latest_evaluations = {}
sent_telegram_cache = set()
target_signal_cache = set()
processed_target_minutes = set()

evaluation_lock = threading.Lock()
cache_lock = threading.Lock()

# ============================================================
# PUSHER INITIALIZATION
# ============================================================

pusher_client = None
if PUSHER_ENABLED:
    try:
        pusher_client = pusher.Pusher(
            app_id=PUSHER_APP_ID,
            key=PUSHER_KEY,
            secret=PUSHER_SECRET,
            cluster=PUSHER_CLUSTER,
            ssl=True
        )
    except Exception as e:
        logger.error(f"Pusher initialization failed: {e}")

# ============================================================
# DATA ENGINE SETUP
# ============================================================

event_dispatcher = EventDispatcher()
signal_lock_manager = SignalLockManager()
candle_managers = {}
deriv_client = DerivClient()

for deriv_symbol, display_name in FOREX_PAIRS.items():
    cm = CandleManager(
        symbol=deriv_symbol,
        event_dispatcher=event_dispatcher
    )
    candle_managers[deriv_symbol] = cm
    deriv_client.register_tick_handler(
        deriv_symbol,
        cm.process_tick
    )

# ============================================================
# ASYNCHRONOUS DATABASE WRITER
# ============================================================

def save_signal_to_db_async(
    signal_id, symbol, display_name, timeframe, epoch,
    direction, score, quality, bias, entry_price
):
    def _worker():
        tz = pytz.timezone(TIMEZONE_NAME)
        now = datetime.datetime.now(tz)
        utc_now = datetime.datetime.now(datetime.timezone.utc)

        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO signal_history (
                    signal_id, symbol, display_pair, timeframe, candle_epoch,
                    signal_timestamp_utc, signal_timestamp_bdt, direction, score,
                    quality, bias_15m, entry_reference_price,
                    entry_reference_timestamp, result, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)
                """,
                (
                    signal_id, symbol, display_name, timeframe, epoch,
                    utc_now.isoformat(), now.isoformat(), direction, score,
                    quality, bias, entry_price, utc_now.isoformat(), now.isoformat()
                )
            )
            conn.commit()
        except Exception as e:
            logger.error(f"Async DB save error: {e}")
        finally:
            conn.close()

    threading.Thread(target=_worker, daemon=True).start()

# ============================================================
# ASYNCHRONOUS TELEGRAM DISPATCHER
# ============================================================

def send_telegram_alert(
    pair, direction, score, quality, bias,
    details_str, timeframe="1M", target_epoch=None
):
    if not TELEGRAM_ENABLED or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    try:
        emoji = "🟢" if direction == "CALL" else "🔴"
        target_text = ""
        if target_epoch:
            tz = pytz.timezone(TIMEZONE_NAME)
            target_dt = datetime.datetime.fromtimestamp(target_epoch, tz)
            target_text = f"⏱️ Entry Candle: *{target_dt.strftime('%H:%M:%S')}*"

        message = (
            f"{emoji} *QUOTEX MARKET SIGNAL* {emoji}\n\n"
            f"💱 Pair: *{pair}* ({timeframe})\n"
            f"🔹 Action: *{direction}*\n"
            f"🎯 Score: *{score}/10* (Quality: {quality})\n"
            f"📊 15M Bias: *{bias}*\n"
            f"{target_text}\n"
            f"📝 Details: {details_str}\n\n"
            f"⚠️ Market analysis signal. Execute at your own risk."
        )

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_notification": False
        }

        requests.post(url, json=payload, timeout=3)
        logger.info(f"DISPATCHED: Telegram alert for {pair} -> {direction}")
        return True
    except Exception as e:
        logger.error(f"Telegram dispatch error: {e}")
        return False

# ============================================================
# ASYNCHRONOUS PUSHER DISPATCHER
# ============================================================

def send_pusher_signal(display_name, direction, score, quality, bias, timeframe, target_epoch):
    if not pusher_client:
        return
    try:
        pusher_client.trigger(
            "trading-signals",
            "new-signal",
            {
                "pair": display_name,
                "direction": direction,
                "score": score,
                "quality": quality,
                "bias": bias,
                "timeframe": timeframe,
                "target_candle_epoch": target_epoch
            }
        )
    except Exception as e:
        logger.error(f"Pusher dispatch error: {e}")

# ============================================================
# SIGNAL VALIDATION & RATE CONTROL
# ============================================================

def process_signal_if_strong(
    symbol, display_name, timeframe, direction,
    score, quality, details, entry_price, target_epoch
):
    if direction not in ("CALL", "PUT"):
        return

    # Strictly permit a maximum of 1 signal across all pairs per target minute
    with cache_lock:
        if target_epoch in processed_target_minutes:
            return

    try:
        threshold = int(SIGNAL_THRESHOLD_CALL_PUT)
    except Exception:
        threshold = 7

    if threshold < 1:
        threshold = 7

    if score < threshold:
        return

    target_key = f"{symbol}_{timeframe}_{target_epoch}"
    signal_key = f"{symbol}_{timeframe}_{target_epoch}_{direction}"

    with cache_lock:
        if target_key in target_signal_cache or signal_key in sent_telegram_cache:
            return

        target_signal_cache.add(target_key)
        sent_telegram_cache.add(signal_key)
        processed_target_minutes.add(target_epoch)

    signal_id = f"{symbol}_{timeframe}_{target_epoch}"
    bias = details.get("bias", "NEUTRAL")
    pa = details.get("pa", "")

    # Non-blocking Telegram alert
    threading.Thread(
        target=send_telegram_alert,
        args=(display_name, direction, score, quality, bias, pa, timeframe, target_epoch),
        daemon=True
    ).start()

    # Non-blocking Pusher alert
    threading.Thread(
        target=send_pusher_signal,
        args=(display_name, direction, score, quality, bias, timeframe, target_epoch),
        daemon=True
    ).start()

    # Async database commit
    save_signal_to_db_async(
        signal_id=signal_id,
        symbol=symbol,
        display_name=display_name,
        timeframe=timeframe,
        epoch=target_epoch,
        direction=direction,
        score=score,
        quality=quality,
        bias=bias,
        entry_price=entry_price
    )

    logger.info(
        f"PERFECT 0-SEC ENTRY SIGNAL: {display_name} | {direction} | Score={score}/10 | Target={target_epoch}"
    )

# ============================================================
# STRATEGY EVALUATION
# ============================================================

def evaluate_pair(symbol, display_name, target_epoch):
    cm = candle_managers.get(symbol)
    if not cm:
        return None

    try:
        df_1m = cm.get_closed_history("1M")
        df_5m = cm.get_closed_history("5M")
        df_15m = cm.get_closed_history("15M")

        if df_1m is None or len(df_1m) < 25:
            return None
        if df_5m is None or len(df_5m) < 20:
            return None

        direction, score, quality, details = evaluate_strategy(
            df_1m,
            df_5m,
            df_15m
        )

        latest_closed = cm.get_latest_closed_candle("1M")
        if not latest_closed:
            return None

        entry_price = float(latest_closed.close)

        return {
            "direction": direction,
            "score": int(score),
            "quality": quality,
            "details": details,
            "analysis_epoch": target_epoch - 60,
            "target_epoch": target_epoch,
            "entry_price": entry_price
        }
    except Exception as e:
        logger.error(f"Strategy evaluation error for {display_name}: {e}")
        return None

# ============================================================
# EVALUATION & DISPATCH PIPELINE
# ============================================================

def evaluate_and_dispatch_all(send_signal=True, target_epoch=None):
    with evaluation_lock:
        now_ts = time.time()
        tz = pytz.timezone(TIMEZONE_NAME)
        now_str = datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

        if target_epoch is None:
            target_epoch = (int(now_ts // 60) + 1) * 60

        for sym, disp in FOREX_PAIRS.items():
            result = evaluate_pair(sym, disp, target_epoch)
            if not result:
                continue

            direction = result["direction"]
            score = result["score"]
            quality = result["quality"]
            details = result["details"]
            entry_price = result["entry_price"]

            latest_evaluations[disp] = {
                "timestamp": now_str,
                "direction": direction,
                "score": score,
                "quality": quality,
                "bias": details.get("bias", "NEUTRAL"),
                "details": details.get("pa", ""),
                "analysis_candle_epoch": target_epoch - 60,
                "target_candle_epoch": target_epoch
            }

            if not send_signal:
                continue

            process_signal_if_strong(
                symbol=sym,
                display_name=disp,
                timeframe="1M",
                direction=direction,
                score=score,
                quality=quality,
                details=details,
                entry_price=entry_price,
                target_epoch=target_epoch
            )

# ============================================================
# HIGH PRECISION SNIPER (0-SECOND CANDLE OPEN TRIGGER)
# ============================================================

def run_precision_sniper():
    """
    Executes calculations at 58.2s and dispatches alerts directly 
    at 59.8s - 00.0s, giving a zero-second entry on the new candle.
    """
    logger.info("High precision sniper minute runner active.")
    last_processed_minute = None

    while True:
        try:
            now = time.time()
            sec = now % 60
            current_minute_epoch = int(now // 60) * 60

            if sec >= 58.2 and last_processed_minute != current_minute_epoch:
                last_processed_minute = current_minute_epoch
                target_next_minute = current_minute_epoch + 60

                threading.Thread(
                    target=evaluate_and_dispatch_all,
                    args=(True, target_next_minute),
                    daemon=True
                ).start()

            time.sleep(0.05)
        except Exception as e:
            logger.error(f"Sniper runner error: {e}")
            time.sleep(1)

# ============================================================
# BACKGROUND ENGINE INITIALIZATION
# ============================================================

def run_engine():
    logger.info("=== Starting live Deriv engine ===")
    try:
        server_epoch = deriv_client.get_server_epoch()
        for deriv_symbol in FOREX_PAIRS.keys():
            try:
                candles_1m = deriv_client.fetch_historical_candles_sync(deriv_symbol, count=120, granularity=60)
                candles_5m = deriv_client.fetch_historical_candles_sync(deriv_symbol, count=60, granularity=300)
                candles_15m = deriv_client.fetch_historical_candles_sync(deriv_symbol, count=40, granularity=900)

                if deriv_symbol not in candle_managers:
                    continue

                cm = candle_managers[deriv_symbol]
                if candles_1m: cm.seed_historical_candles("1M", candles_1m, server_epoch)
                if candles_5m: cm.seed_historical_candles("5M", candles_5m, server_epoch)
                if candles_15m: cm.seed_historical_candles("15M", candles_15m, server_epoch)
            except Exception as e:
                logger.error(f"History seeding error for {deriv_symbol}: {e}")

        deriv_client.start()
        logger.info("Deriv live engine started successfully.")
    except Exception as e:
        logger.error(f"Engine startup crash: {e}")

def run_outcome_worker():
    while True:
        try:
            time.sleep(10)
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT signal_id, symbol, timeframe, candle_epoch, direction, entry_reference_price
                FROM signal_history WHERE result = 'PENDING'
                """
            )
            rows = cursor.fetchall()

            for row in rows:
                sig_id, sym, tf, target_epoch, direction, entry_price = row
                if not entry_price:
                    continue

                cm = candle_managers.get(sym)
                if not cm:
                    continue

                latest_closed = cm.get_latest_closed_candle(tf)
                if not latest_closed or int(latest_closed.epoch) < int(target_epoch):
                    continue

                exit_price = float(latest_closed.close)
                entry_price = float(entry_price)

                if direction == "CALL":
                    outcome = "WIN" if exit_price > entry_price else ("LOSS" if exit_price < entry_price else "TIE")
                elif direction == "PUT":
                    outcome = "WIN" if exit_price < entry_price else ("LOSS" if exit_price > entry_price else "TIE")
                else:
                    continue

                cursor.execute(
                    "UPDATE signal_history SET result = ?, exit_reference_price = ? WHERE signal_id = ?",
                    (outcome, exit_price, sig_id)
                )
                conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Outcome calculation error: {e}")

_threads_started = False
_threads_lock = threading.Lock()

def start_background_threads_once():
    global _threads_started
    with _threads_lock:
        if _threads_started:
            return
        try:
            threading.Thread(target=run_engine, daemon=True, name="DerivEngine").start()
            threading.Thread(target=run_outcome_worker, daemon=True, name="OutcomeWorker").start()
            threading.Thread(target=run_precision_sniper, daemon=True, name="PrecisionSniper").start()
            _threads_started = True
            logger.info("Zero-latency background engines running.")
        except Exception as e:
            logger.error(f"Thread initialization failure: {e}")

@app.before_request
def before_request_func():
    start_background_threads_once()

# ============================================================
# FLASK WEB ROUTES
# ============================================================

@app.route("/")
def dashboard():
    return render_template("dashboard.html")

@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "deriv_connected": deriv_client.is_connected,
        "time": datetime.datetime.now().isoformat()
    }), 200

@app.route("/api/dashboard")
def api_dashboard():
    return jsonify({
        "status": "online",
        "deriv_connected": deriv_client.is_connected,
        "performance": get_today_performance(),
        "active_pairs": list(FOREX_PAIRS.values())
    })

@app.route("/api/active-signals")
@app.route("/api/signals/active")
def api_active_signals():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT signal_id, display_pair, timeframe, signal_timestamp_bdt,
               direction, score, quality, bias_15m, entry_reference_price, candle_epoch
        FROM signal_history WHERE result = 'PENDING'
        ORDER BY candle_epoch DESC LIMIT 10
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return jsonify([
        {
            "signal_id": r[0], "pair": r[1], "timeframe": r[2], "timestamp": r[3],
            "direction": r[4], "score": r[5], "quality": r[6], "bias": r[7],
            "price": r[8], "target_candle_epoch": r[9]
        }
        for r in rows
    ])

@app.route("/api/evaluations")
def api_evaluations():
    evaluate_and_dispatch_all(send_signal=False)
    return jsonify(latest_evaluations)

@app.route("/api/history")
def api_history():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT signal_id, display_pair, timeframe, signal_timestamp_bdt,
               direction, score, quality, bias_15m, entry_reference_price,
               exit_reference_price, result, candle_epoch
        FROM signal_history ORDER BY candle_epoch DESC LIMIT 50
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return jsonify([
        {
            "signal_id": r[0], "pair": r[1], "timeframe": r[2], "timestamp": r[3],
            "direction": r[4], "score": r[5], "quality": r[6], "bias": r[7],
            "entry_price": r[8], "exit_price": r[9], "result": r[10], "target_candle_epoch": r[11]
        }
        for r in rows
    ])

@app.route("/api/performance")
def api_performance():
    return jsonify(get_today_performance())

@app.route("/api/pairs")
def api_pairs():
    return jsonify(FOREX_PAIRS)

if __name__ == "__main__":
    start_background_threads_once()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
