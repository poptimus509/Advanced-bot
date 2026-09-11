import logging
import os
import threading
import time
from flask import Flask, jsonify, render_template
import pusher
import requests
import pytz
import datetime
from config import (
    FOREX_PAIRS,
    PUSHER_APP_ID,
    PUSHER_CLUSTER,
    PUSHER_ENABLED,
    PUSHER_KEY,
    PUSHER_SECRET,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_ENABLED,
    TIMEZONE_NAME,
    SIGNAL_THRESHOLD_CALL_PUT
)
from core.events import EventDispatcher, CandleClosedEvent, Candle
from core.signal_lock import SignalLockManager
from data.candle_manager import CandleManager
from data.deriv_client import DerivClient
from database import init_db, get_db_connection
from monitor import get_today_performance
from strategy import evaluate_strategy

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("QuotexSignalBoard")

app = Flask(__name__)

init_db()

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
        logger.error(f"Pusher init failed: {e}")

event_dispatcher = EventDispatcher()
signal_lock_manager = SignalLockManager()
candle_managers = {}
deriv_client = DerivClient()

for deriv_symbol, display_name in FOREX_PAIRS.items():
    cm = CandleManager(symbol=deriv_symbol, event_dispatcher=event_dispatcher)
    candle_managers[deriv_symbol] = cm
    deriv_client.register_tick_handler(deriv_symbol, cm.process_tick)

def save_signal_to_db(signal_id, symbol, display_name, timeframe, epoch, direction, score, quality, bias, entry_price):
    tz = pytz.timezone(TIMEZONE_NAME)
    now = datetime.datetime.now(tz)
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
        INSERT OR IGNORE INTO signal_history (
            signal_id, symbol, display_pair, timeframe, candle_epoch,
            signal_timestamp_utc, signal_timestamp_bdt, direction, score,
            quality, bias_15m, entry_reference_price, entry_reference_timestamp,
            result, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)
        """, (
            signal_id, symbol, display_name, timeframe, epoch,
            utc_now.isoformat(), now.isoformat(), direction, score,
            quality, bias, entry_price, utc_now.isoformat(), now.isoformat()
        ))
        conn.commit()
    except Exception as e:
        logger.error(f"DB save error: {e}")
    finally:
        conn.close()

def send_telegram_alert(pair, direction, score, quality, bias, details_str, timeframe="1M"):
    if not TELEGRAM_ENABLED or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram alerts are disabled or credentials missing.")
        return
    try:
        emoji = "🟢" if direction == "CALL" else "🔴"
        message = (
            f"{emoji} *QUOTEX MARKET SIGNAL* {emoji}\n\n"
            f"💱 Pair: *{pair}* ({timeframe})\n"
            f"🔹 Action: *{direction}*\n"
            f"🎯 Score: *{score}/11* (Quality: {quality})\n"
            f"📊 15M Bias: *{bias}*\n"
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
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info(f"Telegram alert successfully sent for {pair} -> {direction}")
        else:
            logger.error(f"Failed to send Telegram alert: Status {response.status_code}, Response: {response.text}")
    except Exception as e:
        logger.error(f"Telegram dispatch error exception: {e}")

def on_candle_closed(event: CandleClosedEvent):
    if event.timeframe != "1M":
        return
    
    display_name = FOREX_PAIRS.get(event.symbol, event.symbol)

    if not signal_lock_manager.acquire_lock(event.symbol, event.timeframe, event.candle_epoch):
        return

    cm = candle_managers.get(event.symbol)
    if not cm:
        return

    df_5m = cm.get_closed_history("5M")
    df_15m = cm.get_closed_history("15M")
    df_1m = cm.get_closed_history("1M")

    direction, score, quality, details = evaluate_strategy(df_1m, df_5m, df_15m)
    
    logger.info(f"[{display_name}] 1M Strategy Evaluated -> Direction: {direction} | Score: {score}/11 | Quality: {quality}")

    signal_id = f"{event.symbol}_{event.timeframe}_{event.candle_epoch}"
    entry_price = event.candle.close

    STRONG_SIGNAL_THRESHOLD = SIGNAL_THRESHOLD_CALL_PUT

    if direction in ["CALL", "PUT"] and score >= STRONG_SIGNAL_THRESHOLD:
        save_signal_to_db(signal_id, event.symbol, display_name, "1M", event.candle_epoch, direction, score, quality, details["bias"], entry_price)
        signal_lock_manager.commit_result(event.symbol, event.timeframe, event.candle_epoch, direction, {"score": score})
        send_telegram_alert(display_name, direction, score, quality, details["bias"], details["pa"], timeframe="1M")
        
        if pusher_client:
            try:
                pusher_client.trigger("trading-signals", "new-signal", {
                    "pair": display_name,
                    "direction": direction,
                    "score": score,
                    "quality": quality,
                    "bias": details["bias"],
                    "timeframe": "1M"
                })
            except Exception as e:
                logger.error(f"Pusher trigger error: {e}")

event_dispatcher.subscribe(on_candle_closed)

def run_engine():
    logger.info("=== Background engine thread starting (Optimized Multi-Timeframe Strategy) ===")
    
    server_epoch = deriv_client.get_server_epoch()
    for deriv_symbol in FOREX_PAIRS.keys():
        try:
            candles_1m = deriv_client.fetch_historical_candles_sync(deriv_symbol, count=120, granularity=60)
            candles_5m = deriv_client.fetch_historical_candles_sync(deriv_symbol, count=100, granularity=300)
            candles_15m = deriv_client.fetch_historical_candles_sync(deriv_symbol, count=100, granularity=900)
            
            if deriv_symbol in candle_managers:
                cm = candle_managers[deriv_symbol]
                if candles_1m:
                    cm.seed_historical_candles("1M", candles_1m, server_epoch)
                if candles_5m:
                    cm.seed_historical_candles("5M", candles_5m, server_epoch)
                if candles_15m:
                    cm.seed_historical_candles("15M", candles_15m, server_epoch)
        except Exception as e:
            logger.error(f"Failed to seed history for {deriv_symbol}: {e}")

    deriv_client.start()

def run_outcome_worker():
    while True:
        try:
            time.sleep(30)
            tz = pytz.timezone(TIMEZONE_NAME)
            now = datetime.datetime.now(tz)
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT signal_id, symbol, timeframe, candle_epoch, direction, entry_reference_price FROM signal_history WHERE result = 'PENDING'")
            rows = cursor.fetchall()
            for row in rows:
                sig_id, sym, tf, epoch, direction, entry_price = row
                if not entry_price:
                    continue
                cm = candle_managers.get(sym)
                if not cm:
                    continue
                latest_closed = cm.get_latest_closed_candle(tf)
                if latest_closed and latest_closed.epoch > epoch:
                    exit_price = latest_closed.close
                    outcome = "WIN"
                    if direction == "CALL":
                        outcome = "WIN" if exit_price > entry_price else ("LOSS" if exit_price < entry_price else "TIE")
                    elif direction == "PUT":
                        outcome = "WIN" if exit_price < entry_price else ("LOSS" if exit_price > entry_price else "TIE")
                    
                    cursor.execute("UPDATE signal_history SET result = ?, exit_reference_price = ? WHERE signal_id = ?", (outcome, exit_price, sig_id))
                    conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Outcome worker error: {e}")

@app.route("/")
def dashboard():
    return render_template("dashboard.html")

@app.route("/health")
def health():
    return jsonify({"status": "healthy", "time": datetime.datetime.now().isoformat()}), 200

@app.route("/api/dashboard")
def api_dashboard():
    perf = get_today_performance()
    return jsonify({
        "status": "online",
        "performance": perf,
        "active_pairs": list(FOREX_PAIRS.values())
    })

@app.route("/api/active-signals")
@app.route("/api/signals/active")
def api_active_signals():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT signal_id, display_pair, timeframe, signal_timestamp_bdt, direction, score, quality, bias_15m, entry_reference_price FROM signal_history WHERE result = 'PENDING' ORDER BY candle_epoch DESC LIMIT 10")
    rows = cursor.fetchall()
    conn.close()
    signals = [{
        "signal_id": r[0], "pair": r[1], "timeframe": r[2], "timestamp": r[3],
        "direction": r[4], "score": r[5], "quality": r[6], "bias": r[7], "price": r[8]
    } for r in rows]
    return jsonify(signals)

@app.route("/api/history")
def api_history():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT signal_id, display_pair, timeframe, signal_timestamp_bdt, direction, score, quality, bias_15m, entry_reference_price, exit_reference_price, result FROM signal_history ORDER BY candle_epoch DESC LIMIT 50")
    rows = cursor.fetchall()
    conn.close()
    history = [{
        "signal_id": r[0], "pair": r[1], "timeframe": r[2], "timestamp": r[3],
        "direction": r[4], "score": r[5], "quality": r[6], "bias": r[7],
        "entry_price": r[8], "exit_price": r[9], "result": r[10]
    } for r in rows]
    return jsonify(history)

@app.route("/api/performance")
def api_performance():
    return jsonify(get_today_performance())

@app.route("/api/pairs")
def api_pairs():
    return jsonify(FOREX_PAIRS)

# Lazy initialization for Gunicorn/Render worker process compatibility
_threads_started = False
_threads_lock = threading.Lock()

def start_background_threads_once():
    global _threads_started
    with _threads_lock:
        if not _threads_started:
            try:
                engine_thread = threading.Thread(target=run_engine, daemon=True)
                engine_thread.start()
                
                outcome_thread = threading.Thread(target=run_outcome_worker, daemon=True)
                outcome_thread.start()
                logger.info("Background engine & outcome worker threads successfully initialized in worker process.")
                _threads_started = True
            except Exception as e:
                logger.error(f"Failed to initialize background threads: {e}")

@app.before_request
def before_request_func():
    start_background_threads_once()

if __name__ == "__main__":
    start_background_threads_once()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
