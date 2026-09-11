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
from core.events import EventDispatcher, CandleClosedEvent
from core.signal_lock import SignalLockManager
from data.candle_manager import CandleManager
from data.deriv_client import DerivClient
from database import init_db, get_db_connection
from monitor import get_today_performance
from strategy import evaluate_strategy

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("QuotexSignalBoard")

app = Flask(__name__)

# ডাটাবেস ইনিশিয়ালাইজেশন
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

def send_telegram_alert(pair, direction, score, quality, bias, details_str):
    if not TELEGRAM_ENABLED:
        return
    try:
        message = (
            f"🚨 *QUOTEX MARKET SIGNAL* 🚨\n\n"
            f"💱 Pair: *{pair}*\n"
            f"🟢 Action: *{direction}*\n"
            f"⏱ Timeframe: 5M\n"
            f"🎯 Score: *{score}/11*\n"
            f"⭐ Quality: *{quality}*\n"
            f"📊 15M Bias: *{bias}*\n"
            f"📝 Details: {details_str}\n\n"
            f"⚠️ Market analysis signal only. No auto-execution."
        )
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_notification": False
        }
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Telegram dispatch error: {e}")

def on_candle_closed(event: CandleClosedEvent):
    if event.timeframe != "5M":
        return
    
    display_name = FOREX_PAIRS.get(event.symbol, event.symbol)
    if not signal_lock_manager.acquire_lock(event.symbol, event.timeframe, event.candle_epoch):
        return

    cm = candle_managers.get(event.symbol)
    if not cm:
        return

    df_5m = cm.get_closed_history("5M")
    df_15m = cm.get_closed_history("15M")

    direction, score, quality, details = evaluate_strategy(df_5m, df_15m)
    
    logger.info(f"[{display_name}] Strategy Evaluated -> Direction: {direction} | Score: {score}/11 | Quality: {quality} | Threshold Required: {SIGNAL_THRESHOLD_CALL_PUT}")

    signal_id = f"{event.symbol}_{event.timeframe}_{event.candle_epoch}"
    entry_price = event.candle.close

    if direction in ["CALL", "PUT"] and score >= SIGNAL_THRESHOLD_CALL_PUT:
        save_signal_to_db(signal_id, event.symbol, display_name, "5M", event.candle_epoch, direction, score, quality, details["bias"], entry_price)
        signal_lock_manager.commit_result(event.symbol, event.timeframe, event.candle_epoch, direction, {"score": score})
        send_telegram_alert(display_name, direction, score, quality, details["bias"], details["pa"])
        
        if pusher_client:
            try:
                pusher_client.trigger("trading-signals", "new-signal", {
                    "pair": display_name,
                    "direction": direction,
                    "score": score,
                    "quality": quality,
                    "bias": details["bias"]
                })
            except:
                pass
    else:
        signal_lock_manager.commit_result(event.symbol, event.timeframe, event.candle_epoch, "NO_TRADE")

event_dispatcher.subscribe(on_candle_closed)

def run_engine():
    logger.info("=== Background engine thread starting ===")
    try:
        deriv_client.start()
        logger.info("Deriv client start signal sent. Waiting for server epoch...")
        time.sleep(3)
        server_epoch = deriv_client.get_server_epoch()
        logger.info(f"Server epoch acquired: {server_epoch}")
        
        for deriv_symbol in FOREX_PAIRS.keys():
            logger.info(f"Fetching historical candles for {deriv_symbol}...")
            candles = deriv_client.fetch_historical_candles_sync(deriv_symbol, count=120, granularity=60)
            if candles and deriv_symbol in candle_managers:
                candle_managers[deriv_symbol].seed_historical_candles("1M", candles, server_epoch)
                logger.info(f"Seeded candles for {deriv_symbol}")
            else:
                logger.warning(f"Failed or empty candles for {deriv_symbol}")
            time.sleep(0.2)
            
        logger.info("=== All pairs seeded successfully. Subscribing to live ticks ===")
        
        # ডেরিভ ওয়েবসকেটে লাইভ টিক সাবস্ক্রিপশন রিকোয়েস্ট পাঠানো হচ্ছে
        for deriv_symbol in FOREX_PAIRS.keys():
            try:
                if hasattr(deriv_client, 'subscribe_ticks'):
                    deriv_client.subscribe_ticks(deriv_symbol)
                elif hasattr(deriv_client, 'send'):
                    deriv_client.send({"ticks": deriv_symbol, "subscribe": 1})
                logger.info(f"Subscribed to live ticks for: {deriv_symbol}")
            except Exception as sub_err:
                logger.error(f"Failed to subscribe ticks for {deriv_symbol}: {sub_err}")
            time.sleep(0.2)

        logger.info("=== Entering live monitoring loop ===")
    except Exception as e:
        logger.error(f"Critical error in run_engine: {e}", exc_info=True)
        
    while True:
        time.sleep(15)

worker_thread = threading.Thread(target=run_engine, daemon=True)
worker_thread.start()

def run_outcome_worker():
    while True:
        time.sleep(30)
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT signal_id, symbol, timeframe, candle_epoch, direction, entry_reference_price FROM signal_history WHERE result = 'PENDING'")
            pending_signals = cursor.fetchall()
            
            for sig in pending_signals:
                sym = sig["symbol"]
                epoch = sig["candle_epoch"]
                direction = sig["direction"]
                entry = sig["entry_reference_price"]
                
                cm = candle_managers.get(sym)
                if cm:
                    history_1m = cm.get_closed_history("1M")
                    target_epoch = epoch
                    matching = history_1m[history_1m["Time"] >= target_epoch]
                    if len(matching) >= 2:
                        expiry_row = matching.iloc[1]
                        expiry_price = expiry_row["Close"]
                        
                        if direction == "CALL":
                            res = "WIN" if expiry_price > entry else ("LOSS" if expiry_price < entry else "TIE")
                        else:
                            res = "WIN" if expiry_price < entry else ("LOSS" if expiry_price > entry else "TIE")
                        
                        tz = pytz.timezone(TIMEZONE_NAME)
                        now = datetime.datetime.now(tz)
                        
                        cursor.execute("""
                        UPDATE signal_history 
                        SET result = ?, expiry_price = ?, result_timestamp = ?
                        WHERE signal_id = ?
                        """, (res, expiry_price, now.isoformat(), sig["signal_id"]))
                        conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Outcome worker error: {e}")

outcome_thread = threading.Thread(target=run_outcome_worker, daemon=True)
outcome_thread.start()

@app.route("/")
def dashboard():
    return render_template("dashboard.html")

@app.route("/health")
def health():
    return jsonify({
        "status": "healthy" if deriv_client.is_connected else "degraded",
        "websocket_connected": deriv_client.is_connected,
        "server_epoch": deriv_client.get_server_epoch(),
        "pairs_tracked": len(FOREX_PAIRS)
    })

@app.route("/api/dashboard")
def api_dashboard():
    perf = get_today_performance()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM signal_history ORDER BY candle_epoch DESC LIMIT 10")
    history = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify({
        "performance": perf,
        "history": history,
        "connected": deriv_client.is_connected
    })

@app.route("/api/signals/active")
def api_active_signals():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM signal_history WHERE result = 'PENDING' ORDER BY candle_epoch DESC LIMIT 10")
    active = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(active)

@app.route("/api/history")
def api_history():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM signal_history ORDER BY candle_epoch DESC LIMIT 50")
    history = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(history)

@app.route("/api/performance")
def api_performance():
    perf = get_today_performance()
    return jsonify(perf)

@app.route("/api/pairs")
def api_pairs():
    pairs_data = []
    for sym, disp in FOREX_PAIRS.items():
        cm = candle_managers.get(sym)
        last_c = cm.get_latest_closed_candle("5M") if cm else None
        price = last_c.close if last_c else 0.0
        pairs_data.append({
            "symbol": sym,
            "display": disp,
            "price": price,
            "status": "HEALTHY" if deriv_client.is_connected else "DISCONNECTED"
        })
    return jsonify(pairs_data)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
