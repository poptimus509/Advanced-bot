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
    if not TELEGRAM_ENABLED:
        return
    try:
        message = (
            f"🚨 *QUOTEX MARKET SIGNAL* 🚨\n\n"
            f"💱 Pair: *{pair}*\n"
            f"🟢 Action: *{direction}*\n"
            f"⏱ Timeframe: *{timeframe}*\n"
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
    if event.timeframe != "1M":
        return
    
    display_name = FOREX_PAIRS.get(event.symbol, event.symbol)
    if not signal_lock_manager.acquire_lock(event.symbol, event.timeframe, event.candle_epoch):
        return

    cm = candle_managers.get(event.symbol)
    if not cm:
        return

    # Strategy evaluation uses 5M and 15M trend/indicators
    df_5m = cm.get_closed_history("5M")
    df_15m = cm.get_closed_history("15M")

    direction, score, quality, details = evaluate_strategy(df_5m, df_15m)
    
    logger.info(f"[{display_name}] 1M Strategy Evaluated -> Direction: {direction} | Score: {score}/11 | Quality: {quality} | Threshold Required: {SIGNAL_THRESHOLD_CALL_PUT}")

    signal_id = f"{event.symbol}_{event.timeframe}_{event.candle_epoch}"
    entry_price = event.candle.close

    if direction in ["CALL", "PUT"] and score >= SIGNAL_THRESHOLD_CALL_PUT:
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
            except:
                pass
    else:
        signal_lock_manager.commit_result(event.symbol, event.timeframe, event.candle_epoch, "NO_TRADE")

event_dispatcher.subscribe(on_candle_closed)

def run_engine():
    logger.info("=== Background engine thread starting (1M Signal Mode) ===")
    try:
        deriv_client.start()
        time.sleep(3)
        server_epoch = deriv_client.get_server_epoch()
        logger.info(f"Server epoch acquired: {server_epoch}")
        
        for deriv_symbol in FOREX_PAIRS.keys():
            candles_1m = deriv_client.fetch_historical_candles_sync(deriv_symbol, count=120, granularity=60)
            if candles_1m and deriv_symbol in candle_managers:
                candle_managers[deriv_symbol].seed_historical_candles("1M", candles_1m, server_epoch)
                
            candles_5m = deriv_client.fetch_historical_candles_sync(deriv_symbol, count=100, granularity=300)
            if candles_5m and deriv_symbol in candle_managers:
                candle_managers[deriv_symbol].seed_historical_candles("5M", candles_5m, server_epoch)
                
            candles_15m = deriv_client.fetch_historical_candles_sync(deriv_symbol, count=100, granularity=900)
            if candles_15m and deriv_symbol in candle_managers:
                candle_managers[deriv_symbol].seed_historical_candles("15M", candles_15m, server_epoch)
                
            time.sleep(0.2)
            
        logger.info("=== Seeding complete. Polling 1M candles for signals ===")
    except Exception as e:
        logger.error(f"Critical error in run_engine: {e}", exc_info=True)
        
    last_evaluated_epochs = {sym: 0 for sym in FOREX_PAIRS.keys()}

    while True:
        time.sleep(15)  # Check frequently for 1M candle completion
        server_epoch = deriv_client.get_server_epoch()
        
        for sym, disp in FOREX_PAIRS.items():
            try:
                cm = candle_managers.get(sym)
                if not cm:
                    continue
                
                # Fetch fresh 1M, 5M, and 15M data
                candles_1m = deriv_client.fetch_historical_candles_sync(sym, count=120, granularity=60)
                if candles_1m:
                    cm.seed_historical_candles("1M", candles_1m, server_epoch)
                
                candles_5m = deriv_client.fetch_historical_candles_sync(sym, count=100, granularity=300)
                if candles_5m:
                    cm.seed_historical_candles("5M", candles_5m, server_epoch)

                candles_15m = deriv_client.fetch_historical_candles_sync(sym, count=100, granularity=900)
                if candles_15m:
                    cm.seed_historical_candles("15M", candles_15m, server_epoch)

                df_1m = cm.get_closed_history("1M")
                if not df_1m.empty:
                    latest_row = df_1m.iloc[-1]
                    latest_epoch = int(latest_row["Time"])
                    
                    if latest_epoch > last_evaluated_epochs[sym]:
                        last_evaluated_epochs[sym] = latest_epoch
                        candle_obj = Candle(
                            symbol=sym,
                            timeframe="1M",
                            epoch=latest_epoch,
                            open=float(latest_row["Open"]),
                            high=float(latest_row["High"]),
                            low=float(latest_row["Low"]),
                            close=float(latest_row["Close"]),
                            is_closed=True,
                            ticks_count=int(latest_row["TicksCount"]),
                            close_epoch=latest_epoch + 60
                        )
                        event = CandleClosedEvent(
                            symbol=sym,
                            timeframe="1M",
                            candle_epoch=latest_epoch,
                            candle=candle_obj,
                            server_time_at_close=server_epoch,
                            closed_history=df_1m
                        )
                        event_dispatcher.dispatch_candle_closed(event)
                        logger.info(f"Polled & Dispatched 1M closed candle for {disp} at epoch {latest_epoch}")
            except Exception as loop_err:
                logger.error(f"Error in polling loop for {sym}: {loop_err}")

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
        last_c = cm.get_latest_closed_candle("1M") if cm else None
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
