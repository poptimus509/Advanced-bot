import datetime
import logging
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
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("QuotexSignalBoard")

# ============================================================
# FLASK & DATABASE
# ============================================================

app = Flask(__name__)
init_db()

# ============================================================
# GLOBAL STATE & LOCKS
# ============================================================

latest_evaluations = {}
sent_telegram_cache = set()
processed_closed_candles = set()
target_signal_cache = set()

# প্রতি টার্গেট মিনিটে সর্বোচ্চ একটি সিগন্যাল অনুমোদন করার জন্য গ্লোবাল সেট
processed_target_minutes = set()

evaluation_lock = threading.Lock()
cache_lock = threading.Lock()

# ============================================================
# PUSHER
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
        logger.error(f"Pusher init failed: {e}")

# ============================================================
# EVENT / DATA ENGINE
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
# DATABASE: SAVE SIGNAL
# ============================================================

def save_signal_to_db(
    signal_id, symbol, display_name, timeframe, epoch,
    direction, score, quality, bias, entry_price
):
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
        logger.error(f"DB save error: {e}")
    finally:
        conn.close()

# ============================================================
# TELEGRAM
# ============================================================

def send_telegram_alert(
    pair, direction, score, quality, bias,
    details_str, timeframe="1M", target_epoch=None
):
    if not TELEGRAM_ENABLED or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram alerts are disabled or credentials missing.")
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

        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info(f"SUCCESS: Telegram alert sent for {pair} -> {direction} (Score: {score}/10)")
            return True

        logger.error(f"Failed to send Telegram alert: Status {response.status_code}, Response: {response.text}")
        return False
    except Exception as e:
        logger.error(f"Telegram dispatch error exception: {e}")
        return False

# ============================================================
# PUSHER
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
        logger.error(f"Pusher trigger error: {e}")

# ============================================================
# PROCESS VALID SIGNAL (WITH RATE LIMITER)
# ============================================================

def process_signal_if_strong(
    symbol, display_name, timeframe, direction,
    score, quality, details, entry_price, analysis_epoch
):
    if direction not in ("CALL", "PUT"):
        return

    target_epoch = int(analysis_epoch) + 60

    # 1. প্রতি ১ মিনিটে যাতে একাধিক সিগন্যাল স্প্যাম না হয় তার গ্লোবাল লক
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
        # একই মিনিটে অন্য কোনো পেয়ারের সিগন্যাল ব্লক করতে রিজার্ভ করা হলো
        processed_target_minutes.add(target_epoch)

    signal_id = f"{symbol}_{timeframe}_{target_epoch}"
    bias = details.get("bias", "NEUTRAL")
    pa = details.get("pa", "")

    save_signal_to_db(
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

    telegram_sent = send_telegram_alert(
        pair=display_name,
        direction=direction,
        score=score,
        quality=quality,
        bias=bias,
        details_str=pa,
        timeframe=timeframe,
        target_epoch=target_epoch
    )

    send_pusher_signal(
        display_name=display_name,
        direction=direction,
        score=score,
        quality=quality,
        bias=bias,
        timeframe=timeframe,
        target_epoch=target_epoch
    )

    logger.info(
        f"NEXT-CANDLE SIGNAL: {display_name} | {direction} | Score={score}/10 | Target={target_epoch} | Telegram={telegram_sent}"
    )

# ============================================================
# STRATEGY EVALUATION ONLY
# ============================================================

def evaluate_pair(symbol, display_name):
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

        analysis_epoch = int(latest_closed.epoch)
        entry_price = float(latest_closed.close)

        return {
            "direction": direction,
            "score": int(score),
            "quality": quality,
            "details": details,
            "analysis_epoch": analysis_epoch,
            "target_epoch": analysis_epoch + 60,
            "entry_price": entry_price
        }

    except Exception as e:
        logger.error(f"Strategy evaluation error for {display_name}: {e}")
        return None

# ============================================================
# EVALUATE ALL PAIRS
# ============================================================

def evaluate_and_dispatch_all(send_signal=True, target_symbol=None, trigger_epoch=None):
    with evaluation_lock:
        tz = pytz.timezone(TIMEZONE_NAME)
        now_str = datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

        pairs_to_scan = (
            {target_symbol: FOREX_PAIRS[target_symbol]}
            if target_symbol and target_symbol in FOREX_PAIRS
            else FOREX_PAIRS
        )

        for sym, disp in pairs_to_scan.items():
            result = evaluate_pair(sym, disp)
            if not result:
                continue

            direction = result["direction"]
            score = result["score"]
            quality = result["quality"]
            details = result["details"]
            analysis_epoch = result["analysis_epoch"]
            target_epoch = result["target_epoch"]
            entry_price = result["entry_price"]

            latest_evaluations[disp] = {
                "timestamp": now_str,
                "direction": direction,
                "score": score,
                "quality": quality,
                "bias": details.get("bias", "NEUTRAL"),
                "details": details.get("pa", ""),
                "analysis_candle_epoch": analysis_epoch,
                "target_candle_epoch": target_epoch
            }

            if not send_signal:
                continue

            if trigger_epoch is not None and analysis_epoch != int(trigger_epoch):
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
                analysis_epoch=analysis_epoch
            )

# ============================================================
# 1M CANDLE CLOSED EVENT
# ============================================================

def on_candle_closed(event: CandleClosedEvent):
    if getattr(event, "timeframe", "") != "1M":
        return

    try:
        event_epoch = getattr(event, "epoch", None)
        event_symbol = getattr(event, "symbol", getattr(event, "pair", None))

        if event_epoch is not None and event_symbol is not None:
            event_key = f"{event_symbol}_{int(event_epoch)}"

            with cache_lock:
                if event_key in processed_closed_candles:
                    return
                processed_closed_candles.add(event_key)

        evaluate_and_dispatch_all(
            send_signal=True,
            target_symbol=event_symbol,
            trigger_epoch=event_epoch
        )

    except Exception as e:
        logger.error(f"on_candle_closed error: {e}")

event_dispatcher.subscribe(on_candle_closed)

# ============================================================
# FAST MINUTE CHECKER (RECOVERY MODE)
# ============================================================

def run_fast_minute_checker():
    logger.info("Fast minute checker started (recovery mode).")
    last_minute = None

    while True:
        try:
            current_epoch = int(time.time() // 60) * 60
            current_minute = current_epoch // 60

            if last_minute is None:
                last_minute = current_minute
            elif current_minute != last_minute:
                last_minute = current_minute
                time.sleep(2.0)

                for sym, disp in FOREX_PAIRS.items():
                    cm = candle_managers.get(sym)
                    if not cm:
                        continue

                    latest_closed = cm.get_latest_closed_candle("1M")
                    if not latest_closed:
                        continue

                    closed_epoch = int(latest_closed.epoch)
                    recovery_key = f"{sym}_{closed_epoch}"

                    with cache_lock:
                        already_processed = recovery_key in processed_closed_candles

                    if already_processed:
                        continue

                    with cache_lock:
                        processed_closed_candles.add(recovery_key)

                    evaluate_and_dispatch_all(
                        send_signal=True,
                        target_symbol=sym,
                        trigger_epoch=closed_epoch
                    )
                    # প্রতি মিনিটে রিকভারি থেকে একটি পেয়ারের বেশি সিগন্যাল ট্রিগার হতে দেওয়া হবে না
                    break

            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Fast minute checker error: {e}")
            time.sleep(5)

# ============================================================
# ENGINE START
# ============================================================

def run_engine():
    logger.info("=== Background engine thread starting ===")
    try:
        server_epoch = deriv_client.get_server_epoch()

        for deriv_symbol in FOREX_PAIRS.keys():
            try:
                logger.info(f"Seeding history for {deriv_symbol}")
                candles_1m = deriv_client.fetch_historical_candles_sync(
                    deriv_symbol, count=120, granularity=60
                )
                candles_5m = deriv_client.fetch_historical_candles_sync(
                    deriv_symbol, count=60, granularity=300
                )
                candles_15m = deriv_client.fetch_historical_candles_sync(
                    deriv_symbol, count=40, granularity=900
                )

                if deriv_symbol not in candle_managers:
                    continue

                cm = candle_managers[deriv_symbol]

                if candles_1m:
                    cm.seed_historical_candles("1M", candles_1m, server_epoch)
                if candles_5m:
                    cm.seed_historical_candles("5M", candles_5m, server_epoch)
                if candles_15m:
                    cm.seed_historical_candles("15M", candles_15m, server_epoch)

                logger.info(f"History seeded successfully for {deriv_symbol}")
            except Exception as e:
                logger.error(f"Failed to seed history for {deriv_symbol}: {e}")

        deriv_client.start()
        logger.info("Deriv live engine started.")
    except Exception as e:
        logger.error(f"Engine startup error: {e}")

# ============================================================
# OUTCOME WORKER
# ============================================================

def run_outcome_worker():
    while True:
        try:
            time.sleep(10)
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT signal_id, symbol, timeframe, candle_epoch, direction, entry_reference_price
                FROM signal_history
                WHERE result = 'PENDING'
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
                if not latest_closed:
                    continue

                latest_epoch = int(latest_closed.epoch)
                target_epoch = int(target_epoch)

                if latest_epoch < target_epoch:
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
                    """
                    UPDATE signal_history
                    SET result = ?, exit_reference_price = ?
                    WHERE signal_id = ?
                    """,
                    (outcome, exit_price, sig_id)
                )
                conn.commit()
                logger.info(f"OUTCOME: {sym} | {direction} | {outcome} | Entry={entry_price} | Exit={exit_price}")

            conn.close()
        except Exception as e:
            logger.error(f"Outcome worker error: {e}")

# ============================================================
# BACKGROUND THREAD CONTROL
# ============================================================

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
            threading.Thread(target=run_fast_minute_checker, daemon=True, name="MinuteRecoveryChecker").start()

            _threads_started = True
            logger.info("Background engine & workers successfully initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize background threads: {e}")

@app.before_request
def before_request_func():
    start_background_threads_once()

# ============================================================
# FLASK ROUTES
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

    signals = [
        {
            "signal_id": r[0], "pair": r[1], "timeframe": r[2], "timestamp": r[3],
            "direction": r[4], "score": r[5], "quality": r[6], "bias": r[7],
            "price": r[8], "target_candle_epoch": r[9]
        }
        for r in rows
    ]
    return jsonify(signals)

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

    history = [
        {
            "signal_id": r[0], "pair": r[1], "timeframe": r[2], "timestamp": r[3],
            "direction": r[4], "score": r[5], "quality": r[6], "bias": r[7],
            "entry_price": r[8], "exit_price": r[9], "result": r[10], "target_candle_epoch": r[11]
        }
        for r in rows
    ]
    return jsonify(history)

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
