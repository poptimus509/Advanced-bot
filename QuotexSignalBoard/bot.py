import logging
import os
import threading
import time
import datetime

from flask import Flask, jsonify, render_template
import pusher
import requests
import pytz

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
    SIGNAL_THRESHOLD_CALL_PUT,
)

from core.events import EventDispatcher, CandleClosedEvent, Candle
from core.signal_lock import SignalLockManager
from data.candle_manager import CandleManager
from data.deriv_client import DerivClient
from database import init_db, get_db_connection
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
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# DATABASE
# ============================================================

init_db()


# ============================================================
# GLOBAL STATE
# ============================================================

latest_evaluations = {}

# Prevent duplicate Telegram/Pusher signals
sent_telegram_cache = set()

# Prevent multiple evaluations for the same closed candle
processed_closed_candles = set()

# Prevent multiple signals for the same target candle
target_signal_cache = set()

# Thread safety
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
# TIME HELPERS
# ============================================================

def get_local_now():
    tz = pytz.timezone(TIMEZONE_NAME)
    return datetime.datetime.now(tz)


def get_utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


def get_current_minute_epoch():
    return int(time.time() // 60) * 60


# ============================================================
# DATABASE: SAVE SIGNAL
# ============================================================

def save_signal_to_db(
    signal_id,
    symbol,
    display_name,
    timeframe,
    epoch,
    direction,
    score,
    quality,
    bias,
    entry_price
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
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?
            )
            """,
            (
                signal_id,
                symbol,
                display_name,
                timeframe,
                epoch,
                utc_now.isoformat(),
                now.isoformat(),
                direction,
                score,
                quality,
                bias,
                entry_price,
                utc_now.isoformat(),
                now.isoformat()
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
    pair,
    direction,
    score,
    quality,
    bias,
    details_str,
    timeframe="1M",
    target_epoch=None
):

    if (
        not TELEGRAM_ENABLED
        or not TELEGRAM_BOT_TOKEN
        or not TELEGRAM_CHAT_ID
    ):
        logger.warning(
            "Telegram alerts are disabled or credentials missing."
        )
        return False

    try:

        emoji = "🟢" if direction == "CALL" else "🔴"

        target_text = ""

        if target_epoch:
            tz = pytz.timezone(TIMEZONE_NAME)

            target_dt = datetime.datetime.fromtimestamp(
                target_epoch,
                tz
            )

            target_text = (
                f"⏱️ Entry Candle: "
                f"*{target_dt.strftime('%H:%M:%S')}*"
            )

        message = (
            f"{emoji} *QUOTEX MARKET SIGNAL* {emoji}\n\n"
            f"💱 Pair: *{pair}* ({timeframe})\n"
            f"🔹 Action: *{direction}*\n"
            f"🎯 Score: *{score}/10* "
            f"(Quality: {quality})\n"
            f"📊 15M Bias: *{bias}*\n"
            f"{target_text}\n"
            f"📝 Details: {details_str}\n\n"
            f"⚠️ Market analysis signal. "
            f"Execute at your own risk."
        )

        url = (
            f"https://api.telegram.org/"
            f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        )

        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_notification": False
        }

        response = requests.post(
            url,
            json=payload,
            timeout=10
        )

        if response.status_code == 200:

            logger.info(
                f"SUCCESS: Telegram alert sent for "
                f"{pair} -> {direction} "
                f"(Score: {score}/10)"
            )

            return True

        logger.error(
            f"Failed to send Telegram alert: "
            f"Status {response.status_code}, "
            f"Response: {response.text}"
        )

        return False

    except Exception as e:

        logger.error(
            f"Telegram dispatch error exception: {e}"
        )

        return False


# ============================================================
# PUSHER
# ============================================================

def send_pusher_signal(
    display_name,
    direction,
    score,
    quality,
    bias,
    timeframe,
    target_epoch
):

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

        logger.error(
            f"Pusher trigger error: {e}"
        )


# ============================================================
# PROCESS VALID SIGNAL
# ============================================================

def process_signal_if_strong(
    symbol,
    display_name,
    timeframe,
    direction,
    score,
    quality,
    details,
    entry_price,
    analysis_epoch
):

    # --------------------------------------------------------
    # Only CALL / PUT
    # --------------------------------------------------------

    if direction not in ("CALL", "PUT"):
        return

    # --------------------------------------------------------
    # Base threshold
    #
    # Strategy itself handles special cases such as
    # counter-trend threshold.
    # --------------------------------------------------------

    try:
        threshold = int(
            SIGNAL_THRESHOLD_CALL_PUT
        )
    except Exception:
        threshold = 7

    if threshold < 1:
        threshold = 7

    if score < threshold:
        return

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # analysis_epoch = CLOSED candle N
    # target_epoch   = NEXT candle N+1
    # --------------------------------------------------------

    target_epoch = int(analysis_epoch) + 60

    target_key = (
        f"{symbol}_{timeframe}_{target_epoch}"
    )

    signal_key = (
        f"{symbol}_{timeframe}_{target_epoch}_{direction}"
    )

    # --------------------------------------------------------
    # Never generate another signal for the same target candle
    # --------------------------------------------------------

    with cache_lock:

        if target_key in target_signal_cache:
            logger.info(
                f"Signal already generated for "
                f"{display_name} target candle "
                f"{target_epoch}. Skipping."
            )
            return

        if signal_key in sent_telegram_cache:
            return

        # Reserve immediately to avoid race condition
        target_signal_cache.add(target_key)
        sent_telegram_cache.add(signal_key)

    # --------------------------------------------------------
    # Signal ID belongs to TARGET candle
    # --------------------------------------------------------

    signal_id = (
        f"{symbol}_{timeframe}_{target_epoch}"
    )

    bias = details.get(
        "bias",
        "NEUTRAL"
    )

    pa = details.get(
        "pa",
        ""
    )

    # --------------------------------------------------------
    # Save signal
    # candle_epoch = TARGET candle
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Telegram
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Pusher
    # --------------------------------------------------------

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
        f"NEXT-CANDLE SIGNAL: "
        f"{display_name} | "
        f"{direction} | "
        f"Score={score}/10 | "
        f"Analysis={analysis_epoch} | "
        f"Target={target_epoch} | "
        f"Telegram={telegram_sent}"
    )


# ============================================================
# STRATEGY EVALUATION ONLY
#
# IMPORTANT:
# This function DOES NOT send signals.
#
# Dashboard/API can call this safely.
# ============================================================

def evaluate_pair(symbol, display_name):

    cm = candle_managers.get(symbol)

    if not cm:
        return None

    try:

        df_1m = cm.get_closed_history("1M")
        df_5m = cm.get_closed_history("5M")
        df_15m = cm.get_closed_history("15M")

        if df_1m is None or len(df_1m) < 30:
            logger.warning(
                f"{display_name}: "
                f"Not enough 1M candles."
            )
            return None

        if df_5m is None or len(df_5m) < 30:
            logger.warning(
                f"{display_name}: "
                f"Not enough 5M candles."
            )
            return None

        if df_15m is None or len(df_15m) < 30:
            logger.warning(
                f"{display_name}: "
                f"Not enough 15M candles."
            )
            return None

        # ----------------------------------------------------
        # Strategy
        # ----------------------------------------------------

        direction, score, quality, details = evaluate_strategy(
            df_1m,
            df_5m,
            df_15m
        )

        # ----------------------------------------------------
        # Latest CLOSED 1M candle
        # ----------------------------------------------------

        latest_closed = cm.get_latest_closed_candle("1M")

        if not latest_closed:
            return None

        analysis_epoch = int(
            latest_closed.epoch
        )

        # ----------------------------------------------------
        # Entry reference
        #
        # This is the closing price of candle N.
        # It is used as the reference price for the
        # immediately following candle N+1.
        # ----------------------------------------------------

        entry_price = float(
            latest_closed.close
        )

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

        logger.error(
            f"Strategy evaluation error "
            f"for {display_name}: {e}"
        )

        return None


# ============================================================
# EVALUATE ALL PAIRS
#
# send_signal=False:
# Dashboard/API evaluation only.
#
# send_signal=True:
# Closed-candle event can generate next-candle signals.
# ============================================================

def evaluate_and_dispatch_all(
    send_signal=True,
    trigger_epoch=None
):

    with evaluation_lock:

        tz = pytz.timezone(
            TIMEZONE_NAME
        )

        now_str = datetime.datetime.now(
            tz
        ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        for sym, disp in FOREX_PAIRS.items():

            result = evaluate_pair(
                sym,
                disp
            )

            if not result:
                continue

            direction = result["direction"]
            score = result["score"]
            quality = result["quality"]
            details = result["details"]
            analysis_epoch = result["analysis_epoch"]
            target_epoch = result["target_epoch"]
            entry_price = result["entry_price"]

            # ------------------------------------------------
            # Dashboard state
            # ------------------------------------------------

            latest_evaluations[disp] = {
                "timestamp": now_str,
                "direction": direction,
                "score": score,
                "quality": quality,
                "bias": details.get(
                    "bias",
                    "NEUTRAL"
                ),
                "details": details.get(
                    "pa",
                    ""
                ),
                "analysis_candle_epoch": analysis_epoch,
                "target_candle_epoch": target_epoch
            }

            # ------------------------------------------------
            # ONLY candle-close event can dispatch signals
            # ------------------------------------------------

            if not send_signal:
                continue

            # If a specific trigger epoch was supplied,
            # ensure this is exactly that closed candle.
            if (
                trigger_epoch is not None
                and analysis_epoch != int(trigger_epoch)
            ):
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
#
# THIS IS THE PRIMARY SIGNAL TRIGGER.
#
# Candle N closes
#       ↓
# evaluate
#       ↓
# signal is for Candle N+1
# ============================================================

def on_candle_closed(event: CandleClosedEvent):

    if event.timeframe != "1M":
        return

    try:

        event_epoch = getattr(
            event,
            "epoch",
            None
        )

        logger.info(
            f"1M CANDLE CLOSED EVENT received: "
            f"epoch={event_epoch}"
        )

        # ----------------------------------------------------
        # Evaluate once per candle
        # ----------------------------------------------------

        if event_epoch is not None:

            event_key = int(
                event_epoch
            )

            with cache_lock:

                if event_key in processed_closed_candles:

                    logger.info(
                        f"Already processed "
                        f"1M candle {event_key}. "
                        f"Skipping duplicate event."
                    )

                    return

                processed_closed_candles.add(
                    event_key
                )

        # ----------------------------------------------------
        # Generate next-candle signal
        # ----------------------------------------------------

        evaluate_and_dispatch_all(
            send_signal=True,
            trigger_epoch=event_epoch
        )

    except Exception as e:

        logger.error(
            f"on_candle_closed error: {e}"
        )


event_dispatcher.subscribe(
    on_candle_closed
)


# ============================================================
# FAST MINUTE CHECKER
#
# IMPORTANT:
# This is NO LONGER a signal generator.
#
# It only checks whether a candle-close event was missed.
# ============================================================

def run_fast_minute_checker():

    logger.info(
        "Fast minute checker started "
        "(recovery mode only)."
    )

    last_minute = None

    while True:

        try:

            current_epoch = (
                int(time.time() // 60) * 60
            )

            current_minute = current_epoch // 60

            if last_minute is None:

                last_minute = current_minute

            elif current_minute != last_minute:

                last_minute = current_minute

                # Wait briefly for Deriv/CandleManager
                # to finalize the previous candle.
                time.sleep(1.5)

                # ------------------------------------------------
                # Check the latest closed candle from each pair.
                #
                # If event was already processed, this does
                # nothing.
                # ------------------------------------------------

                for sym, disp in FOREX_PAIRS.items():

                    cm = candle_managers.get(sym)

                    if not cm:
                        continue

                    latest_closed = (
                        cm.get_latest_closed_candle("1M")
                    )

                    if not latest_closed:
                        continue

                    closed_epoch = int(
                        latest_closed.epoch
                    )

                    with cache_lock:

                        already_processed = (
                            closed_epoch
                            in processed_closed_candles
                        )

                    if already_processed:
                        continue

                    logger.warning(
                        f"Missed candle-close event detected "
                        f"for {disp}. "
                        f"Running recovery evaluation."
                    )

                    with cache_lock:
                        processed_closed_candles.add(
                            closed_epoch
                        )

                    # Evaluate entire set only once.
                    evaluate_and_dispatch_all(
                        send_signal=True,
                        trigger_epoch=closed_epoch
                    )

                    break

            time.sleep(0.25)

        except Exception as e:

            logger.error(
                f"Fast minute checker error: {e}"
            )

            time.sleep(5)


# ============================================================
# ENGINE START
# ============================================================

def run_engine():

    logger.info(
        "=== Background engine thread starting "
        "(Next-Candle Multi-Timeframe Strategy) ==="
    )

    try:

        server_epoch = (
            deriv_client.get_server_epoch()
        )

        for deriv_symbol in FOREX_PAIRS.keys():

            try:

                logger.info(
                    f"Seeding history for "
                    f"{deriv_symbol}"
                )

                candles_1m = (
                    deriv_client
                    .fetch_historical_candles_sync(
                        deriv_symbol,
                        count=120,
                        granularity=60
                    )
                )

                candles_5m = (
                    deriv_client
                    .fetch_historical_candles_sync(
                        deriv_symbol,
                        count=100,
                        granularity=300
                    )
                )

                candles_15m = (
                    deriv_client
                    .fetch_historical_candles_sync(
                        deriv_symbol,
                        count=100,
                        granularity=900
                    )
                )

                if deriv_symbol not in candle_managers:
                    continue

                cm = candle_managers[
                    deriv_symbol
                ]

                if candles_1m:
                    cm.seed_historical_candles(
                        "1M",
                        candles_1m,
                        server_epoch
                    )

                if candles_5m:
                    cm.seed_historical_candles(
                        "5M",
                        candles_5m,
                        server_epoch
                    )

                if candles_15m:
                    cm.seed_historical_candles(
                        "15M",
                        candles_15m,
                        server_epoch
                    )

                logger.info(
                    f"History seeded successfully "
                    f"for {deriv_symbol}"
                )

            except Exception as e:

                logger.error(
                    f"Failed to seed history "
                    f"for {deriv_symbol}: {e}"
                )

        # ----------------------------------------------------
        # Start live Deriv connection
        # ----------------------------------------------------

        deriv_client.start()

        logger.info(
            "Deriv live engine started."
        )

    except Exception as e:

        logger.error(
            f"Engine startup error: {e}"
        )


# ============================================================
# OUTCOME WORKER
#
# Signal candle is N+1.
#
# Signal:
# analysis candle N
# target candle N+1
#
# Result is calculated only after target candle closes.
# ============================================================

def run_outcome_worker():

    while True:

        try:

            time.sleep(10)

            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT
                    signal_id,
                    symbol,
                    timeframe,
                    candle_epoch,
                    direction,
                    entry_reference_price
                FROM signal_history
                WHERE result = 'PENDING'
                """
            )

            rows = cursor.fetchall()

            for row in rows:

                (
                    sig_id,
                    sym,
                    tf,
                    target_epoch,
                    direction,
                    entry_price
                ) = row

                if not entry_price:
                    continue

                cm = candle_managers.get(
                    sym
                )

                if not cm:
                    continue

                latest_closed = (
                    cm.get_latest_closed_candle(tf)
                )

                if not latest_closed:
                    continue

                latest_epoch = int(
                    latest_closed.epoch
                )

                target_epoch = int(
                    target_epoch
                )

                # ------------------------------------------------
                # Wait until TARGET candle has closed.
                #
                # Example:
                # signal target = 12:31
                # do not evaluate before 12:32 candle-close.
                # ------------------------------------------------

                if latest_epoch < target_epoch:
                    continue

                # ------------------------------------------------
                # We use target candle close as exit.
                # ------------------------------------------------

                exit_price = float(
                    latest_closed.close
                )

                entry_price = float(
                    entry_price
                )

                if direction == "CALL":

                    if exit_price > entry_price:
                        outcome = "WIN"

                    elif exit_price < entry_price:
                        outcome = "LOSS"

                    else:
                        outcome = "TIE"

                elif direction == "PUT":

                    if exit_price < entry_price:
                        outcome = "WIN"

                    elif exit_price > entry_price:
                        outcome = "LOSS"

                    else:
                        outcome = "TIE"

                else:
                    continue

                cursor.execute(
                    """
                    UPDATE signal_history
                    SET
                        result = ?,
                        exit_reference_price = ?
                    WHERE signal_id = ?
                    """,
                    (
                        outcome,
                        exit_price,
                        sig_id
                    )
                )

                conn.commit()

                logger.info(
                    f"OUTCOME: "
                    f"{sym} | "
                    f"{direction} | "
                    f"{outcome} | "
                    f"Entry={entry_price} | "
                    f"Exit={exit_price}"
                )

            conn.close()

        except Exception as e:

            logger.error(
                f"Outcome worker error: {e}"
            )

            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def dashboard():

    return render_template(
        "dashboard.html"
    )


# ------------------------------------------------------------
# HEALTH
# ------------------------------------------------------------

@app.route("/health")
def health():

    return jsonify(
        {
            "status": "healthy",
            "deriv_connected": (
                deriv_client.is_connected
            ),
            "time": datetime.datetime.now().isoformat()
        }
    ), 200


# ------------------------------------------------------------
# DASHBOARD
# ------------------------------------------------------------

@app.route("/api/dashboard")
def api_dashboard():

    perf = get_today_performance()

    return jsonify(
        {
            "status": "online",
            "deriv_connected": (
                deriv_client.is_connected
            ),
            "performance": perf,
            "active_pairs": list(
                FOREX_PAIRS.values()
            )
        }
    )


# ------------------------------------------------------------
# ACTIVE SIGNALS
# ------------------------------------------------------------

@app.route("/api/active-signals")
@app.route("/api/signals/active")
def api_active_signals():

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
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
            candle_epoch
        FROM signal_history
        WHERE result = 'PENDING'
        ORDER BY candle_epoch DESC
        LIMIT 10
        """
    )

    rows = cursor.fetchall()

    conn.close()

    signals = []

    for r in rows:

        signals.append(
            {
                "signal_id": r[0],
                "pair": r[1],
                "timeframe": r[2],
                "timestamp": r[3],
                "direction": r[4],
                "score": r[5],
                "quality": r[6],
                "bias": r[7],
                "price": r[8],
                "target_candle_epoch": r[9]
            }
        )

    return jsonify(signals)


# ------------------------------------------------------------
# EVALUATIONS
#
# IMPORTANT:
# This does NOT send Telegram signals.
# ------------------------------------------------------------

@app.route("/api/evaluations")
def api_evaluations():

    evaluate_and_dispatch_all(
        send_signal=False
    )

    return jsonify(
        latest_evaluations
    )


# ------------------------------------------------------------
# HISTORY
# ------------------------------------------------------------

@app.route("/api/history")
def api_history():

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
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
        ORDER BY candle_epoch DESC
        LIMIT 50
        """
    )

    rows = cursor.fetchall()

    conn.close()

    history = []

    for r in rows:

        history.append(
            {
                "signal_id": r[0],
                "pair": r[1],
                "timeframe": r[2],
                "timestamp": r[3],
                "direction": r[4],
                "score": r[5],
                "quality": r[6],
                "bias": r[7],
                "entry_price": r[8],
                "exit_price": r[9],
                "result": r[10],
                "target_candle_epoch": r[11]
            }
        )

    return jsonify(history)


# ------------------------------------------------------------
# PERFORMANCE
# ------------------------------------------------------------

@app.route("/api/performance")
def api_performance():

    return jsonify(
        get_today_performance()
    )


# ------------------------------------------------------------
# PAIRS
# ------------------------------------------------------------

@app.route("/api/pairs")
def api_pairs():

    return jsonify(
        FOREX_PAIRS
    )


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

            # -----------------------------------------------
            # Live Deriv engine
            # -----------------------------------------------

            engine_thread = threading.Thread(
                target=run_engine,
                daemon=True,
                name="DerivEngine"
            )

            engine_thread.start()

            # -----------------------------------------------
            # Outcome worker
            # -----------------------------------------------

            outcome_thread = threading.Thread(
                target=run_outcome_worker,
                daemon=True,
                name="OutcomeWorker"
            )

            outcome_thread.start()

            # -----------------------------------------------
            # Recovery checker
            # -----------------------------------------------

            fast_checker_thread = threading.Thread(
                target=run_fast_minute_checker,
                daemon=True,
                name="MinuteRecoveryChecker"
            )

            fast_checker_thread.start()

            _threads_started = True

            logger.info(
                "Background engine, outcome worker "
                "and minute recovery checker "
                "successfully initialized."
            )

        except Exception as e:

            logger.error(
                f"Failed to initialize "
                f"background threads: {e}"
            )


# ============================================================
# FLASK REQUEST INITIALIZATION
# ============================================================

@app.before_request
def before_request_func():

    start_background_threads_once()


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_background_threads_once()

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )
