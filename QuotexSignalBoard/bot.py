import logging
import math
import os
import threading
import time

import pandas as pd

from flask import (
    Flask,
    jsonify,
    render_template,
)

import config as cfg

from core.events import EventDispatcher
from data.candle_manager import CandleManager
from data.deriv_client import DerivClient
from strategy import (
    evaluate_strategy,
    prepare_history,
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "[%(levelname)s] "
        "%(name)s: "
        "%(message)s"
    ),
)

logger = logging.getLogger(
    "PaperResearchBot"
)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# SHARED STATE
# ============================================================

event_dispatcher = (
    EventDispatcher()
)

market_client = (
    DerivClient()
)

candle_managers = {}

live_state = {}

latest_evaluations = {}

paper_signals = []

state_lock = threading.RLock()

ready = threading.Event()

_threads_started = False

_threads_lock = threading.Lock()


# ============================================================
# SYMBOL HELPERS
# ============================================================

def normalize_symbol(symbol):
    return (
        str(symbol)
        .replace("frx", "")
        .replace("/", "")
        .upper()
    )


# ============================================================
# TICK HANDLER
# ============================================================

def create_tick_handler(
    symbol,
    manager,
):

    clean = normalize_symbol(
        symbol
    )

    def handle(
        epoch,
        price,
        receipt_time,
    ):
        try:
            epoch = int(epoch)

            price = float(price)

            if (
                not math.isfinite(
                    price
                )
                or price <= 0
            ):
                return

            with state_lock:
                live_state[
                    clean
                ] = {
                    "epoch":
                        epoch,
                    "price":
                        price,
                    "receipt":
                        float(
                            receipt_time
                        ),
                }

            manager.process_tick(
                epoch,
                price,
                receipt_time,
            )

        except Exception:
            logger.exception(
                "Tick handling failed "
                "for %s",
                clean,
            )

    return handle


# ============================================================
# CREATE CANDLE MANAGERS
# ============================================================

for symbol in cfg.FOREX_PAIRS:
    manager = CandleManager(
        symbol=symbol,
        event_dispatcher=(
            event_dispatcher
        ),
        max_history=getattr(
            cfg,
            "CANDLE_HISTORY_LIMIT",
            1000,
        ),
    )

    candle_managers[
        symbol
    ] = manager

    market_client.register_tick_handler(
        symbol,
        create_tick_handler(
            symbol,
            manager,
        ),
    )


# ============================================================
# LIVE QUOTE
# ============================================================

def live_quote(symbol):
    clean = normalize_symbol(
        symbol
    )

    with state_lock:
        row = live_state.get(
            clean
        )

        if row is None:
            return None

        row = dict(row)

    max_age = float(
        getattr(
            cfg,
            "STALE_TICK_THRESHOLD_SEC",
            10.0,
        )
    )

    epoch_age = (
        time.time()
        - row["epoch"]
    )

    receipt_age = (
        time.monotonic()
        - row["receipt"]
    )

    if (
        epoch_age > max_age
        or receipt_age > max_age
    ):
        return None

    return row


# ============================================================
# HISTORY
# ============================================================

def history_snapshot(
    symbol,
    timeframe="1M",
):
    manager = (
        candle_managers.get(
            symbol
        )
    )

    if manager is None:
        return pd.DataFrame()

    try:
        return (
            manager
            .get_closed_history(
                timeframe
            )
            .copy()
        )

    except Exception:
        return pd.DataFrame()


def seed_history():
    jobs = []

    for symbol in (
        cfg.FOREX_PAIRS
    ):
        jobs.append(
            {
                "key":
                    symbol,
                "symbol":
                    symbol,
                "count":
                    getattr(
                        cfg,
                        "SIM_HISTORY_LIMIT",
                        1000,
                    ),
                "granularity":
                    60,
            }
        )

    histories = (
        market_client
        .fetch_historical_candles_batch_sync(
            jobs
        )
    )

    server_epoch = int(
        market_client
        .get_server_time()
    )

    success = 0

    for symbol, manager in (
        candle_managers.items()
    ):
        raw = histories.get(
            symbol,
            [],
        )

        if not raw:
            continue

        try:
            manager.seed_historical_candles(
                "1M",
                raw,
                server_epoch,
            )

            success += 1

        except Exception:
            logger.exception(
                "History seed failed "
                "for %s",
                symbol,
            )

    logger.info(
        "Synthetic history seeded "
        "for %s/%s symbols.",
        success,
        len(
            cfg.FOREX_PAIRS
        ),
    )


# ============================================================
# PAPER SIGNAL RECORDING
# ============================================================

def record_paper_signal(
    candidate,
    target_epoch,
):
    quote = live_quote(
        candidate["symbol"]
    )

    if quote is None:
        logger.warning(
            "PAPER_SIGNAL skipped "
            "%s: no current "
            "synthetic quote",
            candidate["symbol"],
        )

        return

    item = {
        "timestamp":
            int(
                time.time()
            ),
        "target_epoch":
            int(
                target_epoch
            ),
        "symbol":
            candidate[
                "symbol"
            ],
        "pair":
            candidate[
                "display_name"
            ],
        "direction":
            candidate[
                "direction"
            ],
        "score":
            candidate[
                "score"
            ],
        "quality":
            candidate[
                "quality"
            ],
        "entry_reference":
            quote["price"],
        "mode":
            "PAPER_SYNTHETIC",
    }

    with state_lock:
        paper_signals.insert(
            0,
            item,
        )

        del paper_signals[
            100:
        ]

    logger.info(
        "PAPER_SIGNAL "
        "%s %s score=%s "
        "entry=%s",
        candidate[
            "display_name"
        ],
        candidate[
            "direction"
        ],
        candidate[
            "score"
        ],
        quote["price"],
    )


# ============================================================
# STRATEGY EVALUATION
# ============================================================

def evaluate_all(
    target_epoch,
):
    reports = {}

    candidates = []

    expected_last = (
        int(target_epoch)
        - 60
    )

    for (
        symbol,
        display,
    ) in cfg.FOREX_PAIRS.items():

        report = {
            "symbol":
                symbol,
            "pair":
                display,
            "direction":
                "NO_TRADE",
            "score":
                0,
            "quality":
                "WAIT",
            "mode":
                "PAPER_SYNTHETIC",
        }

        try:
            raw = history_snapshot(
                symbol,
                "1M",
            )

            if (
                raw.empty
                or "time"
                not in raw.columns
            ):
                report[
                    "reason"
                ] = "NO_HISTORY"

                reports[
                    display
                ] = report

                continue

            raw = raw[
                raw["time"]
                <= expected_last
            ].reset_index(
                drop=True
            )

            prepared = (
                prepare_history(
                    raw
                )
            )

            if (
                prepared.empty
                or len(prepared)
                < getattr(
                    cfg,
                    "MIN_1M_HISTORY",
                    20,
                )
            ):
                report[
                    "reason"
                ] = (
                    "INSUFFICIENT_HISTORY"
                )

                reports[
                    display
                ] = report

                continue

            latest_epoch = int(
                prepared.iloc[-1][
                    "time"
                ]
            )

            if (
                latest_epoch
                != expected_last
            ):
                report[
                    "reason"
                ] = (
                    "LATEST_CANDLE_"
                    "NOT_READY"
                )

                reports[
                    display
                ] = report

                continue

            df_5m = (
                history_snapshot(
                    symbol,
                    "5M",
                )
            )

            (
                direction,
                score,
                quality,
                details,
            ) = evaluate_strategy(
                prepared,
                df_5m,
                {
                    "adx_5m":
                        None
                },
            )

            report.update(
                {
                    "direction":
                        direction,
                    "score":
                        score,
                    "quality":
                        quality,
                    "reason":
                        details.get(
                            "score_reason",
                            "EVALUATED",
                        ),
                    "structure":
                        details.get(
                            "structure"
                        ),
                    "trend_5m":
                        details.get(
                            "trend_5m"
                        ),
                    "call_score":
                        details.get(
                            "call_score"
                        ),
                    "put_score":
                        details.get(
                            "put_score"
                        ),
                    "analysis_epoch":
                        latest_epoch,
                }
            )

            if direction in (
                "CALL",
                "PUT",
            ):
                candidates.append(
                    {
                        "symbol":
                            symbol,
                        "display_name":
                            display,
                        "direction":
                            direction,
                        "score":
                            score,
                        "quality":
                            quality,
                        "details":
                            details,
                    }
                )

        except Exception as exc:
            report[
                "reason"
            ] = (
                f"ERROR:"
                f"{type(exc).__name__}"
            )

            logger.exception(
                "Evaluation failed "
                "for %s",
                symbol,
            )

        reports[
            display
        ] = report

    candidates.sort(
        key=lambda x: (
            -x["score"],
            x["symbol"],
        )
    )

    with state_lock:
        latest_evaluations.clear()

        latest_evaluations.update(
            reports
        )

    logger.info(
        "Paper scan: "
        "%s symbols, "
        "%s candidates.",
        len(reports),
        len(candidates),
    )

    if candidates:
        record_paper_signal(
            candidates[0],
            target_epoch,
        )


# ============================================================
# SCANNER
# ============================================================

def scanner_worker():
    ready.wait()

    last_minute = None

    while True:
        try:
            now = int(
                time.time()
            )

            minute = (
                now // 60
            ) * 60

            seconds = (
                now - minute
            )

            if (
                minute
                != last_minute
                and seconds
                >= getattr(
                    cfg,
                    "SCAN_DELAY_SECONDS",
                    2,
                )
            ):
                evaluate_all(
                    minute
                )

                last_minute = (
                    minute
                )

            time.sleep(
                0.5
            )

        except Exception:
            logger.exception(
                "Paper scanner failed."
            )

            time.sleep(
                2
            )


# ============================================================
# ENGINE
# ============================================================

def engine_worker():
    try:
        # Seed historical data first.
        seed_history()

        # Start synthetic live ticks.
        market_client.start()

        ready.set()

        logger.info(
            "OFFLINE PAPER ENGINE "
            "READY. "
            "No external market "
            "service is connected."
        )

    except Exception:
        logger.exception(
            "Paper engine startup "
            "failed."
        )


# ============================================================
# THREAD STARTUP
# ============================================================

def start_background_threads_once():
    global _threads_started

    with _threads_lock:
        if _threads_started:
            return

        _threads_started = True

        threading.Thread(
            target=engine_worker,
            daemon=True,
            name="PaperEngine",
        ).start()

        threading.Thread(
            target=scanner_worker,
            daemon=True,
            name="PaperScanner",
        ).start()


start_background_threads_once()


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def dashboard():
    try:
        return render_template(
            "dashboard.html"
        )
    except Exception:
        return (
            "<h2>Offline Paper "
            "Research Bot</h2>"
            "<p>Use /health, "
            "/api/evaluations or "
            "/api/live-feed-diagnostics"
            "</p>"
        )


@app.route("/health")
def health():
    fresh = sum(
        live_quote(
            symbol
        )
        is not None
        for symbol
        in cfg.FOREX_PAIRS
    )

    return jsonify(
        {
            "status":
                (
                    "healthy"
                    if ready.is_set()
                    else "starting"
                ),
            "mode":
                "OFFLINE_SYNTHETIC",
            "paper_mode":
                True,
            "fresh_symbols":
                fresh,
            "total_symbols":
                len(
                    cfg.FOREX_PAIRS
                ),
            "real_money_execution":
                False,
        }
    )


@app.route(
    "/api/evaluations"
)
def api_evaluations():
    with state_lock:
        return jsonify(
            dict(
                latest_evaluations
            )
        )


@app.route(
    "/api/paper-signals"
)
def api_paper_signals():
    with state_lock:
        return jsonify(
            list(
                paper_signals
            )
        )


@app.route(
    "/api/live-feed-diagnostics"
)
def api_live_feed_diagnostics():
    client = (
        market_client
        .diagnostics()
    )

    output = {}

    for (
        symbol,
        display,
    ) in cfg.FOREX_PAIRS.items():

        clean = (
            normalize_symbol(
                symbol
            )
        )

        output[
            display
        ] = {
            "symbol":
                clean,
            "feed":
                client.get(
                    "symbols",
                    {},
                ).get(
                    clean,
                    {},
                ),
            "fresh_quote":
                live_quote(
                    symbol
                )
                is not None,
        }

    return jsonify(
        {
            "mode":
                "OFFLINE_SYNTHETIC",
            "connected":
                market_client
                .is_connected,
            "tick_rx_total":
                client.get(
                    "tick_rx_total",
                    0,
                ),
            "pairs":
                output,
        }
    )


@app.route(
    "/api/history-diagnostics"
)
def api_history_diagnostics():
    data = {}

    for (
        symbol,
        display,
    ) in cfg.FOREX_PAIRS.items():

        df_1m = history_snapshot(
            symbol,
            "1M",
        )

        df_5m = history_snapshot(
            symbol,
            "5M",
        )

        data[
            display
        ] = {
            "1m_count":
                int(
                    len(
                        df_1m
                    )
                ),
            "1m_last_epoch":
                (
                    int(
                        df_1m
                        .iloc[-1][
                            "time"
                        ]
                    )
                    if (
                        not df_1m.empty
                        and "time"
                        in df_1m.columns
                    )
                    else None
                ),
            "5m_count":
                int(
                    len(
                        df_5m
                    )
                ),
        }

    return jsonify(
        {
            "mode":
                "OFFLINE_SYNTHETIC",
            "pairs":
                data,
        }
    )


@app.route(
    "/api/dashboard"
)
def api_dashboard():
    return jsonify(
        {
            "status":
                (
                    "online"
                    if ready.is_set()
                    else "starting"
                ),
            "mode":
                "OFFLINE_SYNTHETIC",
            "paper_mode":
                True,
            "real_money_execution":
                False,
            "signals_recorded":
                len(
                    paper_signals
                ),
        }
    )


@app.route("/api/pairs")
def api_pairs():
    return jsonify(
        cfg.FOREX_PAIRS
    )


# ============================================================
# LOCAL
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                10000,
            )
        ),
        debug=False,
    )
