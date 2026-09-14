import os

RUN_LEGACY_STRATEGY = False
PHASE_A_DATA_ENGINE_ONLY = False

SIGNALS_ENABLED = True
TELEGRAM_ENABLED = True
PUSHER_ENABLED = True

DERIV_APP_ID = 1089
DERIV_WS_URL = (
    f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
)
DERIV_PING_INTERVAL_SECONDS = 25
DERIV_RECONNECT_MAX_BACKOFF = 32

TELEGRAM_BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN", ""
).strip()

TELEGRAM_CHAT_ID = (
    os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    or os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()
)

PUSHER_APP_ID = os.environ.get("PUSHER_APP_ID", "")
PUSHER_KEY = os.environ.get("PUSHER_KEY", "")
PUSHER_SECRET = os.environ.get("PUSHER_SECRET", "")
PUSHER_CLUSTER = os.environ.get("PUSHER_CLUSTER", "ap2")

CANDLE_HISTORY_LIMIT = 1000
MIN_1M_HISTORY = 120

STALE_TICK_THRESHOLD_SEC = 25.0
SERVER_SYNC_TOLERANCE_SEC = 3.0

SIGNAL_THRESHOLD_CALL_PUT = 8
SIGNAL_THRESHOLD_WATCH = 5

# Existing bot uses the end of the entry minute as expiry.
HYPOTHETICAL_EXPIRY_SECONDS = 60

DB_PATH = os.environ.get("DB_PATH", "signal_board.db")
TIMEZONE_NAME = "Asia/Dhaka"

# Experimental strategy parameters; not backtest-optimized.
ATR_PERIOD = 14
SWING_REVERSAL_ATR = 1.5

ZONE_WIDTH_ATR = 0.25
MIN_CLEARANCE_ATR = 0.75

MAX_REACTION_RANGE_ATR = 2.5
MAX_ENTRY_DRIFT_ATR = 0.25

SCAN_DELAY_SECONDS = 2.0
MAX_ENTRY_DELAY_SECONDS = 10.0

# Only fully observed live M1 tick counts are used.
ACTIVITY_BASELINE_CANDLES = 20

# M5 context: no mandatory alignment and no signal-score points.
CONTEXT_5M_MIN_CANDLES = 10
CONTEXT_5M_RANK_WEIGHT = 0.05

SAME_PAIR_COOLDOWN_MINUTES = 0
ALLOW_NEXT_PAIR_DURING_COOLDOWN = True

REQUIRE_CURRENT_CANDLE_CONFIRMATION = True

# Compatibility exports for older modules.
# The replacement strategy does not use these as signal gates.
MIN_5M_HISTORY = 0
MIN_15M_HISTORY = 0
MIN_ADX_5M = 20.0

RSI_CALL_MIN = 55.0
RSI_CALL_MAX = 70.0
RSI_PUT_MIN = 30.0
RSI_PUT_MAX = 45.0

REQUIRE_15M_ALIGNMENT = False
REQUIRE_5M_1M_ALIGNMENT = False

FOREX_PAIRS = {
    "frxEURUSD": "EUR/USD",
    "frxGBPUSD": "GBP/USD",
    "frxUSDJPY": "USD/JPY",
    "frxUSDCHF": "USD/CHF",
    "frxAUDUSD": "AUD/USD",
    "frxUSDCAD": "USD/CAD",
    "frxNZDUSD": "NZD/USD",
    "frxEURJPY": "EUR/JPY",
    "frxGBPJPY": "GBP/JPY",
    "frxEURGBP": "EUR/GBP",
    "frxEURCHF": "EUR/CHF",
    "frxGBPCHF": "GBP/CHF",
    "frxAUDJPY": "AUD/JPY",
    "frxCADJPY": "CAD/JPY",
    "frxCHFJPY": "CHF/JPY",
    "frxAUDCAD": "AUD/CAD",
}
