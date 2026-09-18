import os

# ------------------------------------------------------------------
# Every value below is actually read somewhere in the codebase.
# ------------------------------------------------------------------

SIGNALS_ENABLED = True
TELEGRAM_ENABLED = True
PUSHER_ENABLED = True

# Safety/diagnostic mode.
# এখন debugging-এর সময় এটা ON রাখুন।
PAPER_MODE = os.environ.get("PAPER_MODE", "1").strip().lower() not in {
    "0", "false", "no", "off"
}

# Live tick pipeline diagnostics
LIVE_TICK_DIAGNOSTICS = True
TICK_HEALTH_LOG_INTERVAL_SECONDS = 15.0
TICK_RESUBSCRIBE_AFTER_SECONDS = 45.0

# Deriv API
APP_ID = 1089
API_TOKEN = os.environ.get("API_TOKEN", "").strip()

DERIV_PING_INTERVAL_SECONDS = 25
DERIV_RECONNECT_MAX_BACKOFF = 32

# Telegram
TELEGRAM_BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN",
    "",
).strip()

TELEGRAM_CHAT_ID = (
    os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    or os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()
)

# Pusher
PUSHER_APP_ID = os.environ.get("PUSHER_APP_ID", "")
PUSHER_KEY = os.environ.get("PUSHER_KEY", "")
PUSHER_SECRET = os.environ.get("PUSHER_SECRET", "")
PUSHER_CLUSTER = os.environ.get("PUSHER_CLUSTER", "ap2")

# Candle history
CANDLE_HISTORY_LIMIT = 1000
MIN_1M_HISTORY = 20

# Live quote older than this is stale
STALE_TICK_THRESHOLD_SEC = 25.0

# Strategy threshold
SIGNAL_THRESHOLD_CALL_PUT = 6

# History diagnostics
HISTORY_DIAGNOSTICS = True

DB_PATH = os.environ.get(
    "DB_PATH",
    "signal_board.db",
)

TIMEZONE_NAME = "Asia/Dhaka"

# Scanner timing
SCAN_DELAY_SECONDS = 2.0
MAX_ENTRY_DELAY_SECONDS = 10.0

# Minute boundary repair
BOUNDARY_SYNC_CANDLE_COUNT = 4
BOUNDARY_SYNC_TIMEOUT_SECONDS = 2.0

ACTIVITY_BASELINE_CANDLES = 20

# 5M context
CONTEXT_5M_MIN_CANDLES = 10
MIN_ADX_5M = 20.0

# RSI
RSI_CALL_MIN = 55.0
RSI_CALL_MAX = 70.0

RSI_PUT_MIN = 30.0
RSI_PUT_MAX = 45.0

FOREX_PAIRS = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "USDCHF": "USD/CHF",
    "AUDUSD": "AUD/USD",
    "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD",
    "EURJPY": "EUR/JPY",
    "GBPJPY": "GBP/JPY",
    "EURGBP": "EUR/GBP",
    "EURCHF": "EUR/CHF",
    "GBPCHF": "GBP/CHF",
    "AUDJPY": "AUD/JPY",
    "CADJPY": "CAD/JPY",
    "CHFJPY": "CHF/JPY",
    "AUDCAD": "AUD/CAD",
}

ACTIVE_SYMBOLS = list(FOREX_PAIRS.keys())
