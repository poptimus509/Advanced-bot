import os

# ------------------------------------------------------------------
# Every value below is actually read somewhere in the codebase.
# ------------------------------------------------------------------

SIGNALS_ENABLED = True
TELEGRAM_ENABLED = True
PUSHER_ENABLED = True

# Deriv API Configuration (used by data/deriv_client.py)
APP_ID = 1089
API_TOKEN = os.environ.get("API_TOKEN", "").strip()

DERIV_PING_INTERVAL_SECONDS = 25
DERIV_RECONNECT_MAX_BACKOFF = 32

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = (
    os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    or os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()
)

PUSHER_APP_ID = os.environ.get("PUSHER_APP_ID", "")
PUSHER_KEY = os.environ.get("PUSHER_KEY", "")
PUSHER_SECRET = os.environ.get("PUSHER_SECRET", "")
PUSHER_CLUSTER = os.environ.get("PUSHER_CLUSTER", "ap2")

# How many closed 1M candles CandleManager keeps in memory per symbol.
CANDLE_HISTORY_LIMIT = 1000

# Minimum number of 1M candles required before a symbol is evaluated at all
MIN_1M_HISTORY = 20

# A tick/quote older than this is treated as dead, not "the current price"
STALE_TICK_THRESHOLD_SEC = 25.0

# Strategy scoring threshold (Reduced to 6 so valid currency pairs can trigger signals)
SIGNAL_THRESHOLD_CALL_PUT = 6

DB_PATH = os.environ.get("DB_PATH", "signal_board.db")
TIMEZONE_NAME = "Asia/Dhaka"

SCAN_DELAY_SECONDS = 2.0
MAX_ENTRY_DELAY_SECONDS = 10.0

ACTIVITY_BASELINE_CANDLES = 20

# Minimum 5M candles required before the 5M regime filter is trusted
CONTEXT_5M_MIN_CANDLES = 10

# ADX(14) computed on 5M candles
MIN_ADX_5M = 20.0

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
