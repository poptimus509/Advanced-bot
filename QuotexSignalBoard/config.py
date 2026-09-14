import os

# Database & Timezone Configuration
DB_PATH = os.getenv("DB_PATH", "signals.db")
TIMEZONE_NAME = os.getenv("TIMEZONE_NAME", "Asia/Dhaka")

# Deriv API Configuration
APP_ID = os.getenv("APP_ID", "1089")
API_TOKEN = os.getenv("API_TOKEN", "")

# Forex Pairs Dictionary & Active Symbols
FOREX_PAIRS = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "USDCHF": "USD/CHF",
    "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD",
    "AUDUSD": "AUD/USD",
    "EURGBP": "EUR/GBP",
    "GBPJPY": "GBP/JPY",
    "EURJPY": "EUR/JPY",
    "AUDJPY": "AUD/JPY",
    "CADJPY": "CAD/JPY",
    "CHFJPY": "CHF/JPY",
    "EURAUD": "EUR/AUD",
    "GBPCAD": "GBP/CAD",
    "EURCAD": "EUR/CAD"
}

ACTIVE_SYMBOLS = list(FOREX_PAIRS.keys())

# Strategy & History Settings
TIMEFRAME = 60
CANDLE_HISTORY_LIMIT = 100
MIN_1M_HISTORY = 50  # strategy.py এর জন্য এটি জরুরি
SIGNALS_ENABLED = True

# Thresholds & Delays
STALE_TICK_THRESHOLD_SEC = 5
SERVER_SYNC_TOLERANCE_SEC = 5
MAX_ENTRY_DELAY_SECONDS = 15
SCAN_DELAY_SECONDS = 2
MAX_ENTRY_DRIFT_ATR = 1.5

# Telegram Settings
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Pusher Settings
PUSHER_ENABLED = os.getenv("PUSHER_ENABLED", "false").lower() == "true"
PUSHER_APP_ID = os.getenv("PUSHER_APP_ID", "")
PUSHER_KEY = os.getenv("PUSHER_KEY", "")
PUSHER_SECRET = os.getenv("PUSHER_SECRET", "")
PUSHER_CLUSTER = os.getenv("PUSHER_CLUSTER", "ap2")
