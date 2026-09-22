"""
Configuration settings for Quotex Signal Board (1M Precision & 24/7 Execution).
"""

import os

DB_PATH = os.environ.get("DB_PATH", "signal_board.db")
TIMEZONE_NAME = "Asia/Dhaka"

# ------------------------------------------------------------------
# Operational Toggles
# ------------------------------------------------------------------
SIGNALS_ENABLED = os.environ.get("SIGNALS_ENABLED", "True").lower() == "true"
TELEGRAM_ENABLED = os.environ.get("TELEGRAM_ENABLED", "True").lower() == "true"
PUSHER_ENABLED = os.environ.get("PUSHER_ENABLED", "True").lower() == "true"

# Deriv API Configuration
APP_ID = int(os.environ.get("APP_ID", "1089"))
API_TOKEN = os.environ.get("API_TOKEN", "").strip()

DERIV_PING_INTERVAL_SECONDS = 25
DERIV_RECONNECT_MAX_BACKOFF = 32

# Telegram Configuration
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = (
    os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    or os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()
)

# Pusher Configuration
PUSHER_APP_ID = os.environ.get("PUSHER_APP_ID", "")
PUSHER_KEY = os.environ.get("PUSHER_KEY", "")
PUSHER_SECRET = os.environ.get("PUSHER_SECRET", "")
PUSHER_CLUSTER = os.environ.get("PUSHER_CLUSTER", "ap2")

# Candle buffer limits
CANDLE_HISTORY_LIMIT = 1000
MIN_1M_HISTORY = 20
STALE_TICK_THRESHOLD_SEC = 25.0

# ------------------------------------------------------------------
# STRATEGY TUNING & 24/7 EXECUTION TIMING
# ------------------------------------------------------------------
SIGNAL_THRESHOLD_CALL_PUT = 7
EXPIRY_SECONDS = 60

# Payout settings
PAYOUT_PERCENT = 85.0
MIN_PAYOUT_PERCENT = 85.0

# 5M Regime Gate & Volatility
CONTEXT_5M_MIN_CANDLES = 10
MIN_ADX_5M = 20.0

# Pullback / Entry Boundaries
RSI_PULLBACK_CALL_MIN = 40.0
RSI_PULLBACK_CALL_MAX = 50.0
RSI_PULLBACK_PUT_MIN = 50.0
RSI_PULLBACK_PUT_MAX = 60.0
PULLBACK_LOOKBACK_CANDLES = 6

ACTIVITY_BASELINE_CANDLES = 20

# Latency & Entry Window Filters
SCAN_DELAY_SECONDS = 1.0
MAX_ENTRY_DELAY_SECONDS = 5.0

# Cooldown
PAIR_COOLDOWN_MINUTES = 10

# ------------------------------------------------------------------
# 24/7 FULL OPERATION (NO RESTRICTION)
# ------------------------------------------------------------------
SIGNAL_HOURS_UTC = (0, 24)

# Auto-filter guards
AUTO_FILTER_MIN_TRADES = 30
AUTO_FILTER_MIN_WIN_RATE = 0.50

# Active Real-Market Forex Pairs
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
