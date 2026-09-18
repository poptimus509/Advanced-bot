import os

# ============================================================
# PAPER / RESEARCH MODE ONLY
# ============================================================

PAPER_MODE = True
SIGNALS_ENABLED = True

# No live trade delivery
TELEGRAM_ENABLED = False
PUSHER_ENABLED = False

TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = ""

# ============================================================
# SIMULATION
# ============================================================

SIMULATION_MODE = True
SIM_TICK_INTERVAL_SECONDS = 0.50
SIM_HISTORY_LIMIT = 1000

CANDLE_HISTORY_LIMIT = 1000
MIN_1M_HISTORY = 20

STALE_TICK_THRESHOLD_SEC = 10.0

# Scanner
SCAN_DELAY_SECONDS = 2.0
MAX_ENTRY_DELAY_SECONDS = 10.0

# Keep the existing strategy threshold.
SIGNAL_THRESHOLD_CALL_PUT = 6

# ============================================================
# STRATEGY SUPPORT
# ============================================================

CONTEXT_5M_MIN_CANDLES = 10
MIN_ADX_5M = 20.0

RSI_CALL_MIN = 55.0
RSI_CALL_MAX = 70.0

RSI_PUT_MIN = 30.0
RSI_PUT_MAX = 45.0

ACTIVITY_BASELINE_CANDLES = 20

TIMEZONE_NAME = "Asia/Dhaka"

DB_PATH = os.environ.get(
    "DB_PATH",
    "signal_board.db",
)

# ============================================================
# TEST SYMBOLS
# ============================================================

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

ACTIVE_SYMBOLS = list(
    FOREX_PAIRS.keys()
)
