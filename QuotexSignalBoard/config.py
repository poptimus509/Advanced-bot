import os

# ------------------------------------------------------------------
# Every value below is actually read somewhere in the codebase.
# (The previous config.py had ~15 knobs that looked configurable but
# were never referenced anywhere - changing them silently did nothing.
# If you add a new setting here, also wire it into the code that should
# use it, and grep for it before assuming it works.)
# ------------------------------------------------------------------

SIGNALS_ENABLED = True
TELEGRAM_ENABLED = True
PUSHER_ENABLED = True

# Deriv API Configuration (used by data/deriv_client.py)
APP_ID = 1089
API_TOKEN = os.environ.get("API_TOKEN", "").strip()

DERIV_PING_INTERVAL_SECONDS = 25
DERIV_RECONNECT_MAX_BACKOFF = 32

# Credentials are intentionally NOT committed here. Set them as
# environment variables before deploying, and rotate any credentials
# that were ever committed to source control in the past.
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

# Minimum number of 1M candles required before a symbol is evaluated at
# all (bot.py enforces this before calling evaluate_strategy; note that
# evaluate_strategy also enforces its own internal minimum of 15 bars
# for indicator warm-up, so this should stay >= 15).
MIN_1M_HISTORY = 60

# A tick/quote older than this is treated as dead, not "the current
# price" (bot.py's live_quote()).
STALE_TICK_THRESHOLD_SEC = 25.0

# Strategy scoring. Max achievable score is 8 (four independent 2-point
# components: structure, EMA stack, RSI momentum, candle reaction) -
# see strategy.py. A threshold of 8 requires ALL FOUR to agree.
SIGNAL_THRESHOLD_CALL_PUT = 8

DB_PATH = os.environ.get("DB_PATH", "signal_board.db")
TIMEZONE_NAME = "Asia/Dhaka"

# Scan worker timing (bot.py run_scan_worker). Entries dispatched late
# in a minute have already missed a chunk of that candle's move, which
# was silently allowed before (window stretched to second 45). Keeping
# the window tight to the start of the minute keeps the recorded
# "entry price" close to the actual candle open used for the trade.
SCAN_DELAY_SECONDS = 2.0
MAX_ENTRY_DELAY_SECONDS = 10.0

ACTIVITY_BASELINE_CANDLES = 20

# Minimum 5M candles required before the 5M regime filter is trusted
# (strategy.py's _safe_5m_bias). Below this, the filter is skipped
# rather than guessed at.
CONTEXT_5M_MIN_CANDLES = 10

# ADX(14) computed on 5M candles. Below this, the 5M trend is
# considered too weak/choppy to trust as an alignment gate.
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
