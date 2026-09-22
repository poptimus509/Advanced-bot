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

# ------------------------------------------------------------------
# STRATEGY: Pullback-then-continuation (replaces 8/8 momentum chasing)
# Score components (max 8): trend(2) + pullback(2) + continuation(2)
#                           + RSI slope(2). Threshold is 6. Decided.
# ------------------------------------------------------------------
SIGNAL_THRESHOLD_CALL_PUT = 6

# Expiry: 5 minutes (broker edge is far smaller than 1M turbo expiry)
EXPIRY_SECONDS = 300

# Quotex payout % for this asset class (used for payout-aware tracking).
# Update to your real observed payout; expectancy = WR*payout - (1-WR)
PAYOUT_PERCENT = 80.0

# 5M regime filter
CONTEXT_5M_MIN_CANDLES = 10
MIN_ADX_5M = 20.0

# Pullback zone on RSI(14) of the 1M series, WITH the higher trend.
# CALL (uptrend): RSI dips into 40-50 then recovers with a bullish candle.
# PUT  (downtrend): RSI rises into 50-60 then rejects with a bearish candle.
RSI_PULLBACK_CALL_MIN = 40.0
RSI_PULLBACK_CALL_MAX = 50.0
RSI_PULLBACK_PUT_MIN = 50.0
RSI_PULLBACK_PUT_MAX = 60.0
PULLBACK_LOOKBACK_CANDLES = 6

ACTIVITY_BASELINE_CANDLES = 20

# Signal must reach Telegram within 5 seconds of the new candle opening.
# The scan window is [SCAN_DELAY_SECONDS, MAX_ENTRY_DELAY_SECONDS] after
# the candle boundary.
SCAN_DELAY_SECONDS = 0.2
MAX_ENTRY_DELAY_SECONDS = 5.0

# Per-pair auto-filter: a pair with this many settled signals and a win
# rate below AUTO_FILTER_MIN_WIN_RATE gets disabled automatically.
AUTO_FILTER_MIN_TRADES = 30
AUTO_FILTER_MIN_WIN_RATE = 0.50

# Real-market forex only. No OTC pairs are subscribed or evaluated.
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
