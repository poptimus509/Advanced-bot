import os

# Database Path Configuration
DB_PATH = os.getenv("DB_PATH", "signals.db")

# Deriv API Configuration
APP_ID = os.getenv("APP_ID", "1089")
API_TOKEN = os.getenv("API_TOKEN", "")

# Active Trading Symbols
ACTIVE_SYMBOLS = [
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "USDCAD",
    "NZDUSD",
    "AUDUSD",
    "EURGBP",
    "GBPJPY",
    "EURJPY",
    "AUDJPY",
    "CADJPY",
    "CHFJPY",
    "EURAUD",
    "GBPCAD",
    "EURCAD"
]

# Timeframe & Strategy Settings
TIMEFRAME = 60  # 1 Minute in seconds
SIGNALS_ENABLED = True
