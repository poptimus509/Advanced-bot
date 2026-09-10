import sqlite3
import datetime
from config import DB_PATH

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS signal_history (
        signal_id TEXT PRIMARY KEY,
        symbol TEXT,
        display_pair TEXT,
        timeframe TEXT,
        candle_epoch INTEGER,
        signal_timestamp_utc TEXT,
        signal_timestamp_bdt TEXT,
        direction TEXT,
        score INTEGER,
        quality TEXT,
        bias_15m TEXT,
        entry_reference_price REAL,
        entry_reference_timestamp TEXT,
        expiry_timestamp TEXT,
        expiry_price REAL,
        result TEXT,
        result_timestamp TEXT,
        telegram_message_id TEXT,
        delivery_status TEXT,
        api_latency_ms REAL,
        created_at TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS telegram_delivery_history (
        delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id TEXT,
        attempted_at TEXT,
        success BOOLEAN,
        message_id TEXT,
        latency_ms REAL,
        error_category TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_performance (
        report_date TEXT PRIMARY KEY,
        total_signals INTEGER,
        call_count INTEGER,
        put_count INTEGER,
        settled_count INTEGER,
        win_count INTEGER,
        loss_count INTEGER,
        tie_count INTEGER,
        pending_count INTEGER,
        unknown_count INTEGER,
        win_rate REAL
    )
    """)
    
    conn.commit()
    conn.close()