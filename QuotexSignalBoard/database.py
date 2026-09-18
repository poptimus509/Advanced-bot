import sqlite3
import datetime
from config import DB_PATH

def get_db_connection():
    # timeout: wait for locks instead of raising "database is locked"
    # immediately - with 16 pairs scanned every minute and several
    # short-lived connections opened per scan, contention is routine.
    # WAL mode lets readers and the single writer proceed concurrently
    # instead of serializing on a single lock.
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
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
        exit_reference_price REAL,
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
    
    # Auto-migration: Check if exit_reference_price column exists, if not add it dynamically
    try:
        cursor.execute("PRAGMA table_info(signal_history)")
        columns = [column["name"] for column in cursor.fetchall()]
        if "exit_reference_price" not in columns:
            cursor.execute("ALTER TABLE signal_history ADD COLUMN exit_reference_price REAL")
    except Exception as e:
        pass
    
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
