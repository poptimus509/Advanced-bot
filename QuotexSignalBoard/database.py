import os
import sqlite3
import logging

import config as cfg

logger = logging.getLogger("QuotexSignalBoard")


def get_db_connection():
    conn = sqlite3.connect(cfg.DB_PATH, check_same_thread=False, timeout=10)
    conn.row_factory = None
    return conn


def init_db():
    conn = get_db_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_history (
                signal_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                display_pair TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                candle_epoch INTEGER NOT NULL,
                signal_timestamp_utc TEXT NOT NULL,
                signal_timestamp_bdt TEXT NOT NULL,
                direction TEXT NOT NULL,
                score INTEGER NOT NULL,
                quality TEXT NOT NULL,
                bias_15m TEXT DEFAULT 'NOT_USED',
                entry_reference_price REAL,
                entry_reference_timestamp TEXT,
                exit_reference_price REAL,
                result TEXT DEFAULT 'PENDING',
                created_at TEXT,
                payout_percent REAL DEFAULT 80.0,
                expiry_seconds INTEGER DEFAULT 60,
                delivery_status TEXT DEFAULT 'SENT'
            )
            """
        )
        conn.commit()

        # Migration for existing databases: add payout-aware columns if missing.
        existing = {row[1] for row in conn.execute("PRAGMA table_info(signal_history)").fetchall()}
        migrations = {
            "delivery_status": "ALTER TABLE signal_history ADD COLUMN delivery_status TEXT DEFAULT 'SENT'",
            "payout_percent": "ALTER TABLE signal_history ADD COLUMN payout_percent REAL DEFAULT 80.0",
            "expiry_seconds": "ALTER TABLE signal_history ADD COLUMN expiry_seconds INTEGER DEFAULT 300",
            "exit_reference_price": "ALTER TABLE signal_history ADD COLUMN exit_reference_price REAL",
        }
        for col, stmt in migrations.items():
            if col not in existing:
                try:
                    conn.execute(stmt)
                    conn.commit()
                except sqlite3.Error as e:
                    logger.warning("Migration for column '%s' skipped: %s", col, e)
    finally:
        conn.close()
