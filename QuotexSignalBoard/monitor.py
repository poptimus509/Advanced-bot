import datetime
import sqlite3
import pytz
from config import TIMEZONE_NAME
from database import get_db_connection

def calculate_win_rate(wins, losses, ties=0):
    """
    Ties are counted against the trader in the denominator (a break-even
    or refunded trade is not a win). Excluding ties from the denominator,
    as the previous version did, inflates the displayed win rate.
    """
    settled = wins + losses + ties
    if settled == 0:
        return 0.0
    return (wins / settled) * 100.0

def get_today_performance():
    tz = pytz.timezone(TIMEZONE_NAME)
    today_bdt = datetime.datetime.now(tz).strftime("%Y-%m-%d")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row
    
    cursor.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN direction = 'CALL' THEN 1 ELSE 0 END) as calls,
        SUM(CASE WHEN direction = 'PUT' THEN 1 ELSE 0 END) as puts,
        SUM(CASE WHEN result IN ('WIN', 'LOSS', 'TIE') THEN 1 ELSE 0 END) as settled,
        SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END) as losses,
        SUM(CASE WHEN result = 'TIE' THEN 1 ELSE 0 END) as ties,
        SUM(CASE WHEN result = 'PENDING' THEN 1 ELSE 0 END) as pending,
        SUM(CASE WHEN result = 'UNKNOWN' THEN 1 ELSE 0 END) as unknown
    FROM signal_history
    WHERE substr(signal_timestamp_bdt, 1, 10) = ?
    """, (today_bdt,))
    
    row = cursor.fetchone()
    conn.close()
    
    total = row["total"] or 0
    calls = row["calls"] or 0
    puts = row["puts"] or 0
    settled = row["settled"] or 0
    wins = row["wins"] or 0
    losses = row["losses"] or 0
    ties = row["ties"] or 0
    pending = row["pending"] or 0
    unknown = row["unknown"] or 0
    
    wr = calculate_win_rate(wins, losses, ties)
    
    return {
        "date": today_bdt,
        "total": total,
        "calls": calls,
        "puts": puts,
        "settled": settled,
        "wins": wins,
        "losses": losses,
        "ties": ties,
        "pending": pending,
        "unknown": unknown,
        "win_rate": round(wr, 2)
    }
