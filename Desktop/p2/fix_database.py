import sqlite3
import os
from datetime import datetime

def fix_latest_candles_table():
    """Drop and recreate the latest_candles table with correct schema"""

    db_path = 'database/upstox_v3_live_trading.db'

    # Connect to database
    conn = sqlite3.connect(db_path, timeout=30.0, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()

    print("Dropping old latest_candles table...")
    cursor.execute("DROP TABLE IF EXISTS latest_candles")

    print("Creating new latest_candles table with all columns...")
    cursor.execute('''
    CREATE TABLE latest_candles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instrument_key TEXT UNIQUE NOT NULL,
        instrument_name TEXT NOT NULL,
        instrument_type TEXT NOT NULL,
        strike_price REAL DEFAULT 0,
        option_type TEXT,
        timestamp TIMESTAMP NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume INTEGER NOT NULL,
        atp REAL NOT NULL,
        vwap REAL DEFAULT 0,
        price_change REAL DEFAULT 0,
        price_change_pct REAL DEFAULT 0,
        delta INTEGER DEFAULT 0,
        delta_pct REAL DEFAULT 0,
        min_delta INTEGER DEFAULT 0,
        max_delta INTEGER DEFAULT 0,
        buy_volume INTEGER DEFAULT 0,
        sell_volume INTEGER DEFAULT 0,
        tick_count INTEGER DEFAULT 0,
        vtt_open REAL DEFAULT 0,
        vtt_close REAL DEFAULT 0,
        candle_interval TEXT NOT NULL,
        trend_value INTEGER DEFAULT 0,
        buy_recommendation TEXT,
        entry_price REAL,
        target REAL,
        sl REAL,
        profit_loss REAL,
        prev_close REAL DEFAULT 0,
        intraday_high REAL DEFAULT 0,
        intraday_low REAL DEFAULT 0,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Create index
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_latest_candles_instrument ON latest_candles(instrument_key)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_latest_candles_updated ON latest_candles(updated_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_latest_candles_timestamp ON latest_candles(timestamp)')

    conn.commit()
    conn.close()

    print("✅ Database schema fixed successfully!")

    # Test the schema by checking column count
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(latest_candles)")
    columns = cursor.fetchall()
    conn.close()

    print(f"✅ Table now has {len(columns)} columns:")
    for col in columns:
        print(f"   {col[1]} - {col[2]}")


if __name__ == "__main__":
    fix_latest_candles_table()
