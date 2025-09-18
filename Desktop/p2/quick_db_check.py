#!/usr/bin/env python3
import sqlite3
from datetime import datetime
print("="*50)
print("🔍 QUICK DATABASE CHECK")
print("="*50)

db_path = 'database/upstox_v3_live_trading.db'
try:
    conn = sqlite3.connect(db_path, timeout=30.0)
    cursor = conn.cursor()

    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='latest_candles'")
    if not cursor.fetchone():
        print("❌ No latest_candles table! Need to restart pipeline")
    else:
        # Check column count
        cursor.execute("PRAGMA table_info(latest_candles)")
        columns = cursor.fetchall()
        print(f"✅ Table exists with {len(columns)} columns")

        if len(columns) != 37:
            print(f"❌ Expected 37 columns, got {len(columns)}")
        else:
            print("✅ Correct schema!")

        # Check data
        cursor.execute("SELECT COUNT(*) FROM latest_candles")
        count = cursor.fetchone()[0]
        print(f"📊 Records: {count}")

        if count > 0:
            cursor.execute("SELECT instrument_key, close, volume, delta FROM latest_candles LIMIT 3")
            rows = cursor.fetchall()
            print("🔥 Latest Data:")
            for i, row in enumerate(rows, 1):
                print(f"  {i}. {row[0]} - ₹{row[1]:.2f} - Vol: {row[2]} - Delta: {row[3]}")

    conn.close()
except Exception as e:
    print(f"❌ Database error: {e}")

print("="*50)
