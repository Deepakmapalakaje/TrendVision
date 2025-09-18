#!/usr/bin/env python3
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
now = datetime.now(IST)

def insert_manual():
    conn = sqlite3.connect('database/upstox_v3_live_trading.db', timeout=30.0, detect_types=sqlite3.PARSE_DECLTYPES)
    c = conn.cursor()

    print("📝 Inserting manually crafted test data...")

    # Insert 4 test records for the main instruments
    c.execute('''INSERT OR REPLACE INTO latest_candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (1, 'NSE_INDEX|Nifty 50', 'NIFTY 50', 'INDEX', 0, '', now - timedelta(seconds=30), 25325.45, 25328.90, 25323.15, 25327.80, 0, 25327.80, 25326.42, 2.35, 0.0093, 0, 0.0, 0, 0, 0, 0, 45, 25325.45, 25327.80, '1min', 1, 'Buy CALL Option', 25327.80, 25332.85, 25322.75, None, 25325.45, 25328.90, 25323.15, now, now))

    c.execute('''INSERT OR REPLACE INTO latest_candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (2, 'NSE_FO|47723', '24950 CE', 'OPTION', 24950, 'CE', now - timedelta(seconds=35), 448.80, 448.80, 448.80, 448.80, 150, 448.80, 448.80, 0.00, 0.0000, 0, 0.0000, 0, 0, 0, 0, 15, 448.80, 448.80, '1min', 0, None, None, None, None, None, 448.80, 448.80, 448.80, now, now))

    c.execute('''INSERT OR REPLACE INTO latest_candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (3, 'NSE_FO|47734', '25000 PE', 'OPTION', 25000, 'PE', now - timedelta(seconds=28), 18.10, 18.10, 18.00, 18.10, 121950, 18.05, 18.05, 0.00, 0.0000, -3225, -0.0265, -3250, 1850, 0, 0, 87, 18.10, 18.10, '1min', -1, None, None, None, None, None, 18.10, 18.10, 18.00, now, now))

    c.execute('''INSERT OR REPLACE INTO latest_candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (4, 'NSE_FO|5001', 'NIFTY Future', 'FUTURE', 0, '', now - timedelta(seconds=25), 25325.80, 25329.65, 25324.20, 25328.95, 87500, 25327.42, 25327.42, 3.15, 0.0124, 2450, 0.0280, -1200, 2850, 8500, 6200, 234, 25325.80, 25328.95, '1min', 1, None, None, None, None, None, 25325.80, 25329.65

    conn.commit()

    c.execute("SELECT instrument_name, close, trend_value FROM latest_candles")
    rows = c.fetchall()
    print(f"✅ {len(rows)} records inserted successfully!")
    for row in rows:
        print(f"   {row[0]} - ₹{row[1]} - Trend: {row[2]}")

    conn.close()
    print("\n🎯 DASHBOARD READY WITH TEST DATA!")

if __name__ == "__main__":
    insert_manual()
