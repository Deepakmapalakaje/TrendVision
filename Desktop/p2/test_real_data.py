#!/usr/bin/env python3
"""Test if latest_candles table has live data"""

import sqlite3
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def test_latest_candles_data():
    """Test if latest_candles table has data"""
    IST = ZoneInfo("Asia/Kolkata")

    db_path = 'database/upstox_v3_live_trading.db'

    try:
        conn = sqlite3.connect(db_path, timeout=30.0, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        print("🔍 Checking latest_candles table...")

        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='latest_candles'")
        table_exists = cursor.fetchone()

        if not table_exists:
            print("❌ latest_candles table does not exist!")
            return False

        # Check table schema
        print("\n📋 Table Schema:")
        cursor.execute("PRAGMA table_info(latest_candles)")
        columns = cursor.fetchall()
        expected_columns = 37

        if len(columns) != expected_columns:
            print(f"❌ Incorrect column count: {len(columns)} != {expected_columns}")
            return False

        print(f"✅ Correct schema with {len(columns)} columns:")
        for col in columns:
            print(f"   {col[1]} - {col[2]}")

        # Check data
        print("
🔥 Live Data Check:"        cursor.execute("SELECT COUNT(*) as count FROM latest_candles")
        count_row = cursor.fetchone()
        total_count = count_row[0]

        print(f"📊 Total records: {total_count}")

        if total_count == 0:
            print("❌ No data in latest_candles table - pipeline not saving data!")
            print("💡 Tip: Check database schema and pipeline logs")

            # Check trend table too
            cursor.execute("SELECT COUNT(*) as trend_count FROM trend")
            trend_count_row = cursor.fetchone()
            trend_count = trend_count_row[0]
            print(f"🎯 Trend records: {trend_count}")

            return False

        # Show recent data
        print(f"\n🔥 Recent Data (last {min(total_count, 3)} records):")

        cursor.execute("""
            SELECT instrument_key, instrument_name, close, volume, delta,
                   last_updated, candle_interval
            FROM latest_candles
            ORDER BY last_updated DESC
            LIMIT 3
        """)

        for i, row in enumerate(cursor.fetchall(), 1):
            print(f"\n📄 Record #{i}:")
            print(f"   Symbol: {row[0]} ({row[1]})")
            print(f"   Close: ₹{row[2]:.2f}")
            print(f"   Volume: {row[3]:,}")
            print(f"   Delta: {row[4]}")
            print(f"   Interval: {row[6]}")
            print(f"   Updated: {row[5]}")

        # Check for real-time updates
        print("
💹 Real-time Status:"        cursor.execute("""
            SELECT COUNT(*) as recent_count
            FROM latest_candles
            WHERE last_updated > ?
        """, ((datetime.now(IST) - timedelta(minutes=1)).isoformat(),))

        recent_count = cursor.fetchone()[0]
        print(f"📈 Records updated in last 1 minute: {recent_count}")

        if recent_count > 0:
            print("🌟 Real-time data is being updated!")
        else:
            print("⚠️ No recent updates - data may be stale")

        # Summary
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                AVG(volume) as avg_volume,
                AVG(delta) as avg_delta,
                MAX(last_updated) as latest_update
            FROM latest_candles
        """)

        summary = cursor.fetchone()
        print("
📊 Data Summary:"        print(f"   Total Records: {summary[0]}")
        print(f"   Avg Volume: {summary[1]:,.0f}" if summary[1] else "   Avg Volume: N/A")
        print(f"   Avg Delta: {summary[2]:.1f}" if summary[2] else "   Avg Delta: N/A")
        print(f"   Last Update: {summary[3]}")

        conn.close()

        print("
✨ SUCCESS: Database has live trading data! 🎯"        if datetime.now(IST) - datetime.fromisoformat(summary[3]) < timedelta(minutes=5):
            print("🔥 Data is currently live and updating!")
        else:
            print("⚠️ Data may be stale (last update > 5 minutes)")

        return True

    except Exception as e:
        print(f"❌ Error checking database: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("="*60)
    print("🧪 TRENDVISION DATABASE DATA TEST")
    print("="*60)

    success = test_latest_candles_data()

    print("\n" + "="*60)
    if success:
        print("✅ Latest candles table has data - dashboard should display!")
        print("🔗 Open dashboard: http://localhost:8080/dashboard")
    else:
        print("❌ Table empty or schema mismatch - check pipeline logs")
    print("="*60)
