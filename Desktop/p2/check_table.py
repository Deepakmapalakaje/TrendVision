#!/usr/bin/env python3
import sqlite3
import json

def check_table_structure():
    """Compare table structure between pipeline and current database"""
    db_path = 'database/upstox_v3_live_trading.db'

    try:
        conn = sqlite3.connect(db_path, timeout=30.0)
        cursor = conn.cursor()

        print("="*60)
        print("🔍 DETAILED TABLE STRUCTURE ANALYSIS")
        print("="*60)

        # Get current table schema
        cursor.execute("PRAGMA table_info(latest_candles)")
        current_columns = cursor.fetchall()

        print("📋 CURRENT TABLE SCHEMA:")
        for i, col in enumerate(current_columns):
            print(f"  {i}. {col[1]} - {col[2]}")

        print(f"\n🔢 CURRENT COLUMN COUNT: {len(current_columns)}\n")

        # Check what the pipeline expects to insert
        cursor.execute("SELECT COUNT(*) FROM latest_candles")
        count = cursor.fetchone()[0]
        print(f"🔥 TABLE HAS {count} ROWS")

        conn.close()

    except Exception as e:
        print(f"❌ Database check failed: {e}")

if __name__ == "__main__":
    check_table_structure()
