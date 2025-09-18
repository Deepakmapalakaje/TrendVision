#!/usr/bin/env python3
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import random

def insert_test_data():
    """Insert test data into latest_candles table"""
    IST = ZoneInfo("Asia/Kolkata")
    db_path = 'database/upstox_v3_live_trading.db'

    try:
        conn = sqlite3.connect(db_path, timeout=30.0, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = conn.cursor()

        current_time = datetime.now(IST)

        test_data = [
            # NIFTY 50
            {
                'instrument_key': 'NSE_INDEX|Nifty 50',
                'instrument_name': 'NIFTY 50',
                'instrument_type': 'INDEX',
                'strike_price': 0,
                'option_type': '',
                'timestamp': current_time - timedelta(seconds=30),
                'open': 25325.45,
                'high': 25328.90,
                'low': 25323.15,
                'close': 25327.80,
                'volume': 0,
                'atp': 25327.80,
                'vwap': 25326.42,
                'price_change': 2.35,
                'price_change_pct': 0.0093,
                'delta': 0,
                'delta_pct': 0.0000,
                'min_delta': 0,
                'max_delta': 0,
                'buy_volume': 0,
                'sell_volume': 0,
                'tick_count': 45,
                'vtt_open': 25325.45,
                'vtt_close': 25327.80,
                'candle_interval': '1min',
                'trend_value': 1,  # UP
                'buy_recommendation': 'Buy CALL Option',
                'entry_price': 25327.80,
                'target': 25332.85,
                'sl': 25322.75,
                'profit_loss': None,
                'prev_close': 25325.45,
                'intraday_high': 25328.90,
                'intraday_low': 25323.15,
                'last_updated': current_time,
                'updated_at': current_time
            },
            # 24950 CE
            {
                'instrument_key': 'NSE_FO|47723',
                'instrument_name': '24950 CE',
                'instrument_type': 'OPTION',
                'strike_price': 24950,
                'option_type': 'CE',
                'timestamp': current_time - timedelta(seconds=35),
                'open': 448.80,
                'high': 448.80,
                'low': 448.80,
                'close': 448.80,
                'volume': 150,
                'atp': 448.80,
                'vwap': 448.80,
                'price_change': 0.00,
                'price_change_pct': 0.0000,
                'delta': 0,
                'delta_pct': 0.0000,
                'min_delta': 0,
                'max_delta': 0,
                'buy_volume': 0,
                'sell_volume': 0,
                'tick_count': 15,
                'vtt_open': 448.80,
                'vtt_close': 448.80,
                'candle_interval': '1min',
                'trend_value': 0,  # NEUTRAL
                'buy_recommendation': None,
                'entry_price': None,
                'target': None,
                'sl': None,
                'profit_loss': None,
                'prev_close': 448.80,
                'intraday_high': 448.80,
                'intraday_low': 448.80,
                'last_updated': current_time,
                'updated_at': current_time
            },
            # 25000 PE
            {
                'instrument_key': 'NSE_FO|47734',
                'instrument_name': '25000 PE',
                'instrument_type': 'OPTION',
                'strike_price': 25000,
                'option_type': 'PE',
                'timestamp': current_time - timedelta(seconds=28),
                'open': 18.10,
                'high': 18.10,
                'low': 18.00,
                'close': 18.10,
                'volume': 121950,
                'atp': 18.05,
                'vwap': 18.05,
                'price_change': 0.00,
                'price_change_pct': 0.0000,
                'delta': -3225,
                'delta_pct': -0.0265,
                'min_delta': -3250,
                'max_delta': 1850,
                'buy_volume': 0,
                'sell_volume': 0,
                'tick_count': 87,
                'vtt_open': 18.10,
                'vtt_close': 18.10,
                'candle_interval': '1min',
                'trend_value': -1,  # DOWN
                'buy_recommendation': None,
                'entry_price': None,
                'target': None,
                'sl': None,
                'profit_loss': None,
                'prev_close': 18.10,
                'intraday_high': 18.10,
                'intraday_low': 18.00,
                'last_updated': current_time,
                'updated_at': current_time
            },
            # NIFTY Future
            {
                'instrument_key': 'NSE_FO|5001',
                'instrument_name': 'NIFTY Future',
                'instrument_type': 'FUTURE',
                'strike_price': 0,
                'option_type': '',
                'timestamp': current_time - timedelta(seconds=25),
                'open': 25325.80,
                'high': 25329.65,
                'low': 25324.20,
                'close': 25328.95,
                'volume': 87500,
                'atp': 25327.42,
                'vwap': 25327.42,
                'price_change': 3.15,
                'price_change_pct': 0.0124,
                'delta': 2450,
                'delta_pct': 0.0280,
                'min_delta': -1200,
                'max_delta': 2850,
                'buy_volume': 8500,
                'sell_volume': 6200,
                'tick_count': 234,
                'vtt_open': 25325.80,
                'vtt_close': 25328.95,
                'candle_interval': '1min',
                'trend_value': 1,  # UP
                'buy_recommendation': None,
                'entry_price': None,
                'target': None,
                'sl': None,
                'profit_loss': None,
                'prev_close': 25325.80,
                'intraday_high': 25329.65,
                'intraday_low': 25324.20,
                'last_updated': current_time,
                'updated_at': current_time
            }
        ]

        print("🧪 Inserting test data into latest_candles table...")

        for i, data in enumerate(test_data, 1):
            values = list(data.values())
            placeholders = '?' * len(values)
            placeholders = ','.join(placeholders)

            cursor.execute(f'''
                INSERT OR REPLACE INTO latest_candles
                VALUES ({placeholders})
            ''', values)

            print(f"✅ Inserted test data #{i}: {data['instrument_name']}")

        conn.commit()

        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM latest_candles")
        count = cursor.fetchone()[0]
        print(f"\n✅ Total records in latest_candles: {count}")

        # Show sample data
        cursor.execute("SELECT instrument_name, close, trend_value, buy_recommendation FROM latest_candles")
        rows = cursor.fetchall()
        print("
💹 Test Data Preview:"        for row in rows:
            trend = "UP" if row[2] == 1 else "DOWN" if row[2] == -1 else "NEUTRAL"
            recommendation = f" ({row[3]})" if row[3] else ""
            print(f"  {row[0]} - ₹{row[1]:.2f} - {trend}{recommendation}")

        conn.close()
        print("\n🎯 TEST DATA INSERTED SUCCESSFULLY!")
        print("📊 Dashboard should now show live-looking data")
        print("🔗 Open: http://localhost:8080/dashboard")

        return True

    except Exception as e:
        print(f"❌ Error inserting test data: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("="*60)
    print("🧪 TRENDVISION TEST DATA INSERTION")
    print("="*60)

    success = insert_test_data()

    print("\n" + "="*60)
    if success:
        print("✅ Test data inserted! Dashboard should show:")
        print("   • Racha Analytics style market updates")
        print("   • Live price feeds for 4 instruments")
        print("   • Trend indicators (UP/DOWN/NEUTRAL)")
        print("   • Buy/Sell recommendations")
        print("   • Real performance metrics")
    else:
        print("❌ Failed to insert test data")
    print("="*60)
