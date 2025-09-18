#!/usr/bin/env python3
"""Test API endpoint to verify latest_candles data"""

import requests
import json

def test_api_summary():
    """Test the /api/summary endpoint"""
    url = "http://localhost:8080/api/summary?interval=1min"

    print("🔍 Testing API Summary Endpoint...")
    print(f"📡 URL: {url}")

    try:
        response = requests.get(url, timeout=10)
        print(f"📊 Response Status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print("✅ API Response OK")

            if data.get('ok'):
                candles = data.get('candles', [])
                print(f"📈 Number of candles returned: {len(candles)}")

                if candles:
                    print("\n🎯 Sample Candle Data:")
                    candle = candles[0]  # First candle
                    print(f"  - Instrument: {candle.get('instrument_key', 'N/A')}")
                    print(f"  - Symbol: {candle.get('symbol', 'N/A')}")
                    print(f"  - OHLC: {candle.get('open', 0):.2f} / {candle.get('high', 0):.2f} / {candle.get('low', 0):.2f} / {candle.get('close', 0):.2f}")
                    print(f"  - Volume: {candle.get('volume', 0):,}")
                    print(f"  - Price Change: {candle.get('price_change_pct', 0):.2f}%")
                    print(f"  - Delta: {candle.get('delta', 0)}")
                    print(f"  - Trend: {candle.get('trend_value', 0)}")
                    print(f"  - Timestamp: {candle.get('timestamp', 'N/A')}")

                    print("\n🎯 Market Summary:")
                    summary = data.get('market_summary', {})
                    print(f"  - Total Instruments: {summary.get('total_instruments', 0)}")
                    print(f"  - Total Volume: {summary.get('total_volume', 0):,}")
                    print(f"  - Sentiment: {summary.get('market_sentiment', 'N/A')}")
                    print(f"  - Positive Moves: {summary.get('positive_moves', 0)}")
                    print(f"  - Negative Moves: {summary.get('negative_moves', 0)}")

                    print("\n🎯 Performance Metrics:")
                    metrics = data.get('performance_metrics', {})
                    print(f"  - Total Delta: {metrics.get('total_delta', 0):,}")
                    print(f"  - Avg Delta: {metrics.get('avg_delta', 0):.2f}")
                    print(f"  - Total Ticks: {metrics.get('total_tick_count', 0):,}")

                    print("\n📊 Complete Candles Data:")
                    for i, candle in enumerate(candles, 1):
                        print(f"{i}. {candle.get('instrument_key')} - {candle.get('symbol')} - {candle.get('close', 0):.2f}")
                else:
                    print("❌ No candles returned - database might be empty")

                    # Try alternative API with 5min interval
                    print("\n🔄 Trying 5min interval...")
                    response_5m = requests.get("http://localhost:8080/api/summary?interval=5min", timeout=10)
                    if response_5m.status_code == 200:
                        data_5m = response_5m.json()
                        candles_5m = data_5m.get('candles', [])
                        print(f"📈 5min candles: {len(candles_5m)}")
                        if candles_5m:
                            print("✅ Data available in 5min interval")
                            # Show fallback data
                            candle = candles_5m[0]
                            print(f"🎯 Sample 5min Data: {candle.get('instrument_key')} - {candle.get('close', 0):.2f}")
                        else:
                            print("❌ No data in 5min interval either")
                    else:
                        print("❌ 5min API failed")

                return True
            else:
                print("❌ API returned error:", data.get('error', 'Unknown error'))
                return False
        else:
            print("❌ API returned status:", response.status_code)
            print("Response:", response.text[:500])
            return False

    except requests.exceptions.ConnectionError:
        print("❌ Connection failed - Is the Flask app running?")
        print("💡 Start the app with: python app.py")
        return False
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False

if __name__ == "__main__":
    print("="*60)
    print("🧪 TRENDVISION DASHBOARD API TEST")
    print("="*60)

    success = test_api_summary()

    print("\n" + "="*60)
    if success:
        print("✅ API TEST PASSED - Dashboard should display live data!")
    else:
        print("❌ API TEST FAILED - Check app logs and database connection")
    print("="*60)
