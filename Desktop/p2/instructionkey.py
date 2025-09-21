#!/usr/bin/env python3
"""
Daily Instruction Key Fetcher for NIFTY Options
Fetches latest instrument keys and saves to NSE.csv
"""
import requests
import pandas as pd
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_nifty_instruments():
    """Fetch NIFTY instruments from Upstox API"""
    try:
        # Load access token
        with open('config/config.json', 'r') as f:
            config = json.load(f)
            access_token = config.get('ACCESS_TOKEN', '')

        if not access_token or access_token == "update-daily-in-admin-panel":
            logger.error("❌ Access token not found or not updated")
            return False

        # Fetch instruments
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        # Get all NSE instruments
        url = "https://api.upstox.com/v2/option/contract"
        params = {'instrument_key': 'NSE_INDEX|Nifty 50'}

        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()
            instruments = data.get('data', [])

            # Filter for NIFTY options only
            nifty_instruments = []
            for instrument in instruments:
                name = instrument.get('name', '')
                if 'NIFTY' in name.upper():
                    nifty_instruments.append({
                        'instrument_key': instrument.get('instrument_key', ''),
                        'name': name,
                        'option_type': instrument.get('option_type', ''),
                        'strike_price': instrument.get('strike_price', 0),
                        'expiry': instrument.get('expiry', ''),
                        'lot_size': instrument.get('lot_size', 50),
                        'segment': 'NSE_FO'
                    })

            # Save to NSE.csv
            df = pd.DataFrame(nifty_instruments)
            df.to_csv('NSE.csv', index=False)

            logger.info(f"✅ Fetched {len(nifty_instruments)} NIFTY instruments")
            logger.info(f"💾 Saved to NSE.csv")

            return True

        else:
            logger.error(f"❌ API Error: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"❌ Error fetching instruments: {e}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting daily instrument key fetch...")
    success = fetch_nifty_instruments()
    if success:
        logger.info("✅ Daily instrument fetch completed")
    else:
        logger.error("❌ Daily instrument fetch failed")
