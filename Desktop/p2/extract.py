#!/usr/bin/env python3
"""
Options Data Extractor
Extracts 60 options from NSE.csv based on selected expiry
"""
import pandas as pd
import sys
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_options_for_expiry(expiry_date):
    """Extract 60 options for specified expiry"""
    try:
        # Load NSE.csv
        df = pd.read_csv('NSE.csv')

        # Filter for selected expiry
        filtered_df = df[df['expiry'] == expiry_date].copy()

        if filtered_df.empty:
            logger.error(f"❌ No options found for expiry: {expiry_date}")
            return False

        # Get CE and PE options
        ce_options = filtered_df[filtered_df['option_type'] == 'CE']
        pe_options = filtered_df[filtered_df['option_type'] == 'PE']

        if ce_options.empty or pe_options.empty:
            logger.error(f"❌ Missing CE or PE options for expiry: {expiry_date}")
            return False

        # Get strikes and find ATM
        all_strikes = sorted(filtered_df['strike_price'].unique())
        atm_strike = all_strikes[len(all_strikes) // 2]  # Middle strike as ATM

        # Select 30 CE options around ATM
        atm_index = all_strikes.index(atm_strike)
        ce_start = max(0, atm_index - 15)
        ce_end = min(len(all_strikes), atm_index + 15)
        selected_ce_strikes = all_strikes[ce_start:ce_end]

        # Select 30 PE options around ATM
        pe_start = max(0, atm_index - 15)
        pe_end = min(len(all_strikes), atm_index + 15)
        selected_pe_strikes = all_strikes[pe_start:pe_end]

        # Get final selection
        selected_ce = ce_options[ce_options['strike_price'].isin(selected_ce_strikes)]
        selected_pe = pe_options[pe_options['strike_price'].isin(selected_pe_strikes)]

        final_selection = pd.concat([selected_ce, selected_pe]).sort_values(['option_type', 'strike_price'])

        # Prepare for trading system
        final_selection['symbol'] = final_selection['name']
        final_selection['strike'] = final_selection['strike_price']
        final_selection['last_price'] = 100.0  # Default, updated by live data

        # Save to extracted_data.csv
        final_selection.to_csv('extracted_data.csv', index=False)

        ce_count = len(selected_ce)
        pe_count = len(selected_pe)

        logger.info(f"✅ Extracted {len(final_selection)} options for {expiry_date}")
        logger.info(f"📊 Selection: {ce_count} CE + {pe_count} PE around ATM {atm_strike}")
        logger.info(f"💾 Saved to extracted_data.csv")

        return True

    except Exception as e:
        logger.error(f"❌ Error extracting options: {e}")
        return False

def get_available_expiries():
    """Get list of available expiry dates"""
    try:
        df = pd.read_csv('NSE.csv')
        expiries = sorted(df['expiry'].unique())
        return expiries
    except Exception as e:
        logger.error(f"❌ Error reading expiries: {e}")
        return []

if __name__ == "__main__":
    if len(sys.argv) > 1:
        expiry_date = sys.argv[1]
        extract_options_for_expiry(expiry_date)
    else:
        expiries = get_available_expiries()
        if expiries:
            latest_expiry = expiries[-1]
            logger.info(f"🎯 Using latest expiry: {latest_expiry}")
            extract_options_for_expiry(latest_expiry)
        else:
            logger.error("❌ No expiries found")
