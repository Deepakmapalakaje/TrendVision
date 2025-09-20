import os
import threading
import sqlite3
import schedule
import time
import json
import csv
import io
import psutil
import logging
import random
from datetime import datetime, time as dt_time, timedelta
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash, Response
from werkzeug.security import generate_password_hash, check_password_hash
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from email_utils import send_otp_email
from zoneinfo import ZoneInfo

# --- Initialization ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TRADING_DB = os.getenv("TRADING_DB", "database/upstox_v3_live_trading.db")
USER_DB = os.getenv("USER_DB", "database/users.db")
IST = ZoneInfo("Asia/Kolkata")

pipeline_running = False

# --- Database & App Helpers ---
def get_db_connection(db_path):
    conn = sqlite3.connect(db_path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key-for-trendvision")

    Limiter(
        get_remote_address,
        app=app,
        default_limits=["200 per day", "50 per hour"]
    )

    @app.after_request
    def set_security_headers(response):
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'SAMEORIGIN'
        response.headers['X-XSS-Protection'] = '1; mode=block'
        if request.is_secure:
            response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
        return response

    # --- API Endpoints ---
    @app.route("/api/summary")
    def api_summary():
        try:
            with get_db_connection(TRADING_DB) as conn:
                cursor = conn.cursor()
                
                # Fetch Cash Flow
                cursor.execute("SELECT * FROM options_cash_flow ORDER BY timestamp DESC LIMIT 1")
                cash_flow_row = cursor.fetchone()
                cash_flow = dict(cash_flow_row) if cash_flow_row else {}

                # Fetch Active Signals
                cursor.execute("SELECT * FROM buy_signals ORDER BY timestamp DESC LIMIT 5")
                signals_rows = cursor.fetchall()
                active_signals = [dict(row) for row in signals_rows]

                # Fetch NIFTY and Future data from pipeline1's output (assuming it writes to a known table)
                # This part needs to be robust. For now, we assume a table `latest_instrument_data`
                # In the final pipeline1.py, we must ensure data is written here.
                nifty_data = []
                try:
                    cursor.execute("SELECT * FROM latest_instrument_data ORDER BY timestamp DESC")
                    nifty_rows = cursor.fetchall()
                    nifty_data = [dict(row) for row in nifty_rows]
                except sqlite3.OperationalError:
                    logger.warning("`latest_instrument_data` table not found. NIFTY data will be missing.")


            return jsonify({
                "ok": True,
                "cash_flow": cash_flow,
                "active_signals": active_signals,
                "nifty_data": nifty_data
            })
        except Exception as e:
            logger.error(f"API summary error: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500
            
    # All other routes from the original app.py go here
    # ... /login, /signup, /dashboard, etc. ...

    return app

# --- Main Execution ---
if __name__ == "__main__":
    app = create_app()
    # The pipeline is now started independently, not from the web app.
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)