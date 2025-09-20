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
        default_limits=["500 per day", "50 per hour", "5 per minute"]
    )

    @app.after_request
    def set_security_headers(response):
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['X-XSS-Protection'] = '1; mode=block'
        response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com;"
        return response

    # --- API Endpoints ---

    @app.route("/api/summary")
    def api_summary():
        """Enhanced API with cash flow, ITM options, and trend data"""
        try:
            with get_db_connection(TRADING_DB) as conn:
                cursor = conn.cursor()

                # Fetch Latest Cash Flow
                cursor.execute("SELECT * FROM options_cash_flow ORDER BY timestamp DESC LIMIT 1")
                cash_flow_row = cursor.fetchone()
                cash_flow = {
                    'cash': 0.0,
                    'min_cash': 0.0,
                    'max_cash': 0.0,
                    'timestamp': None
                }
                if cash_flow_row:
                    cash_flow = dict(cash_flow_row)

                # Fetch Latest NIFTY Index Data
                cursor.execute("SELECT * FROM latest_candles WHERE instrument_key = 'NSE_INDEX|Nifty 50' ORDER BY last_updated DESC LIMIT 1")
                nifty_row = cursor.fetchone()
                nifty_data = {}
                if nifty_row:
                    nifty_data = dict(nifty_row)

                # Fetch Latest NIFTY Future Data
                cursor.execute("SELECT * FROM latest_candles WHERE instrument_type = 'FUTURE' ORDER BY last_updated DESC LIMIT 1")
                future_row = cursor.fetchone()
                future_data = {}
                if future_row:
                    future_data = dict(future_row)

                # Get ITM Options (highest CE below NIFTY, lowest PE above NIFTY)
                itm_options = {'itm_ce': None, 'itm_pe': None}
                if nifty_data and 'close' in nifty_data:
                    nifty_price = nifty_data['close']
                    try:
                        # Find 1st ITM CE (highest strike below NIFTY)
                        cursor.execute("""
                            SELECT * FROM latest_candles
                            WHERE option_type = 'CE' AND strike_price < ? AND instrument_type = 'OPTION'
                            ORDER BY strike_price DESC, last_updated DESC LIMIT 1
                        """, (nifty_price,))
                        ce_row = cursor.fetchone()
                        if ce_row:
                            itm_options['itm_ce'] = dict(ce_row)

                        # Find 1st ITM PE (lowest strike above NIFTY)
                        cursor.execute("""
                            SELECT * FROM latest_candles
                            WHERE option_type = 'PE' AND strike_price > ? AND instrument_type = 'OPTION'
                            ORDER BY strike_price ASC, last_updated DESC LIMIT 1
                        """, (nifty_price,))
                        pe_row = cursor.fetchone()
                        if pe_row:
                            itm_options['itm_pe'] = dict(pe_row)
                    except Exception as e:
                        logger.error(f"Error fetching ITM options: {e}")

                # Fetch Latest Trend
                cursor.execute("SELECT * FROM trend WHERE buy_recommendation IS NOT NULL ORDER BY timestamp DESC LIMIT 1")
                trend_row = cursor.fetchone()
                trend_data = {}
                if trend_row:
                    trend_data = dict(trend_row)

                # Fetch Recent Buy Signals
                cursor.execute("SELECT * FROM trend WHERE buy_recommendation IS NOT NULL ORDER BY timestamp DESC LIMIT 5")
                signals_rows = cursor.fetchall()
                active_signals = [dict(row) for row in signals_rows]

                return jsonify({
                    "ok": True,
                    "cash_flow": cash_flow,
                    "nifty_data": nifty_data,
                    "future_data": future_data,
                    "itm_options": itm_options,
                    "latest_trend": trend_data,
                    "active_signals": active_signals,
                    "timestamp": datetime.now(IST).isoformat()
                })

        except Exception as e:
            logger.error(f"API summary error: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/cash_flow_history")
    def api_cash_flow_history():
        """Get cash flow history for charts"""
        try:
            with get_db_connection(TRADING_DB) as conn:
                cursor = conn.cursor()

                # Get last 100 cash flow entries
                cursor.execute("SELECT * FROM options_cash_flow ORDER BY timestamp DESC LIMIT 100")
                rows = cursor.fetchall()

                data = []
                for row in rows:
                    data.append({
                        'timestamp': row['timestamp'],
                        'cash': row['cash'],
                        'min_cash': row['min_cash'],
                        'max_cash': row['max_cash']
                    })

                return jsonify({"ok": True, "data": data})

        except Exception as e:
            logger.error(f"API cash flow history error: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/option_performance")
    def api_option_performance():
        """Get option performance data"""
        try:
            with get_db_connection(TRADING_DB) as conn:
                cursor = conn.cursor()

                # Get latest data for all options
                cursor.execute("""
                    SELECT instrument_name, strike_price, option_type, close, vwap, volume, delta, buy_volume, sell_volume
                    FROM latest_candles
                    WHERE instrument_type = 'OPTION'
                    ORDER BY volume DESC
                    LIMIT 60
                """)

                options_data = []
                for row in cursor.fetchall():
                    options_data.append(dict(row))

                return jsonify({"ok": True, "options": options_data})

        except Exception as e:
            logger.error(f"API option performance error: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    # --- Routes ---

    @app.route("/")
    def home():
        return render_template("login.html")

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = request.form.get("username")
            password = request.form.get("password")

            # Simple auth for now (replace with proper auth)
            if username == "admin" and password == "password":
                session["user"] = username
                return redirect(url_for("dashboard"))
            else:
                flash("Invalid credentials", "error")

        return render_template("login.html")

    @app.route("/dashboard")
    def dashboard():
        # Remove authentication for direct trading access
        # if "user" not in session:
        #     return redirect(url_for("login"))
        return render_template("dashboard.html")

    @app.route("/")
    def home():
        return render_template("dashboard.html")  # Direct to dashboard for trading

    @app.route("/logout")
    def logout():
        session.pop("user", None)
        return redirect(url_for("login"))

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"ok": False, "error": "Endpoint not found"}), 404

    @app.errorhandler(500)
    def internal_error(e):
        logger.error(f"Internal server error: {e}")
        return jsonify({"ok": False, "error": "Internal server error"}), 500

    return app

# --- Main Execution ---
if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False, threaded=True)
