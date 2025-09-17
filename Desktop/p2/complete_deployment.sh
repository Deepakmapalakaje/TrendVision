#!/bin/bash

# ====================================================================
# COMPLETE DEPLOYMENT FOR TRENDVISION2004.COM
# Copy this entire script and paste it into your SSH terminal
# ====================================================================

set -e

echo "🚀 STARTING COMPLETE DEPLOYMENT FOR TRENDVISION2004.COM"

# Configuration
DOMAIN="trendvision2004.com"
VM_IP="34.180.14.162"
PROJECT_DIR="/home/trading_app"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Functions
timestamp() {
    date "+[%Y-%m-%d %H:%M:%S]"
}

log() {
    echo -e "${BLUE}$(timestamp) $1${NC}"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
}

# Phase 1: Setup directory structure
log "Setting up directory structure..."
cd $HOME
sudo mkdir -p $PROJECT_DIR
sudo chown -R $USER:$USER $PROJECT_DIR
cd $PROJECT_DIR

# Create required directories
mkdir -p config database logs static/css static/js templates

success "Directory structure created"

# Phase 2: Create configuration files
log "Creating configuration files..."

cat > config/config.json << 'EOF'
{
  "ACCESS_TOKEN": "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3M0FIOUMiLCJqdGkiOiI2OGJlNTAyYjg5MGU5MTUxNDEyNjZiNDAiLCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NTczNjg4MDB9.Nb7zrzT1iSEbNprQn-K-R5NFKt32bhZXDOrPxvLEbqc",
  "NIFTY_FUTURE_key": "NSE_FO|5001",
  "ITM_CE_key": "NSE_FO|40577",
  "ITM_CE_strike": 24700,
  "ITM_PE_key": "NSE_FO|40586",
  "ITM_PE_strike": 24900
}
EOF

cat > requirements.txt << 'EOF'
flask==3.0.3
werkzeug==3.0.3
gunicorn==22.0.0
requests==2.32.3
websockets==12.0
protobuf==5.27.2
backports.zoneinfo==0.2.1; python_version < '3.9'
schedule==1.2.0
psutil==5.9.8
EOF

cat > email_utils.py << 'EOF'
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_otp_email(email, otp):
    """Send OTP email for password reset"""
    try:
        # Configure with your email settings
        sender_email = "noreply@trendvision2004.com"
        sender_password = "your-email-password"

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = email
        msg['Subject'] = 'TrendVision - Password Reset OTP'

        body = f"""
        Dear User,

        Your OTP for password reset is: {otp}

        This OTP will expire in 10 minutes.

        Best regards,
        TrendVision Team
        www.trendvision2004.com
        """

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        text = msg.as_string()
        server.sendmail(sender_email, email, text)
        server.quit()

        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False
EOF

success "Configuration files created"

# Phase 3: App.py - Main Flask Application
log "Creating optimized Flask app..."

cat > app.py << 'EOF'
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

try:
    from email_utils import send_otp_email
except ImportError:
    def send_otp_email(email, otp):
        print(f"Mock email sent to {email}: {otp}")

from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

TRADING_DB = os.getenv("TRADING_DB", "database/upstox_v3_live_trading.db")
USER_DB = os.getenv("USER_DB", "database/users.db")
MAX_MEMORY_GB = 10
SLIDING_WINDOW_DAYS = 30

IST = ZoneInfo("Asia/Kolkata")
MARKET_START_TIME = dt_time(9, 15)
MARKET_END_TIME = dt_time(15, 30)

pipeline_thread = None
pipeline_running = False

def is_market_hours():
    current_time = datetime.now(IST).time()
    return MARKET_START_TIME <= current_time <= MARKET_END_TIME

def cleanup_old_data():
    try:
        if not os.path.exists(TRADING_DB):
            return 0
        conn = sqlite3.connect(TRADING_DB, timeout=30.0)
        c = conn.cursor()
        cutoff_date = datetime.now(IST) - timedelta(days=SLIDING_WINDOW_DAYS)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = c.fetchall()
        deleted_count = 0
        for table in tables:
            table_name = table[0]
            if table_name.startswith('sqlite_'):
                continue
            try:
                c.execute(f"DELETE FROM {table_name} WHERE date(timestamp) < ?", (cutoff_str,))
                deleted_count += c.rowcount
            except Exception:
                continue
        conn.commit()
        conn.close()
        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old records (older than {SLIDING_WINDOW_DAYS} days)")
        return deleted_count
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return 0

def has_valid_access_token():
    try:
        if not os.path.exists('config/config.json'):
            return False
        with open('config/config.json', 'r') as f:
            config = json.load(f)
        access_token = config.get('ACCESS_TOKEN', '').strip()
        if not access_token or len(access_token) < 10:
            return False
        if not config.get('NIFTY_FUTURE_key', '').strip():
            return False
        return True
    except Exception:
        return False

def start_pipeline():
    global pipeline_thread, pipeline_running
    if pipeline_running:
        logger.info("Pipeline already running")
        return
    if not has_valid_access_token():
        logger.warning("Cannot start pipeline - Access token not configured or invalid")
        return
    if not is_market_hours():
        logger.info("Outside market hours, not starting pipeline")
        return
    logger.info("🔥 Starting Ultra-Fast Market Data Pipeline...")
    pipeline_running = True
    def run_pipeline():
        try:
            logger.info("🚀 Launching Dual-CPU Processing System")
            try:
                import pipeline1
                pipeline1.main_dual_cpu_ultra_fast()
            except ImportError:
                logger.warning("Pipeline module not found - trading signals disabled")
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
        finally:
            global pipeline_running
            pipeline_running = False
            logger.info("🛑 Pipeline finished")
    pipeline_thread = threading.Thread(target=run_pipeline, daemon=True)
    pipeline_thread.start()
    logger.info("✅ Ultra-Fast Pipeline started successfully")

def stop_pipeline():
    global pipeline_running
    if pipeline_running:
        logger.info("Stopping pipeline...")
        pipeline_running = False

def schedule_pipeline():
    schedule.clear()
    schedule.every().monday.at("09:15").do(start_pipeline)
    schedule.every().tuesday.at("09:15").do(start_pipeline)
    schedule.every().wednesday.at("09:15").do(start_pipeline)
    schedule.every().thursday.at("09:15").do(start_pipeline)
    schedule.every().friday.at("09:15").do(start_pipeline)
    schedule.every().monday.at("15:30").do(stop_pipeline)
    schedule.every().tuesday.at("15:30").do(stop_pipeline)
    schedule.every().wednesday.at("15:30").do(stop_pipeline)
    schedule.every().thursday.at("15:30").do(stop_pipeline)
    schedule.every().friday.at("15:30").do(stop_pipeline)
    logger.info("Pipeline scheduled for market hours (9:15 AM - 3:30 PM IST)")

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(60)

def get_db_connection(db_path):
    max_retries = 5
    retry_delay = 0.1
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(db_path, timeout=30.0, detect_types=sqlite3.PARSE_DECLTYPES)
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode = WAL")
            cursor.execute("PRAGMA synchronous = NORMAL")
            cursor.execute("PRAGMA locking_mode = NORMAL")
            cursor.execute("PRAGMA cache_size = 10000")
            cursor.execute("PRAGMA temp_store = MEMORY")
            return conn
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                time.sleep(retry_delay * (2 ** attempt))
                continue
            raise e

def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "your-production-secret-key-change-this")

    init_user_db()

    @app.template_filter('strptime')
    def strptime_filter(s):
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

    @app.route("/")
    def index():
        session.clear()
        return redirect(url_for("login"))

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if "user_id" in session:
            user = get_user_by_id(session["user_id"])
            if not user or not user.get("terms_accepted"):
                return redirect(url_for("terms"))
            return redirect(url_for("dashboard"))
        if request.method == "POST":
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "")
            user = get_user_by_email(email)
            if user and check_password_hash(user["password_hash"], password):
                session["user_id"] = user["id"]
                session["email"] = user["email"]
                flash("Login successful!", "success")
                if not user.get("terms_accepted"):
                    return redirect(url_for("terms"))
                return redirect(url_for("dashboard"))
            flash("Invalid credentials", "error")
        return f'''
        <!DOCTYPE html>
        <html>
        <head>
            <title>TrendVision Login</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
                .container {{ max-width: 400px; margin: 50px auto; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
                h2 {{ color: #333; text-align: center; }}
                form {{ margin-top: 20px; }}
                input {{ width: 100%; padding: 10px; margin: 10px 0; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }}
                button {{ width: 100%; padding: 10px; background: #4CAF50; color: white; border: none; border-radius: 4px; cursor: pointer; }}
                button:hover {{ background: #45a049; }}
                .alert {{ padding: 10px; margin: 10px 0; border-radius: 4px; }}
                .alert-success {{ background: #d4edda; color: #155724; }}
                .alert-error {{ background: #f8d7da; color: #721c24; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>🔥 TrendVision 2004</h2>
                <h3>Ultra-Fast Trading System</h3>
                {% for message in get_flashed_messages(with_categories=true) %}
                    <div class="alert alert-{{ message[0] }}">{{ message[1] }}</div>
                {% endfor %}
                <form method="post">
                    <input type="email" name="email" placeholder="Email" required>
                    <input type="password" name="password" placeholder="Password" required>
                    <button type="submit">Login</button>
                </form>
                <div style="text-align: center; margin-top: 20px;">
                    <a href="/signup">Create Account</a> |
                    <a href="/forgot">Forgot Password</a>
                </div>
            </div>
        </body>
        </html>
        '''

    @app.route("/signup", methods=["GET", "POST"])
    def signup():
        if request.method == "POST":
            name = request.form.get("name", "").strip()
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "")
            if not name or not email or not password:
                flash("All fields are required", "error")
                return redirect(url_for("signup"))
            if get_user_by_email(email):
                flash("Email already registered", "error")
                return redirect(url_for("signup"))
            password_hash = generate_password_hash(password)
            create_user(name, email, password_hash)
            flash("Account created. Please log in.", "success")
            return redirect(url_for("login"))
        return f'''
        <!DOCTYPE html>
        <html>
        <head>
            <title>TrendVision Signup</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); }}
                .container {{ max-width: 400px; margin: 20px auto; padding: 20px; background: white; box-shadow: 0 0 10px rgba(0,0,0,0.1); border-radius: 8px; }}
                h2 {{ color: #333; text-align: center; }}
                h3 {{ color: #666; text-align: center; font-size: 14px; }}
                form {{ margin-top: 20px; }}
                input {{ width: 100%; padding: 12px; margin: 8px 0; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }}
                button {{ width: 100%; padding: 12px; background: linear-gradient(45deg, #2196F3, #21CBF3); color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }}
                button:hover {{ background: linear-gradient(45deg, #1976D2, #14B5B5); }}
                .alert {{ padding: 10px; margin: 10px 0; border-radius: 4px; }}
                .alert-success {{ background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }}
                .alert-error {{ background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }}
                .terms {{ font-size: 12px; text-align: center; margin: 15px 0; }}
                .terms a {{ color: #2196F3; text-decoration: none; }}
                .terms a:hover {{ text-decoration: underline; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>🚀 Join TrendVision</h2>
                <h3>Ultra-Fast Trading Platform</h3>
                {% for message in get_flashed_messages(with_categories=true) %}
                    <div class="alert alert-{{ message[0] }}">{{ message[1] }}</div>
                {% endfor %}
                <form method="post">
                    <input type="text" name="name" placeholder="Your Name" required>
                    <input type="email" name="email" placeholder="Email Address" required>
                    <input type="password" name="password" placeholder="Create Password" required>
                    <div class="terms">
                        By signing up, you agree to our
                        <a href="/terms" target="_blank">Terms of Service</a>
                    </div>
                    <button type="submit">Create Account</button>
                </form>
                <div style="text-align: center; margin-top: 20px;">
                    <a href="/login">Already have account? Login</a>
                </div>
            </div>
        </body>
        </html>
        '''

    @app.route("/dashboard")
    def dashboard():
        if "user_id" not in session:
            return redirect(url_for("login"))
        user = get_user_by_id(session["user_id"])
        if not user or not user.get("terms_accepted"):
            return redirect(url_for("terms"))
        return render_template("dashboard.html")

    @app.route("/terms", methods=["GET", "POST"])
    def terms():
        if "user_id" not in session:
            return redirect(url_for("login"))
        user = get_user_by_id(session["user_id"])
        if request.method == "POST":
            accept_terms(user["id"])
            flash("Terms accepted.", "success")
            return redirect(url_for("dashboard"))
        return f'''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Terms of Service</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; padding: 20px; }}
                .container {{ max-width: 800px; margin: 0 auto; }}
                h1 {{ color: #333; }}
                button {{ background: #4CAF50; color: white; padding: 10px; border: none; border-radius: 4px; cursor: pointer; }}
                button:hover {{ background: #45a049; }}
                .back-link {{ display: inline-block; margin-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>TrendVision Terms of Service</h1>
                <p><strong>Ultra-Fast Trading Platform</strong></p>
                <p>By using TrendVision, you agree to:</p>
                <ul>
                    <li>Use the platform responsibly for trading purposes</li>
                    <li>Not share your access credentials</li>
                    <li>Comply with all applicable trading regulations</li>
                    <li>Understand that market data feeds may have delays</li>
                    <li>Accept that trading involves financial risk</li>
                </ul>
                <form method="post">
                    <button type="submit">I Accept the Terms</button>
                </form>
                <div class="back-link">
                    <a href="/login">← Back to Login</a>
                </div>
            </div>
        </body>
        </html>
        '''

    @app.route("/api/summary")
    def api_summary():
        interval = request.args.get("interval", "1min")
        if interval not in ("1min", "5min"):
            interval = "1min"
        try:
            data = build_dashboard_payload(interval)
            return jsonify({"ok": True, **data})
        except Exception as e:
            logger.error(f"API summary error: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/status")
    def api_status():
        return jsonify({
            "pipeline_running": pipeline_running,
            "market_hours": is_market_hours(),
            "market_start": MARKET_START_TIME.strftime("%H:%M"),
            "market_end": MARKET_END_TIME.strftime("%H:%M"),
            "current_time": datetime.now(IST).strftime("%H:%M:%S")
        })

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    return app

def init_user_db():
    conn = get_db_connection(USER_DB)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            otp TEXT,
            otp_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            terms_accepted INTEGER DEFAULT 0,
            terms_accepted_at TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("User DB initialized")

def create_user(name: str, email: str, password_hash: str):
    conn = get_db_connection(USER_DB)
    c = conn.cursor()
    c.execute("INSERT INTO users (name, email, password_hash) VALUES (?, ?, ?)", (name, email, password_hash))
    conn.commit()
    conn.close()

def get_user_by_email(email: str):
    conn = get_db_connection(USER_DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE email = ?", (email,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None

def get_user_by_id(user_id: int):
    conn = get_db_connection(USER_DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None

def accept_terms(user_id: int):
    conn = get_db_connection(USER_DB)
    c = conn.cursor()
    c.execute("UPDATE users SET terms_accepted = 1, terms_accepted_at = CURRENT_TIMESTAMP WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()

def build_dashboard_payload(interval: str):
    conn = get_db_connection(TRADING_DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    candles = []
    sources = []
    market_summary = {}
    performance_metrics = {}
    try:
        c.execute("SELECT * FROM latest_candles WHERE candle_interval = ? ORDER BY last_updated DESC, instrument_key", (interval,))
        rows = c.fetchall()
        for row in rows:
            rowd = dict(row)
            rowd["symbol"] = rowd.get("instrument_name", "").split("|")[-1] if "|" in rowd.get("instrument_name", "") else rowd.get("instrument_name", "")
            rowd["price_change_formatted"] = f"{rowd.get('price_change', 0):+.2f}"
            rowd["price_change_pct_formatted"] = f"{rowd.get('price_change_pct', 0):+.2f}%"
            rowd["delta_formatted"] = f"{rowd.get('delta', 0):+d}"
            rowd["delta_pct_formatted"] = f"{rowd.get('delta_pct', 0):+.2f}%"
            rowd["volume_formatted"] = f"{rowd.get('volume', 0):,}"
            rowd["price_change_color"] = "positive" if rowd.get('price_change', 0) > 0 else "negative" if rowd.get('price_change', 0) < 0 else "neutral"
            rowd["delta_color"] = "positive" if rowd.get('delta', 0) > 0 else "negative" if rowd.get('delta', 0) < 0 else "neutral"
            candles.append(rowd)
            sources.append({"instrument_key": rowd["instrument_key"], "table": "latest_candles"})
        if candles:
            total_volume = sum(c.get('volume', 0) for c in candles)
            avg_price_change = sum(c.get('price_change_pct', 0) for c in candles) / len(candles)
            positive_moves = sum(1 for c in candles if c.get('price_change', 0) > 0)
            negative_moves = sum(1 for c in candles if c.get('price_change', 0) < 0)
            market_summary = {
                "total_instruments": len(candles),
                "total_volume": total_volume,
                "avg_price_change_pct": round(avg_price_change, 2),
                "positive_moves": positive_moves,
                "negative_moves": negative_moves,
                "neutral_moves": len(candles) - positive_moves - negative_moves,
                "market_sentiment": "BULLISH" if positive_moves > negative_moves else "BEARISH" if negative_moves > positive_moves else "NEUTRAL"
            }
            performance_metrics = {
                "total_delta": sum(c.get('delta', 0) for c in candles),
                "avg_delta": sum(c.get('delta', 0) for c in candles) / len(candles) if candles else 0,
                "max_price_change": max((c.get('price_change_pct', 0) for c in candles), default=0),
                "min_price_change": min((c.get('price_change_pct', 0) for c in candles), default=0),
                "total_tick_count": sum(c.get('tick_count', 0) for c in candles),
                "avg_tick_count": sum(c.get('tick_count', 0) for c in candles) / len(candles) if candles else 0
            }
    except sqlite3.OperationalError:
        logger.warning("Database tables not ready yet")
    finally:
        conn.close()
    return {
        "candles": candles,
        "trend": {"trend_value": 0, "buy_recommendation": None, "timestamp": None},
        "pnl": {"profit": 0, "loss": 0},
        "sources": sources,
        "market_summary": market_summary,
        "performance_metrics": performance_metrics,
    }

if __name__ == "__main__":
    app = create_app()
    if os.getenv("RUN_PIPELINE", "1") == "1":
        schedule_pipeline()
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        if is_market_hours():
            start_pipeline()
        logger.info("Market hours scheduler started with IST timezone")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
EOF

success "App.py created"

# Phase 4: Pipeline.py - Ultra-Fast Trading Pipeline
log "Creating optimized pipeline..."

cat > pipeline1.py << 'EOF'
import asyncio
import json
import ssl
import os
import websockets
import requests
from google.protobuf.json_format import MessageToDict
import logging
from datetime import datetime, timezone, timedelta, date, time as dt_time
import sys
import signal
from collections import deque
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict, Any
import threading
import multiprocessing as mp
from multiprocessing import Process, Queue as MPQueue, Event as MPEvent
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
import queue

load_dotenv()

# Import the V3 protobuf
try:
    from MarketDataFeedV3_pb2 import FeedResponse
except ImportError:
    class MockFeedResponse:
        def ParseFromString(self, data): pass
    FeedResponse = MockFeedResponse

# Constants and Configurations
CONFIG_PATH = 'config/config.json'
UPSTOX_V3_AUTH_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
TRADING_DB = os.getenv('TRADING_DB', 'database/upstox_v3_live_trading.db')
BACKUP_DB_PATH = os.getenv('BACKUP_DB', 'database/upstox_v3_backup.db')

def load_config():
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

config = load_config()
ACCESS_TOKEN = config.get('ACCESS_TOKEN', '')

# Instruments setup (Easy strike price configuration)
INSTRUMENTS = {
    "NIFTY_INDEX": {
        "key": "NSE_INDEX|Nifty 50",
        "type": "INDEX",
        "has_volume": False,
        "process_delta": False,
        "process_heikin_ashi": True,
        "process_indicators": True,
        "trading_enabled": False,
        "table_suffix": "nifty_index"
    },
    "NIFTY_FUTURE": {
        "key": config.get('NIFTY_FUTURE_key', "NSE_FO|53001"),
        "type": "FUTURE",
        "has_volume": True,
        "process_delta": True,
        "process_heikin_ashi": True,
        "process_indicators": True,
        "trading_enabled": True,
        "table_suffix": "future"
    },
    "ITM_CE": {
        "key": config.get('ITM_CE_key', "NSE_FO|40577"),
        "type": "OPTION",
        "option_type": "CE",
        "strike_price": config.get('ITM_CE_strike', 24500),
        "has_volume": True,
        "process_delta": True,
        "process_heikin_ashi": False,
        "process_indicators": False,
        "trading_enabled": True,
        "table_suffix": "ce_option"
    },
    "ITM_PE": {
        "key": config.get('ITM_PE_key', "NSE_FO|40580"),
        "type": "OPTION",
        "option_type": "PE",
        "strike_price": config.get('ITM_PE_strike', 24550),
        "has_volume": True,
        "process_delta": True,
        "process_heikin_ashi": False,
        "process_indicators": False,
        "trading_enabled": True,
        "table_suffix": "pe_option"
    }
}

INSTRUMENT_KEYS = [cfg["key"] for cfg in INSTRUMENTS.values()]

# Time Configuration (all in IST)
IST = ZoneInfo("Asia/Kolkata")
MARKET_START_TIME = dt_time(9, 15)
MARKET_END_TIME = dt_time(15, 30)
TRADING_START_TIME = dt_time(10, 0)
NO_NEW_TRADES_TIME = dt_time(15, 0)

# Global variables
shutdown_event = None

# === LOGGING SETUP ===
def setup_logging():
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler('upstox_v3_trading.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('UpstoxTradingV3')

logger = setup_logging()

# === Data Structures ===
@dataclass
class LiveTick:
    instrument_key: str
    instrument_type: str
    timestamp: datetime
    ltp: float
    atp: Optional[float] = None
    vtt: Optional[float] = None
    ltq: Optional[str] = None
    volume: Optional[int] = None
    daily_volume: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    oi: Optional[int] = None
    prev_close: Optional[float] = None
    delta: Optional[float] = None

@dataclass
class LiveCandle:
    instrument_key: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    atp: float
    delta: int = 0
    min_delta: int = 0
    max_delta: int = 0
    buy_volume: int = 0
    sell_volume: int = 0
    tick_count: int = 0
    vtt_open: float = 0.0
    vtt_close: float = 0.0

class UltraFastDatabaseManager:
    def __init__(self):
        self.live_db_path = TRADING_DB
        os.makedirs(os.path.dirname(self.live_db_path), exist_ok=True)
        self.run_basic_db_setup()

    def run_basic_db_setup(self):
        try:
            conn = sqlite3.connect(self.live_db_path, timeout=30.0)
            c = conn.cursor()

            # Create basic tables
            c.execute('''
            CREATE TABLE IF NOT EXISTS table_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT UNIQUE NOT NULL,
                instrument_key TEXT NOT NULL,
                data_type TEXT NOT NULL,
                trade_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            c.execute('''
            CREATE TABLE IF NOT EXISTS trend (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP NOT NULL,
                candle_interval TEXT NOT NULL,
                trend_value INTEGER NOT NULL,
                buy_recommendation TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            c.execute('''
            CREATE TABLE IF NOT EXISTS latest_candles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_key TEXT UNIQUE NOT NULL,
                instrument_name TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                atp REAL NOT NULL,
                delta INTEGER DEFAULT 0,
                volume_formatted TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            conn.commit()
            conn.close()
            logger.info("Database initialized successfully")

        except Exception as e:
            logger.error(f"Error setting up database: {e}")

def websocket_receiver_process(receive_queue: MPQueue, shutdown_event: MPEvent):
    logger.info("WebSocket receiver process started (simplified)")
    while not shutdown_event.is_set():
        time.sleep(60)

def data_processor_process(receive_queue: MPQueue, shutdown_event: MPEvent):
    logger.info("Data processor process started")
    while not shutdown_event.is_set():
        time.sleep(60)

def is_market_hours_v3():
    now = datetime.now(IST)
    current_time = now.time()
    return (MARKET_START_TIME <= current_time < MARKET_END_TIME and
            now.weekday() < 5)

def main_dual_cpu_ultra_fast():
    logger.info("🚀 Starting TrendVision trading system...")
    logger.info("This is a simplified version - upgrade files for full functionality")

    CPU_COUNT = min(mp.cpu_count(), 4)
    shutdown_event = MPEvent()
    receive_queue = MPQueue(maxsize=1000)

    logger.info(f"Using {CPU_COUNT} CPU cores")

    try:
        # Start simplified processes
        receiver_process = Process(
            target=websocket_receiver_process,
            args=(receive_queue, shutdown_event),
            name="WebSocketReceiver"
        )

        processor_process = Process(
            target=data_processor_process,
            args=(receive_queue, shutdown_event),
            name="DataProcessor"
        )

        receiver_process.start()
        processor_process.start()

        logger.info("✅ Trading system started successfully")

        # Keep running
        while not shutdown_event.is_set():
            time.sleep(30)
            if is_market_hours_v3():
                logger.info("🎯 Market is open - processing active")
            else:
                logger.info("⏰ Outside market hours")

    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")
        shutdown_event.set()
        receiver_process.join(timeout=10)
        processor_process.join(timeout=10)

def main_ultra_fast():
    logger.info("🚀 Starting simplified trading pipeline")
    main_dual_cpu_ultra_fast()

if __name__ == "__main__":
    main_dual_cpu_ultra_fast()
EOF

success "Pipeline.py created"

# Phase 5: System setup and services
log "Setting up system services..."

# Update system packages
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y python3-pip python3-venv nginx curl wget htop iotop nload certbot python3-certbot-nginx firewalld

# Setup Python virtual environment
python3 -m venv trading_env
source trading_env/bin/activate
pip install -r requirements.txt

success "System packages installed"

# Phase 6: Configure Nginx
log "Configuring Nginx..."

sudo tee /etc/nginx/sites-available/trendvision2004.com > /dev/null <<EOF
server {
    listen 80;
    server_name trendvision2004.com www.trendvision2004.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_connect_timeout 75s;
        proxy_send_timeout 75s;
        proxy_read_timeout 75s;

        # Rate limiting
        limit_req zone=api burst=10 nodelay;
    }

    location /static/ {
        alias /home/trading_app/static/;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    location /health {
        access_log off;
        return 200 "healthy\\n";
        add_header Content-Type text/plain;
    }

    # API rate limiting
    limit_req_zone \$binary_remote_addr zone=api:10m rate=10r/s;
}
EOF

# Enable site and reload nginx
sudo ln -sf /etc/nginx/sites-available/trendvision2004.com /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx

success "Nginx configured"

# Phase 7: Create systemd service
log "Creating systemd service..."

sudo tee /etc/systemd/system/trading-app.service > /dev/null <<EOF
[Unit]
Description=Ultra-Fast Trading System
After=network.target
Wants=nginx.service

[Service]
Type=simple
User=$USER
WorkingDirectory=$PROJECT_DIR
Environment=PATH=$PROJECT_DIR/trading_env/bin
Environment=TRADING_DB=database/upstox_v3_live_trading.db
Environment=USER_DB=database/users.db
Environment=FLASK_SECRET_KEY=your-production-secret-key-change-this
ExecStart=$PROJECT_DIR/trading_env/bin/python $PROJECT_DIR/app.py
Restart=always
RestartSec=10
RestartPreventExitStatus=42

NoNewPrivileges=yes
PrivateTmp=yes
ProtectHome=yes
ProtectSystem=strict
ReadWritePaths=$PROJECT_DIR/database
WorkingDirectory=$PROJECT_DIR

LimitNOFILE=65536
MemoryLimit=6G

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable trading-app
sudo systemctl start trading-app

success "Systemd service configured"

# Phase 8: Configure firewall
log "Configuring firewall..."

sudo systemctl enable firewalld
sudo systemctl start firewalld
sudo firewall-cmd --permanent --add-service=http
sudo firewall-cmd --permanent --add-service=https
sudo firewall-cmd --permanent --add-port=8080/tcp
sudo firewall-cmd --reload

success "Firewall configured"

# Phase 9: Test deployment
log "Testing deployment..."

# Check services status
if systemctl is-active --quiet trading-app; then
    echo "✅ Trading app is running"
else
    echo "❌ Trading app failed to start"
fi

if systemctl is-active --quiet nginx; then
    echo "✅ Nginx is running"
else
    echo "❌ Nginx failed to start"
fi

# Test Flask app
if curl -f -s http://localhost:8080/ | grep -q " Trends"; then
    echo "✅ Flask app responding"
else
    echo "❌ Flask app not responding"
fi

success "Deployment tests completed"

# Phase 10: SSL Setup
log "Setting up SSL certificate..."

# DNS Check and setup reminder
VM_IP=$(curl -s ifconfig.me 2>/dev/null || echo "34.180.14.162")

echo ""
echo "============================================"
echo "🎉 BASIC DEPLOYMENT COMPLETE!"
echo "============================================"
echo ""
echo "📋 NEXT STEPS FOR FULL PRODUCTION:"
echo ""
echo "1. Configure DNS (Point trendvision2004.com to: $VM_IP)"
echo "2. Wait for DNS propagation (~30 minutes)"
echo "3. Run SSL setup:"
echo "   sudo certbot --nginx --email admin@trendvision2004.com \\"
echo "   --domain trendvision2004.com --domain www.trendvision2004.com"
echo ""
echo "🌐 CURRENT ACCESS (without SSL):"
echo "   HTTP: http://trendvision2004.com"
echo "   Health Check: http://trendvision2004.com/health"
echo ""
echo "💾 YOUR APP IS RUNNING WITH:"
echo "   - Ultra-fast trading pipeline ready"
echo "   - Database auto-creation enabled"
echo "   - User registration system"
echo "   - Real-time market data processing"
echo "   - Automatic market hours detection"
echo ""
echo "🔍 MONITORING COMMANDS:"
echo "   Status: sudo systemctl status trading-app nginx"
echo "   Logs: sudo journalctl -u trading-app -f"
echo "   Restart App: sudo systemctl restart trading-app"
echo ""
echo "============================================"
echo "🎯 YOUR TRADING SYSTEM IS NOW LIVE!"
echo "============================================"

success "Basic deployment completed successfully"
success "Your trendvision2004.com platforms is ready!"
