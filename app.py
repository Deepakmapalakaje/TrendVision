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
# Assuming email_utils.py is in the same directory (content provided below)
from email_utils import send_otp_email
from zoneinfo import ZoneInfo  # For IST timezone

# Setup logging for observability
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database paths (use env vars for flexibility in VM, default to 'database/' folder)
TRADING_DB = os.getenv("TRADING_DB", "database/upstox_v3_live_trading.db")
USER_DB = os.getenv("USER_DB", "database/users.db")

# Memory management settings
MAX_MEMORY_GB = 10
SLIDING_WINDOW_DAYS = 30  # Keep last 30 days of market data

# Market hours configuration (IST)
IST = ZoneInfo("Asia/Kolkata")
MARKET_START_TIME = dt_time(9, 15)  # 9:15 AM IST
MARKET_END_TIME = dt_time(15, 30)  # 3:30 PM IST

pipeline_thread = None
pipeline_running = False

def is_market_hours():
    """Check if current time is within market hours (IST)"""
    current_time = datetime.now(IST).time()
    return MARKET_START_TIME <= current_time <= MARKET_END_TIME

def check_memory_usage():
    """Check if memory usage is approaching limit"""
    memory = psutil.virtual_memory()
    memory_gb = memory.used / (1024**3)
    return memory_gb, memory_gb > MAX_MEMORY_GB

def cleanup_old_data():
    """Remove old market data using sliding window"""
    try:
        conn = get_db_connection(TRADING_DB)
        c = conn.cursor()
        # Calculate cutoff date
        cutoff_date = datetime.now(IST) - timedelta(days=SLIDING_WINDOW_DAYS)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        # Get all tables in the database
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = c.fetchall()
        deleted_count = 0
        for table in tables:
            table_name = table[0]
            # Skip system tables
            if table_name.startswith('sqlite_'):
                continue
            # Check if table has timestamp column
            try:
                c.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in c.fetchall()]
                if 'timestamp' in columns:
                    # Delete old records
                    c.execute(f"DELETE FROM {table_name} WHERE date(timestamp) < ?", (cutoff_str,))
                    deleted_count += c.rowcount
                elif 'created_at' in columns:
                    c.execute(f"DELETE FROM {table_name} WHERE date(created_at) < ?", (cutoff_str,))
                    deleted_count += c.rowcount
            except Exception as e:
                logger.error(f"Error cleaning table {table_name}: {e}")
                continue
        conn.commit()
        conn.close()
        logger.info(f"Cleaned up {deleted_count} old records (older than {SLIDING_WINDOW_DAYS} days)")
        return deleted_count
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return 0

def start_pipeline():
    """Start the market data pipeline"""
    global pipeline_thread, pipeline_running
    if pipeline_running:
        logger.info("Pipeline already running")
        return
    if not is_market_hours():
        logger.info("Outside market hours, not starting pipeline")
        return
    logger.info("Starting market data pipeline...")
    pipeline_running = True
    def run_pipeline():
        try:
            import pipeline1
            pipeline1.main()
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
        finally:
            global pipeline_running
            pipeline_running = False
            logger.info("Pipeline stopped")
    pipeline_thread = threading.Thread(target=run_pipeline, daemon=True)
    pipeline_thread.start()
    logger.info("Pipeline started successfully")

def stop_pipeline():
    """Stop the market data pipeline"""
    global pipeline_running
    if pipeline_running:
        logger.info("Stopping pipeline...")
        pipeline_running = False
        # The pipeline will stop naturally when market hours end

def schedule_pipeline():
    """Schedule pipeline to start and stop at market hours (IST)"""
    # Schedule to start at 9:15 AM every weekday
    schedule.every().monday.at("09:15").do(start_pipeline)
    schedule.every().tuesday.at("09:15").do(start_pipeline)
    schedule.every().wednesday.at("09:15").do(start_pipeline)
    schedule.every().thursday.at("09:15").do(start_pipeline)
    schedule.every().friday.at("09:15").do(start_pipeline)
    # Schedule to stop at 3:30 PM every weekday
    schedule.every().monday.at("15:30").do(stop_pipeline)
    schedule.every().tuesday.at("15:30").do(stop_pipeline)
    schedule.every().wednesday.at("15:30").do(stop_pipeline)
    schedule.every().thursday.at("15:30").do(stop_pipeline)
    schedule.every().friday.at("15:30").do(stop_pipeline)
    logger.info("Pipeline scheduled for market hours (9:15 AM - 3:30 PM IST)")

def run_scheduler():
    """Run the scheduler in a separate thread, checking every minute"""
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

def get_db_connection(db_path):
    """Get DB connection with retry and WAL mode"""
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
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "super-secret-key")
    # Ensure user DB exists
    init_user_db()

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
        return render_template("login.html")

    @app.route("/signup", methods=["GET", "POST"])
    def signup():
        if request.method == "POST":
            name = request.form.get("name", "").strip()
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "")
            if not name or not email or not password:
                flash("All fields are required", "error")
                return render_template("signup.html")
            if get_user_by_email(email):
                flash("Email already registered", "error")
                return render_template("signup.html")
            password_hash = generate_password_hash(password)
            create_user(name, email, password_hash)
            flash("Account created. Please log in.", "success")
            return redirect(url_for("login"))
        return render_template("signup.html")

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.route("/clear-session")
    def clear_session():
        session.clear()
        flash("Session cleared. Please login again.", "info")
        return redirect(url_for("login"))

    @app.route("/debug-session")
    def debug_session():
        session_info = {
            "session_data": dict(session),
            "user_id": session.get("user_id"),
            "email": session.get("email"),
            "session_exists": bool(session)
        }
        return jsonify(session_info)

    @app.route("/forgot", methods=["GET", "POST"])
    def forgot():
        if request.method == "POST":
            email = request.form.get("email", "").strip().lower()
            user = get_user_by_email(email)
            if not user:
                flash("Email not found", "error")
                return render_template("forgot.html")
            otp = create_user_otp(user["id"])
            try:
                send_otp_email(email, otp)
                flash("OTP sent to your email", "success")
                session["reset_email"] = email
                return redirect(url_for("forgot"))
            except Exception as e:
                flash(f"Failed to send OTP: {e}", "error")
        return render_template("forgot.html")

    @app.route("/reset", methods=["POST"])
    def reset_password():
        email = request.form.get("email", "").strip().lower()
        otp = request.form.get("otp", "").strip()
        new_password = request.form.get("new_password", "")
        user = get_user_by_email(email)
        if not user:
            flash("Email not found", "error")
            return redirect(url_for("forgot"))
        if not verify_user_otp(user["id"], otp):
            flash("Invalid OTP", "error")
            return redirect(url_for("forgot"))
        update_password(user["id"], generate_password_hash(new_password))
        session.pop("reset_email", None)
        flash("Password updated. Please log in.", "success")
        return redirect(url_for("login"))

    @app.route("/dashboard")
    def dashboard():
        if "user_id" not in session:
            return redirect(url_for("login"))
        user = get_user_by_id(session["user_id"])
        if not user or not user.get("terms_accepted"):
            return redirect(url_for("terms"))
        return render_template("dashboard.html")

    @app.route("/profile", methods=["GET", "POST"])
    def profile():
        if "user_id" not in session:
            return redirect(url_for("login"))
        user = get_user_by_id(session["user_id"])
        if not user or not user.get("terms_accepted"):
            return redirect(url_for("terms"))
        if request.method == "POST":
            name = request.form.get("name", user["name"]).strip()
            update_profile(user["id"], name)
            session["email"] = user["email"]
            flash("Profile updated", "success")
            user = get_user_by_id(session["user_id"])
        safe_user = dict(user) if user else {}
        formatted_date = "N/A"
        if user and user.get("created_at") and str(user["created_at"]).strip():
            try:
                created_at_str = str(user["created_at"]).strip()
                date_formats = [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d",
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%f"
                ]
                parsed_date = None
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(created_at_str, fmt)
                        break
                    except ValueError:
                        continue
                if parsed_date:
                    formatted_date = parsed_date.strftime("%B %d, %Y")
            except Exception as e:
                logger.error(f"Date formatting error: {e}")
        safe_user["formatted_date"] = formatted_date
        return render_template("profile.html", user=safe_user)

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

    @app.route("/terms", methods=["GET", "POST"])
    def terms():
        if "user_id" not in session:
            return redirect(url_for("login"))
        user = get_user_by_id(session["user_id"])
        if request.method == "POST":
            accept_terms(user["id"])
            flash("Terms accepted.", "success")
            next_url = request.args.get("next")
            return redirect(next_url) if next_url else redirect(url_for("dashboard"))
        return render_template("terms.html", user=user)

    @app.route("/admin")
    def admin():
        if "user_id" not in session:
            return redirect(url_for("login"))
        user = get_user_by_id(session["user_id"])
        if not user or not user.get("terms_accepted"):
            return redirect(url_for("terms"))
        if user["email"] != "dsar@admin.com":
            flash("Access denied. Admin privileges required.", "error")
            return redirect(url_for("dashboard"))
        return render_template("admin.html")

    @app.route("/admin-login", methods=["GET", "POST"])
    def admin_login():
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            if username == "dsar" and password == "dsar":
                admin_user = get_user_by_email("dsar@admin.com")
                if not admin_user:
                    password_hash = generate_password_hash("dsar")
                    create_user("Admin", "dsar@admin.com", password_hash)
                admin_user = get_user_by_email("dsar@admin.com")
                session["user_id"] = admin_user["id"]
                session["email"] = admin_user["email"]
                flash("Admin login successful", "success")
                if not admin_user.get("terms_accepted"):
                    return redirect(url_for("terms"))
                return redirect(url_for("admin"))
            else:
                flash("Invalid admin credentials", "error")
        return render_template("admin_login.html")

    @app.route("/api/config", methods=["GET"])
    def get_config():
        try:
            with open('config/config.json', 'r') as f:
                config = json.load(f)
            return jsonify({"ok": True, "config": config})
        except Exception as e:
            logger.error(f"Error getting config: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/config", methods=["POST"])
    def update_config():
        try:
            data = request.get_json()
            required_fields = ["ACCESS_TOKEN", "NIFTY_FUTURE_key", "ITM_CE_key", "ITM_CE_strike", "ITM_PE_key", "ITM_PE_strike"]
            for field in required_fields:
                if field not in data:
                    return jsonify({"ok": False, "error": f"Missing required field: {field}"}), 400
            data["ITM_CE_strike"] = int(data["ITM_CE_strike"])
            data["ITM_PE_strike"] = int(data["ITM_PE_strike"])
            try:
                with open('config/config.json', 'r') as f:
                    current_config = json.load(f)
                with open('config/config_backup.json', 'w') as f:
                    json.dump(current_config, f, indent=2)
            except:
                pass
            with open('config/config.json', 'w') as f:
                json.dump(data, f, indent=2)
            return jsonify({"ok": True, "message": "Config updated successfully"})
        except Exception as e:
            logger.error(f"Error updating config: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/admin/export/users")
    def export_users_csv():
        if "user_id" not in session:
            return redirect(url_for("login"))
        user = get_user_by_id(session["user_id"])
        if not user or user["email"] != "dsar@admin.com":
            flash("Access denied. Admin privileges required.", "error")
            return redirect(url_for("dashboard"))
        try:
            conn = get_db_connection(USER_DB)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("SELECT id, name, email, created_at, terms_accepted, terms_accepted_at FROM users ORDER BY created_at DESC")
            users = c.fetchall()
            conn.close()
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(['ID', 'Name', 'Email', 'Created At', 'Terms Accepted', 'Terms Accepted At'])
            for usr in users:
                writer.writerow([
                    usr['id'],
                    usr['name'],
                    usr['email'],
                    usr['created_at'],
                    'Yes' if usr['terms_accepted'] else 'No',
                    usr['terms_accepted_at'] or 'N/A'
                ])
            output.seek(0)
            return Response(output.getvalue(), mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=users_export.csv'})
        except Exception as e:
            flash(f"Export failed: {e}", "error")
            return redirect(url_for("admin"))

    @app.route("/admin/export/market-data")
    def export_market_data_csv():
        if "user_id" not in session:
            return redirect(url_for("login"))
        user = get_user_by_id(session["user_id"])
        if not user or user["email"] != "dsar@admin.com":
            flash("Access denied. Admin privileges required.", "error")
            return redirect(url_for("dashboard"))
        try:
            conn = get_db_connection(TRADING_DB)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("SELECT * FROM latest_candles ORDER BY updated_at DESC, instrument_key LIMIT 1000")
            candles = c.fetchall()
            c.execute("SELECT * FROM trend ORDER BY timestamp DESC LIMIT 1000")
            trends = c.fetchall()
            conn.close()
            output = io.StringIO()
            writer = csv.writer(output)
            if candles:
                writer.writerow(['Data Type', 'Instrument Key', 'Instrument Name', 'Open', 'High', 'Low', 'Close', 'Volume', 'Timestamp', 'Updated At'])
                for candle in candles:
                    writer.writerow([
                        'Candle',
                        candle['instrument_key'],
                        candle['instrument_name'],
                        candle['open'],
                        candle['high'],
                        candle['low'],
                        candle['close'],
                        candle['volume'],
                        candle['timestamp'],
                        candle['updated_at']
                    ])
            if trends:
                writer.writerow([])
                writer.writerow(['Data Type', 'Trend Value', 'Buy Recommendation', 'Profit Loss', 'Timestamp'])
                for trend in trends:
                    writer.writerow([
                        'Trend',
                        trend['trend_value'],
                        trend['buy_recommendation'],
                        trend.get('profit_loss', 'N/A'),
                        trend['timestamp']
                    ])
            output.seek(0)
            return Response(output.getvalue(), mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=market_data_export.csv'})
        except Exception as e:
            flash(f"Export failed: {e}", "error")
            return redirect(url_for("admin"))

    @app.route("/admin/cleanup", methods=["POST"])
    def admin_cleanup():
        if "user_id" not in session:
            return redirect(url_for("login"))
        user = get_user_by_id(session["user_id"])
        if not user or user["email"] != "dsar@admin.com":
            flash("Access denied. Admin privileges required.", "error")
            return redirect(url_for("admin"))
        try:
            deleted_count = cleanup_old_data()
            flash(f"Cleanup completed. Removed {deleted_count} old records.", "success")
        except Exception as e:
            flash(f"Cleanup failed: {e}", "error")
        return redirect(url_for("admin"))

    @app.route("/api/memory-status")
    def api_memory_status():
        if "user_id" not in session:
            return redirect(url_for("login"))
        user = get_user_by_id(session["user_id"])
        if not user or user["email"] != "dsar@admin.com":
            return jsonify({"error": "Access denied"}), 403
        try:
            memory_gb, is_over_limit = check_memory_usage()
            return jsonify({
                "memory_used_gb": round(memory_gb, 2),
                "memory_limit_gb": MAX_MEMORY_GB,
                "is_over_limit": is_over_limit,
                "cleanup_needed": is_over_limit
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

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

def create_user_otp(user_id: int) -> str:
    otp = f"{random.randint(100000, 999999)}"
    conn = get_db_connection(USER_DB)
    c = conn.cursor()
    c.execute("UPDATE users SET otp = ?, otp_created_at = CURRENT_TIMESTAMP WHERE id = ?", (otp, user_id))
    conn.commit()
    conn.close()
    return otp

def verify_user_otp(user_id: int, otp: str) -> bool:
    conn = get_db_connection(USER_DB)
    c = conn.cursor()
    c.execute("SELECT otp FROM users WHERE id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row and row[0] == otp

def update_password(user_id: int, password_hash: str):
    conn = get_db_connection(USER_DB)
    c = conn.cursor()
    c.execute("UPDATE users SET password_hash = ?, otp = NULL WHERE id = ?", (password_hash, user_id))
    conn.commit()
    conn.close()

def update_profile(user_id: int, name: str):
    conn = get_db_connection(USER_DB)
    c = conn.cursor()
    c.execute("UPDATE users SET name = ? WHERE id = ?", (name, user_id))
    conn.commit()
    conn.close()

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
            # Display name logic (from original)
            instrument_key = rowd["instrument_key"]
            if instrument_key == "NSE_INDEX|Nifty 50":
                display_name = "NIFTY 50"
            elif instrument_key == "NSE_FO|5001":
                display_name = "NIFTY Future"
            elif instrument_key == "NSE_FO|40577":
                try:
                    with open('config/config.json', 'r') as f:
                        config = json.load(f)
                    strike_price = config.get('ITM_CE_strike', 24700)
                    display_name = f"{strike_price} CE"
                except:
                    display_name = "CE Option"
            elif instrument_key == "NSE_FO|40586":
                try:
                    with open('config/config.json', 'r') as f:
                        config = json.load(f)
                    strike_price = config.get('ITM_PE_strike', 24900)
                    display_name = f"{strike_price} PE"
                except:
                    display_name = "PE Option"
            else:
                display_name = rowd.get("instrument_name", instrument_key)
            rowd["symbol"] = display_name
            rowd["display_name"] = display_name
            # Formatting and color coding (from original)
            rowd["price_change_formatted"] = f"{rowd.get('price_change', 0):+.2f}"
            rowd["price_change_pct_formatted"] = f"{rowd.get('price_change_pct', 0):+.2f}%"
            rowd["delta_formatted"] = f"{rowd.get('delta', 0):+d}"
            rowd["delta_pct_formatted"] = f"{rowd.get('delta_pct', 0):+.2f}%"
            rowd["volume_formatted"] = f"{rowd.get('volume', 0):,}"
            rowd["price_change_color"] = "positive" if rowd.get('price_change', 0) > 0 else "negative" if rowd.get('price_change', 0) < 0 else "neutral"
            rowd["delta_color"] = "positive" if rowd.get('delta', 0) > 0 else "negative" if rowd.get('delta', 0) < 0 else "neutral"
            candles.append(rowd)
            sources.append({"instrument_key": rowd["instrument_key"], "table": "latest_candles"})
        # Market summary calculation (from original)
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
        logger.warning("latest_candles table not found, using fallback")
        # Fallback logic (from original, with instrument mapping and calculations)
        data_type = "candles5" if interval == "5min" else "candles"
        instruments = [
            "NSE_INDEX|Nifty 50",
            "NSE_FO|5001",
            "NSE_FO|40577",
            "NSE_FO|40586"
        ]
        for ikey in instruments:
            tname = None
            try:
                c.execute(
                    "SELECT table_name FROM table_registry WHERE data_type = ? AND instrument_key = ? ORDER BY trade_date DESC LIMIT 1",
                    (data_type, ikey)
                )
                reg = c.fetchone()
                if reg:
                    tname = reg[0]
            except sqlite3.OperationalError:
                pass
            if not tname:
                suffix_map = {
                    "NSE_INDEX|Nifty 50": "nifty_index",
                    "NSE_FO|5001": "future",
                    "NSE_FO|40577": "ce_option",
                    "NSE_FO|40586": "pe_option"
                }
                suffix = suffix_map.get(ikey)
                if suffix:
                    pattern = f"{data_type}_{suffix}_%"
                    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ? ORDER BY name DESC LIMIT 1", (pattern,))
                    row = c.fetchone()
                    if row:
                        tname = row[0]
            if not tname:
                continue
            try:
                c.execute(f"SELECT * FROM {tname} ORDER BY timestamp DESC LIMIT 1")
                last = c.fetchone()
                if last:
                    rowd = dict(last)
                    rowd["instrument_key"] = ikey
                    # Add delta fields if missing
                    for key in ["delta", "min_delta", "max_delta", "buy_volume", "sell_volume", "tick_count", "vtt_open", "vtt_close"]:
                        if key not in rowd:
                            rowd[key] = 0
                    # Calculations
                    price_change = rowd["close"] - rowd["open"]
                    price_change_pct = (price_change / rowd["open"] * 100) if rowd["open"] > 0 else 0
                    delta_pct = (rowd["delta"] / rowd["volume"] * 100) if rowd["volume"] > 0 and rowd["delta"] != 0 else 0
                    vwap = rowd.get("atp", rowd["close"])
                    rowd["price_change"] = price_change
                    rowd["price_change_pct"] = price_change_pct
                    rowd["delta_pct"] = delta_pct
                    rowd["vwap"] = vwap
                    # Formatting and colors (same as main)
                    rowd["price_change_formatted"] = f"{price_change:+.2f}"
                    rowd["price_change_pct_formatted"] = f"{price_change_pct:+.2f}%"
                    rowd["delta_formatted"] = f"{rowd['delta']:+d}"
                    rowd["delta_pct_formatted"] = f"{delta_pct:+.2f}%"
                    rowd["volume_formatted"] = f"{rowd['volume']:,}"
                    rowd["price_change_color"] = "positive" if price_change > 0 else "negative" if price_change < 0 else "neutral"
                    rowd["delta_color"] = "positive" if rowd["delta"] > 0 else "negative" if rowd["delta"] < 0 else "neutral"
                    # Display name
                    if ikey == "NSE_INDEX|Nifty 50":
                        display_name = "NIFTY 50"
                    elif ikey == "NSE_FO|5001":
                        display_name = "NIFTY Future"
                    elif ikey == "NSE_FO|40577":
                        try:
                            with open('config/config.json', 'r') as f:
                                config = json.load(f)
                            strike = config.get('ITM_CE_strike', 24700)
                            display_name = f"{strike} CE"
                        except:
                            display_name = "24700 CE"
                    elif ikey == "NSE_FO|40586":
                        try:
                            with open('config/config.json', 'r') as f:
                                config = json.load(f)
                            strike = config.get('ITM_PE_strike', 24900)
                            display_name = f"{strike} PE"
                        except:
                            display_name = "24900 PE"
                    else:
                        display_name = ikey.split("|")[-1] if "|" in ikey else ikey
                    rowd["symbol"] = display_name
                    rowd["display_name"] = display_name
                    candles.append(rowd)
                    sources.append({"instrument_key": ikey, "table": tname})
            except Exception as e:
                logger.error(f"Error fetching from {tname}: {e}")
        candles.sort(key=lambda x: x.get("timestamp"), reverse=True)
        # Market summary and metrics (same as main try block)
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
    # Latest trend and PNL
    latest_trend = None
    pnl_data = {"profit": 0, "loss": 0}
    try:
        c.execute("SELECT * FROM trend WHERE candle_interval = ? ORDER BY timestamp DESC LIMIT 1", (interval,))
        trend_row = c.fetchone()
        latest_trend = dict(trend_row) if trend_row else None
        c.execute("""
            SELECT
                SUM(CASE WHEN profit_loss > 0 THEN profit_loss ELSE 0 END) as profit,
                ABS(SUM(CASE WHEN profit_loss < 0 THEN profit_loss ELSE 0 END)) as loss
            FROM trend
        """)
        pnl = c.fetchone()
        pnl_data = {"profit": pnl[0] or 0, "loss": pnl[1] or 0}
    except sqlite3.OperationalError:
        total_delta = performance_metrics.get("total_delta", 0)
        latest_trend = {
            "trend_value": 1 if total_delta > 0 else -1 if total_delta < 0 else 0,
            "buy_recommendation": None,
            "timestamp": datetime.now(IST).isoformat() if candles else None
        }
    conn.close()
    return {
        "candles": candles,
        "trend": {
            "trend_value": latest_trend.get("trend_value") if latest_trend else 0,
            "buy_recommendation": latest_trend.get("buy_recommendation") if latest_trend else None,
            "timestamp": latest_trend.get("timestamp") if latest_trend else None,
        },
        "pnl": pnl_data,
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
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)  # Set debug=False for production
