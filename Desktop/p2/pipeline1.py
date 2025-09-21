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
from typing import Optional, List, Tuple, Dict
import threading
from dotenv import load_dotenv
from zoneinfo import ZoneInfo  # For IST
import pandas as pd
import numpy as np

load_dotenv()

# Import the V3 protobuf
import MarketDataFeedV3_pb2 as pb

# Constants and Configurations
CONFIG_PATH = 'config/config.json'
UPSTOX_V3_AUTH_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
TRADING_DB = os.getenv('TRADING_DB', 'database/upstox_v3_live_trading.db')

def load_config():
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

config = load_config()
ACCESS_TOKEN = config.get('ACCESS_TOKEN', '')

# === AUTO OPTION SELECTION ===
def auto_select_options(csv_path):
    """Auto-select 60 options based on NIFTY level rounded to nearest 50"""
    try:
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return {}
        df = pd.read_csv(csv_path)
        if df.empty:
            logger.error("Empty CSV file")
            return {}

        # Get current NIFTY level from CSV (assume column exists)
        all_strikes = sorted(df['strike'].unique(), key=float)
        ce_options = df[df['option_type'] == 'CE']
        pe_options = df[df['option_type'] == 'PE']

        if ce_options.empty or pe_options.empty:
            logger.error("CE or PE options not found in CSV")
            return {}

        # Find ATM strike by minimizing call-put spread
        atm_options = []
        for strike in ce_options['strike'].unique():
            if strike in pe_options['strike'].values:
                ce_price = ce_options[ce_options['strike'] == strike]['last_price'].iloc[0]
                pe_price = pe_options[pe_options['strike'] == strike]['last_price'].iloc[0]
                spread = abs(ce_price - pe_price)
                atm_options.append((strike, spread))

        atm_strike = min(atm_options, key=lambda x: x[1])[0] if atm_options else ce_options['strike'].iloc[len(ce_options) // 2]

        # Round to nearest multiple of 50
        current_level = round(float(atm_strike) / 50) * 50
        logger.info(f"🎯 Auto-selected NIFTY level: {current_level} (ATM: {atm_strike})")

        # Select 30 CE and 30 PE around current level
        ce_selected = []
        pe_selected = []

        # CE: select strikes above current_level (OTM)
        higher_strikes = sorted([s for s in all_strikes if s >= current_level])
        ce_selected = df[(df['option_type'] == 'CE') & (df['strike'].isin(higher_strikes[:30]))]

        # PE: select strikes below current_level (OTM)
        lower_strikes = sorted([s for s in all_strikes if s <= current_level], reverse=True)
        pe_selected = df[(df['option_type'] == 'PE') & (df['strike'].isin(lower_strikes[:30]))]

        selected = pd.concat([ce_selected, pe_selected]).sort_values(['option_type', 'strike'])

        ce_count = len(ce_selected)
        pe_count = len(pe_selected)
        logger.info(f"✅ Selected: {ce_count} CE + {pe_count} PE = {len(selected)} options")

        return selected, current_level
    except Exception as e:
        logger.error(f"Error in auto_select_options: {e}")
        return pd.DataFrame(), 0

def load_dynamic_instruments():
    """Load dynamic instruments from CSV"""
    selected_options, current_level = auto_select_options('extracted_data.csv')

    if selected_options.empty:
        logger.warning("No options selected, falling back to minimal config")
        return {
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
            }
        }

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
        }
    }

    # Add selected options
    option_counter = {}
    for idx, row in selected_options.iterrows():
        option_type = row['option_type']
        strike = row['strike']
        instrument_key = row['instrument_key']

        if option_type not in option_counter:
            option_counter[option_type] = 0
        option_counter[option_type] += 1

        name = f"{option_type}_{strike}"
        INSTRUMENTS[name] = {
            "key": instrument_key,
            "type": "OPTION",
            "option_type": option_type,
            "strike_price": strike,
            "has_volume": True,
            "process_delta": True,
            "process_heikin_ashi": False,
            "process_indicators": False,
            "trading_enabled": True,
            "table_suffix": f"{option_type.lower()}_{strike}"
        }

    logger.info(f"Loaded {len(INSTRUMENTS)} total instruments: Index=1, Future=1, Options={len(selected_options)}")
    return INSTRUMENTS

# Instruments setup (Dynamic from CSV)
INSTRUMENTS = load_dynamic_instruments()

INSTRUMENT_KEYS = [cfg["key"] for cfg in INSTRUMENTS.values()]

# Technical Indicator Settings
SAR_START = 0.4
SAR_INCREMENT = 0.3
SAR_MAXIMUM = 0.7

# Time Configuration (all in IST)
IST = ZoneInfo("Asia/Kolkata")
MARKET_START_TIME = dt_time(9, 15)  # 9:15 AM IST
MARKET_END_TIME = dt_time(15, 30)  # 3:30 PM IST
TRADING_START_TIME = dt_time(10, 0)  # 10:00 AM IST - Buy recommendations start
NO_NEW_TRADES_TIME = dt_time(15, 0)  # 3:00 PM IST - No new trade recommendations

# Global variables
shutdown_event = None
db_manager = None
processors = {}
cash_flow_calculator = None  # Global cash flow calculator for 60 options
buy_signal_generator = None  # Global buy signal generator
option_tracker = None  # Global option tracker for specific P&L logic

# === LOGGING SETUP ===
def setup_logging():
    log_format = '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler('upstox_v3_trading.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    # Fix Unicode encoding for Windows console
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setStream(sys.stdout)
    return logging.getLogger('UpstoxTradingV3')

logger = setup_logging()

# === SQLite datetime adapters ===
def adapt_datetime_iso(val):
    return val.isoformat()

def convert_datetime(val):
    return datetime.fromisoformat(val.decode())

sqlite3.register_adapter(datetime, adapt_datetime_iso)
sqlite3.register_converter("timestamp", convert_datetime)

# === DATA STRUCTURES ===
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
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    rho: Optional[float] = None
    iv: Optional[float] = None

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

@dataclass
class HeikinAshiCandle:
    instrument_key: str
    timestamp: datetime
    ha_open: float
    ha_high: float
    ha_low: float
    ha_close: float
    volume: int
    hlc3: float
    sar_trend: Optional[int] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None

# === ENHANCED LOCK-FREE DATABASE MANAGER WITH TREND TABLE ===
class LockFreeDatabaseManager:
    def __init__(self):
        self.live_db_path = TRADING_DB
        self.running = True
        # Thread-safe deques for ultra-fast queuing
        self.candle_queue = deque()
        self.ha_queue = deque()
        self.trend_queue = deque()  # NEW: Trend data queue
        self.latest_candle_queue = deque()  # NEW: Latest candles queue
        self._queue_lock = threading.Lock()
        # Single database connection for exclusive access
        self.db_conn = None
        self.cursor = None
        self._initialize_database()
        # Start single-threaded database writer
        self.db_thread = threading.Thread(target=self._single_thread_db_writer, daemon=True)
        self.db_thread.start()
        logger.info("Enhanced lock-free database manager with trend table initialized")

    def _initialize_database(self):
        max_retries = 5
        retry_delay = 0.5
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self.live_db_path, timeout=30.0, detect_types=sqlite3.PARSE_DECLTYPES)
                cursor = conn.cursor()
                # Enable WAL mode for better concurrency
                cursor.execute("PRAGMA journal_mode = WAL")
                cursor.execute("PRAGMA synchronous = NORMAL")
                cursor.execute("PRAGMA locking_mode = NORMAL")
                # Additional SQLite settings
                cursor.execute("PRAGMA cache_size = 500000")
                cursor.execute("PRAGMA temp_store = MEMORY")
                cursor.execute("PRAGMA mmap_size = 268435456")
                cursor.execute("PRAGMA page_size = 4096")
                cursor.execute("PRAGMA count_changes = OFF")
                cursor.execute("PRAGMA auto_vacuum = NONE")
                cursor.execute("PRAGMA busy_timeout = 10000")  # Increased to 10s for better concurrency handling
                # Create registry table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS table_registry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT UNIQUE NOT NULL,
                    instrument_key TEXT NOT NULL,
                    data_type TEXT NOT NULL,
                    trade_date DATE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                ''')
                # Create trend table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS trend (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP NOT NULL,
                    candle_interval TEXT NOT NULL,
                    trend_value INTEGER NOT NULL,
                    buy_recommendation TEXT,
                    entry_price REAL,
                    target REAL,
                    sl REAL,
                    profit_loss REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                ''')
                # Create latest_candles table for chat-like display with enhanced fields
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS latest_candles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instrument_key TEXT UNIQUE NOT NULL,
                    instrument_name TEXT NOT NULL,
                    instrument_type TEXT NOT NULL,
                    strike_price REAL,
                    option_type TEXT,
                    timestamp TIMESTAMP NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    atp REAL NOT NULL,
                    vwap REAL DEFAULT 0,
                    price_change REAL DEFAULT 0,
                    price_change_pct REAL DEFAULT 0,
                    delta INTEGER DEFAULT 0,
                    delta_pct REAL DEFAULT 0,
                    min_delta INTEGER DEFAULT 0,
                    max_delta INTEGER DEFAULT 0,
                    buy_volume INTEGER DEFAULT 0,
                    sell_volume INTEGER DEFAULT 0,
                    tick_count INTEGER DEFAULT 0,
                    vtt_open REAL DEFAULT 0,
                    vtt_close REAL DEFAULT 0,
                    candle_interval TEXT NOT NULL,
                    trend_value INTEGER DEFAULT 0,
                    buy_recommendation TEXT,
                    entry_price REAL,
                    target REAL,
                    sl REAL,
                    profit_loss REAL,
                    prev_close REAL DEFAULT 0,
                    intraday_high REAL DEFAULT 0,
                    intraday_low REAL DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                ''')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trend_timestamp ON trend(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trend_interval ON trend(candle_interval)')
                # Create options cash flow table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS options_cash_flow (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    interval_type TEXT NOT NULL,
                    cash REAL NOT NULL,
                    min_cash REAL NOT NULL,
                    max_cash REAL NOT NULL,
                    total_options INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                ''')
                # Add missing tables: buy_signals and option_tracking
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS buy_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    option_key TEXT NOT NULL,
                    strike REAL NOT NULL,
                    entry_price REAL DEFAULT 0,
                    target REAL DEFAULT 0,
                    sl REAL DEFAULT 0,
                    status TEXT DEFAULT 'ACTIVE',
                    cash_flow REAL DEFAULT 0
                );
                ''')

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS option_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id INTEGER REFERENCES buy_signals(id),
                    timestamp TEXT NOT NULL,
                    current_price REAL NOT NULL,
                    pnl REAL DEFAULT 0,
                    status TEXT NOT NULL
                );
                ''')

                # Add indexes for performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON buy_signals(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_tracking_signal_id ON option_tracking(signal_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_cash_flow_timestamp ON options_cash_flow(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_latest_candles_instrument ON latest_candles(instrument_key)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_latest_candles_updated ON latest_candles(updated_at)')
                conn.commit()
                conn.close()
                logger.info("Database initialized with all required tables")
                break
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"Database locked, retrying in {retry_delay:.1f}s... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.critical(f"Failed to initialize database after {max_retries} attempts: {e}")
                    raise
            except Exception as e:
                logger.critical(f"Failed to initialize database: {e}")
                raise

    def _single_thread_db_writer(self):
        try:
            self.db_conn = sqlite3.connect(
                self.live_db_path,
                timeout=60.0,
                detect_types=sqlite3.PARSE_DECLTYPES,
                check_same_thread=False
            )
            self.db_conn.execute("PRAGMA journal_mode = WAL")
            self.db_conn.execute("PRAGMA synchronous = NORMAL")
            self.db_conn.execute("PRAGMA locking_mode = NORMAL")
            self.db_conn.execute("PRAGMA cache_size = 10000")
            self.db_conn.execute("PRAGMA temp_store = MEMORY")
            self.db_conn.execute("PRAGMA busy_timeout = 10000")  # Increased for better handling
            self.cursor = self.db_conn.cursor()
            logger.info("Single-threaded database writer started")
            while self.running:
                try:
                    processed_items = 0
                    candle_batch = []
                    with self._queue_lock:
                        for _ in range(min(1000, len(self.candle_queue))):
                            if self.candle_queue:
                                candle_batch.append(self.candle_queue.popleft())
                    if candle_batch:
                        self._write_candle_batch(candle_batch)
                        processed_items += len(candle_batch)
                    ha_batch = []
                    with self._queue_lock:
                        for _ in range(min(1000, len(self.ha_queue))):
                            if self.ha_queue:
                                ha_batch.append(self.ha_queue.popleft())
                    if ha_batch:
                        self._write_ha_batch(ha_batch)
                        processed_items += len(ha_batch)
                    trend_batch = []
                    with self._queue_lock:
                        for _ in range(min(1000, len(self.trend_queue))):
                            if self.trend_queue:
                                trend_batch.append(self.trend_queue.popleft())
                    if trend_batch:
                        self._write_trend_batch(trend_batch)
                        processed_items += len(trend_batch)
                    latest_candle_batch = []
                    with self._queue_lock:
                        for _ in range(min(1000, len(self.latest_candle_queue))):
                            if self.latest_candle_queue:
                                latest_candle_batch.append(self.latest_candle_queue.popleft())
                    if latest_candle_batch:
                        self._write_latest_candle_batch(latest_candle_batch)
                        processed_items += len(latest_candle_batch)
                    if processed_items == 0:
                        time.sleep(0.001)
                except Exception as e:
                    logger.error(f"Error in database writer: {e}")
                    time.sleep(0.01)
        except Exception as e:
            logger.error(f"Fatal error in database writer: {e}")
        finally:
            if self.db_conn:
                try:
                    self.db_conn.close()
                except:
                    pass

    def _write_candle_batch(self, batch):
        if not batch:
            return
        try:
            table_batches = {}
            for candle, interval in batch:
                data_type = f"candles{'5' if interval == '5min' else ''}"
                table_name = self._get_table_name(candle.instrument_key, data_type)
                if table_name not in table_batches:
                    table_batches[table_name] = []
                table_batches[table_name].append((candle, interval))
            self.cursor.execute("BEGIN IMMEDIATE")
            for table_name, candles in table_batches.items():
                self._create_candle_table_sync(table_name, candles[0][0].instrument_key, candles[0][1])
                insert_data = []
                for candle, _ in candles:
                    insert_data.append((
                        candle.instrument_key, candle.timestamp, candle.open, candle.high,
                        candle.low, candle.close, candle.volume, candle.atp,
                        candle.delta, candle.min_delta, candle.max_delta,
                        candle.buy_volume, candle.sell_volume, candle.tick_count,
                        candle.vtt_open, candle.vtt_close
                    ))
                self.cursor.executemany(
                    f'''
                    INSERT OR REPLACE INTO {table_name}
                    (instrument_key, timestamp, open, high, low, close, volume, atp,
                    delta, min_delta, max_delta, buy_volume, sell_volume, tick_count, vtt_open, vtt_close)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    insert_data
                )
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Error writing candle batch: {e}")
            try:
                self.db_conn.rollback()
            except:
                pass

    def _write_ha_batch(self, batch):
        if not batch:
            return
        try:
            table_batches = {}
            for ha_candle, interval in batch:
                data_type = f"heikin_ashi{'5' if interval == '5min' else ''}"
                table_name = self._get_table_name(ha_candle.instrument_key, data_type)
                if table_name not in table_batches:
                    table_batches[table_name] = []
                table_batches[table_name].append((ha_candle, interval))
            self.cursor.execute("BEGIN IMMEDIATE")
            for table_name, ha_candles in table_batches.items():
                self._create_heikin_ashi_table_sync(table_name, ha_candles[0][0].instrument_key, ha_candles[0][1])
                insert_data = []
                for ha_candle, _ in ha_candles:
                    insert_data.append((
                        ha_candle.instrument_key, ha_candle.timestamp,
                        ha_candle.ha_open, ha_candle.ha_high, ha_candle.ha_low, ha_candle.ha_close,
                        ha_candle.volume, ha_candle.hlc3,
                        ha_candle.sar_trend, ha_candle.macd, ha_candle.macd_signal
                    ))
                self.cursor.executemany(f'''
                    INSERT OR REPLACE INTO {table_name}
                    (instrument_key, timestamp, ha_open, ha_high, ha_low, ha_close,
                    volume, hlc3, sar_trend, macd, macd_signal)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', insert_data)
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Error writing HA batch: {e}")
            try:
                self.db_conn.rollback()
            except:
                pass

    def _write_trend_batch(self, batch):
        if not batch:
            return
        try:
            self.cursor.execute("BEGIN IMMEDIATE")
            insert_data = []
            for trend_data in batch:
                insert_data.append((
                    trend_data['timestamp'],
                    trend_data['candle_interval'],
                    trend_data['trend_value'],
                    trend_data.get('buy_recommendation'),
                    trend_data.get('entry_price'),
                    trend_data.get('target'),
                    trend_data.get('sl'),
                    trend_data.get('profit_loss')
                ))
            self.cursor.executemany('''
                INSERT INTO trend
                (timestamp, candle_interval, trend_value, buy_recommendation, entry_price, target, sl, profit_loss)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', insert_data)
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Error writing trend batch: {e}")
            try:
                self.db_conn.rollback()
            except:
                pass

    def _write_latest_candle_batch(self, batch):
        if not batch:
            return
        try:
            self.cursor.execute("BEGIN IMMEDIATE")
            insert_data = []
            for candle_data in batch:
                insert_data.append((
                    candle_data['instrument_key'],
                    candle_data['instrument_name'],
                    candle_data['instrument_type'],
                    candle_data.get('strike_price'),
                    candle_data.get('option_type'),
                    candle_data['timestamp'],
                    candle_data['open'],
                    candle_data['high'],
                    candle_data['low'],
                    candle_data['close'],
                    candle_data['volume'],
                    candle_data['atp'],
                    candle_data.get('vwap', 0),
                    candle_data.get('price_change', 0),
                    candle_data.get('price_change_pct', 0),
                    candle_data.get('delta', 0),
                    candle_data.get('delta_pct', 0),
                    candle_data.get('min_delta', 0),
                    candle_data.get('max_delta', 0),
                    candle_data.get('buy_volume', 0),
                    candle_data.get('sell_volume', 0),
                    candle_data.get('tick_count', 0),
                    candle_data.get('vtt_open', 0),
                    candle_data.get('vtt_close', 0),
                    candle_data['candle_interval'],
                    candle_data.get('trend_value', 0),
                    candle_data.get('buy_recommendation'),
                    candle_data.get('entry_price'),
                    candle_data.get('target'),
                    candle_data.get('sl'),
                    candle_data.get('profit_loss'),
                    candle_data.get('prev_close', 0),
                    candle_data.get('intraday_high', 0),
                    candle_data.get('intraday_low', 0),
                    candle_data.get('last_updated')
                ))
            self.cursor.executemany('''
                INSERT OR REPLACE INTO latest_candles
                (instrument_key, instrument_name, instrument_type, strike_price, option_type,
                timestamp, open, high, low, close, volume, atp, vwap, price_change, price_change_pct,
                delta, delta_pct, min_delta, max_delta, buy_volume, sell_volume, tick_count,
                vtt_open, vtt_close, candle_interval, trend_value, buy_recommendation,
                entry_price, target, sl, profit_loss, prev_close, intraday_high, intraday_low, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', insert_data)
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Error writing latest candle batch: {e}")
            try:
                self.db_conn.rollback()
            except:
                pass

    def _get_table_name(self, instrument_key: str, data_type: str, trade_date: date = None) -> str:
        if trade_date is None:
            trade_date = datetime.now(IST).date()  # Fixed: Use IST for date
        instrument_suffix = "unknown"
        for name, config in INSTRUMENTS.items():
            if config["key"] == instrument_key:
                instrument_suffix = config["table_suffix"]
                break
        date_str = trade_date.strftime("%Y%m%d")
        return f"{data_type}_{instrument_suffix}_{date_str}"

    def _create_candle_table_sync(self, table_name: str, instrument_key: str, interval: str = "1min"):
        try:
            instrument_config = next((config for config in INSTRUMENTS.values() if config["key"] == instrument_key), None)
            is_index = instrument_config and instrument_config["type"] == "INDEX"
            volume_default = " DEFAULT 0" if is_index else ""
            data_type = f"candles{'5' if interval == '5min' else ''}"
            self.cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instrument_key TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL{volume_default},
                    atp REAL NOT NULL,
                    delta INTEGER DEFAULT 0,
                    min_delta INTEGER DEFAULT 0,
                    max_delta INTEGER DEFAULT 0,
                    buy_volume INTEGER DEFAULT 0,
                    sell_volume INTEGER DEFAULT 0,
                    tick_count INTEGER DEFAULT 0,
                    vtt_open REAL DEFAULT 0,
                    vtt_close REAL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp)
                )
                ''')
            self.cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp)')
            self.cursor.execute('''
                INSERT OR IGNORE INTO table_registry
                (table_name, instrument_key, data_type, trade_date)
                VALUES (?, ?, ?, ?)
                ''', (table_name, instrument_key, data_type, datetime.now(IST).date()))
        except Exception as e:
            logger.error(f"Error creating candle table: {e}")

    def _create_heikin_ashi_table_sync(self, table_name: str, instrument_key: str, interval: str = "1min"):
        try:
            instrument_config = next((config for config in INSTRUMENTS.values() if config["key"] == instrument_key), None)
            is_index = instrument_config and instrument_config["type"] == "INDEX"
            volume_default = " DEFAULT 0" if is_index else ""
            data_type = f"heikin_ashi{'5' if interval == '5min' else ''}"
            self.cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instrument_key TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    ha_open REAL NOT NULL,
                    ha_high REAL NOT NULL,
                    ha_low REAL NOT NULL,
                    ha_close REAL NOT NULL,
                    volume INTEGER NOT NULL{volume_default},
                    hlc3 REAL NOT NULL,
                    sar_trend INTEGER,
                    macd REAL,
                    macd_signal REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp)
                )
                ''')
            self.cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp)')
            self.cursor.execute('''
                INSERT OR IGNORE INTO table_registry
                (table_name, instrument_key, data_type, trade_date)
                VALUES (?, ?, ?, ?)
                ''', (table_name, instrument_key, data_type, datetime.now(IST).date()))
        except Exception as e:
            logger.error(f"Error creating Heikin Ashi table: {e}")

    def save_candle_instant(self, candle: LiveCandle, interval: str = "1min"):
        try:
            with self._queue_lock:
                self.candle_queue.append((candle, interval))
        except Exception:
            pass

    def save_ha_candle_instant(self, ha_candle: HeikinAshiCandle, interval: str = "1min"):
        try:
            with self._queue_lock:
                self.ha_queue.append((ha_candle, interval))
        except Exception:
            pass

    def save_trend_instant(self, trend_data: dict):
        try:
            with self._queue_lock:
                self.trend_queue.append(trend_data)
        except Exception:
            pass

    def save_latest_candle_instant(self, candle_data: dict):
        try:
            with self._queue_lock:
                self.latest_candle_queue.append(candle_data)
        except Exception:
            pass

    def get_queue_sizes(self):
        with self._queue_lock:
            return len(self.candle_queue), len(self.ha_queue), len(self.trend_queue), len(self.latest_candle_queue)

    def shutdown(self):
        self.running = False
        if self.db_thread.is_alive():
            self.db_thread.join(timeout=5)

# === OPTIMIZED TECHNICAL INDICATORS ===
class FastSAR:
    def __init__(self, start=SAR_START, increment=SAR_INCREMENT, maximum=SAR_MAXIMUM):
        self.start = start
        self.increment = increment
        self.maximum = maximum
        self.sar = None
        self.ep = None
        self.af = start
        self.trend = 1

    def update(self, high: float, low: float, close: float) -> Tuple[float, int]:
        if self.sar is None:
            self.sar = low
            self.ep = high
            return self.sar, self.trend
        try:
            new_sar = self.sar + self.af * (self.ep - self.sar)
            if self.trend == 1:  # Uptrend
                if new_sar > low:
                    self.trend = -1
                    self.sar = self.ep
                    self.ep = low
                    self.af = self.start
                else:
                    self.sar = new_sar
                    if high > self.ep:
                        self.ep = high
                        self.af = min(self.af + self.increment, self.maximum)
            else:  # Downtrend
                if new_sar < high:
                    self.trend = 1
                    self.sar = self.ep
                    self.ep = high
                    self.af = self.start
                else:
                    self.sar = new_sar
                    if low < self.ep:
                        self.ep = low
                        self.af = min(self.af + self.increment, self.maximum)
            return self.sar, self.trend
        except Exception:
            return self.sar or 0.0, self.trend



# === V3 API FUNCTIONS ===
def get_market_data_feed_authorize_v3():
    try:
        headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
        response = requests.get(UPSTOX_V3_AUTH_URL, headers=headers, timeout=10.0)
        if response.status_code == 200:
            logger.info("Retrieved V3 WebSocket URL successfully")
            return response.json()
        else:
            logger.error(f"Failed to get V3 WebSocket URL: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting V3 WebSocket URL: {e}")
        return None

def create_v3_subscription_message(instrument_keys: List[str], mode: str = "full"):
    try:
        data = {
            "guid": "upstox-v3-lockfree-system",
            "method": "sub",
            "data": {"mode": mode, "instrumentKeys": instrument_keys}
        }
        return json.dumps(data).encode('utf-8')
    except Exception as e:
        logger.error(f"Error creating V3 subscription message: {e}")
        return b""

def decode_v3_message(data):
    try:
        if isinstance(data, bytes):
            try:
                response = pb.FeedResponse()
                response.ParseFromString(data)
                return MessageToDict(response)
            except Exception as e:
                logger.debug(f"Protobuf decode failed: {e}")
                try:
                    return json.loads(data.decode('utf-8'))
                except Exception as e:
                    logger.debug(f"JSON decode failed: {e}")
                    pass
        else:
            try:
                return json.loads(data)
            except Exception as e:
                logger.debug(f"JSON decode failed: {e}")
                pass
        return None
    except Exception as e:
        logger.error(f"Error decoding message: {e}")
        return None

# === ENHANCED LOCK-FREE TICK PROCESSOR WITH TREND & TRADE MANAGEMENT ===
class LockFreeTickProcessor:
    def __init__(self, instrument_key: str, config: dict, db_manager: LockFreeDatabaseManager, all_processors: dict):
        self.instrument_key = instrument_key
        self.config = config
        self.db_manager = db_manager
        self.all_processors = all_processors
        self.instrument_type = config["type"]
        # State variables
        self.current_candle: Optional[LiveCandle] = None
        self.current_minute: Optional[datetime] = None
        self.previous_tick: Optional[LiveTick] = None
        self.previous_vtt: float = 0.0
        # 5-min state
        self.current_5min_candle: Optional[LiveCandle] = None
        self.current_5min_start: Optional[datetime] = None
        self.min_candles_in_5min = 0
        # Optimized aggregation data
        self.agg = {
            'high': float('-inf'), 'low': float('inf'), 'volume': 0,
            'delta': 0, 'min_delta': 0, 'max_delta': 0,
            'atp_sum': 0.0, 'atp_volume': 0
        }
        # Heikin Ashi state
        self.previous_ha_candle: Optional[HeikinAshiCandle] = None
        self.previous_ha_candle_5min: Optional[HeikinAshiCandle] = None
        # Fast indicators
        if self.config.get("process_indicators", False):
            self.sar_calc = FastSAR()
            self.sar_calc_5min = FastSAR()
        # Trend and trade state (per timeframe)
        self.previous_trend_1min = 0
        self.present_trend_1min = 0
        self.previous_trend_5min = 0
        self.present_trend_5min = 0
        self.active_trade_1min = None
        self.active_trade_5min = None
        # Stats
        self.processed_ticks = 0
        self.completed_candles = 0
        self.completed_5min_candles = 0
        logger.info(f"Enhanced processor initialized for {instrument_key}")

    def decode_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        try:
            timestamp_ms = int(timestamp_str)
            dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            return dt_utc.astimezone(IST)
        except Exception:
            return None

    def extract_tick_data_v3(self, feed_data) -> Optional[LiveTick]:
        try:
            if 'feeds' not in feed_data or self.instrument_key not in feed_data['feeds']:
                return None
            instrument_feed = feed_data['feeds'][self.instrument_key]
            # Handle full feed
            if 'fullFeed' in instrument_feed:
                full_feed = instrument_feed['fullFeed']
                if 'marketFF' in full_feed:
                    market_ff = full_feed['marketFF']
                    ltpc = market_ff.get('ltpc', {})
                    timestamp = self.decode_timestamp(ltpc.get('ltt'))
                    if not timestamp:
                        return None
                    ltp = float(ltpc.get('ltp', 0))
                    atp = float(market_ff.get('atp', ltp))
                    vtt = full_feed.get('vtt') or market_ff.get('vtt')
                    volume = None
                    if 'marketOHLC' in market_ff and 'ohlc' in market_ff['marketOHLC']:
                        for ohlc in market_ff['marketOHLC']['ohlc']:
                            if ohlc.get('interval') == 'I1':
                                volume = int(ohlc.get('vol', 0))
                                break
                    return LiveTick(
                        instrument_key=self.instrument_key,
                        instrument_type=self.instrument_type,
                        timestamp=timestamp,
                        ltp=ltp,
                        atp=atp,
                        vtt=float(vtt) if vtt is not None else None,
                        volume=volume,
                        prev_close=float(ltpc.get('cp', 0))
                    )
                elif 'indexFF' in full_feed:
                    index_ff = full_feed['indexFF']
                    ltpc = index_ff.get('ltpc', {})
                    timestamp = self.decode_timestamp(ltpc.get('ltt'))
                    if not timestamp:
                        return None
                    ltp = float(ltpc.get('ltp', 0))
                    return LiveTick(
                        instrument_key=self.instrument_key,
                        instrument_type=self.instrument_type,
                        timestamp=timestamp,
                        ltp=ltp,
                        atp=ltp,
                        prev_close=float(ltpc.get('cp', 0))
                    )
            # Handle LTPC only
            elif 'ltpc' in instrument_feed:
                ltpc = instrument_feed['ltpc']
                timestamp = self.decode_timestamp(ltpc.get('ltt'))
                if not timestamp:
                    return None
                ltp = float(ltpc.get('ltp', 0))
                return LiveTick(
                    instrument_key=self.instrument_key,
                    instrument_type=self.instrument_type,
                    timestamp=timestamp,
                    ltp=ltp,
                    atp=ltp,
                    prev_close=float(ltpc.get('cp', 0))
                )
            return None
        except Exception as e:
            logger.error(f"Error extracting tick data: {e}")
            return None

    def get_candle_minute(self, timestamp: datetime) -> datetime:
        adjusted_time = timestamp - timedelta(seconds=1)
        return adjusted_time.replace(second=0, microsecond=0)

    def get_5min_bucket(self, timestamp: datetime) -> datetime:
        bucket_minute = (timestamp.minute // 5) * 5
        return timestamp.replace(minute=bucket_minute, second=0, microsecond=0)

    def process_tick(self, feed_data):
        try:
            tick = self.extract_tick_data_v3(feed_data)
            if not tick:
                return
            current_time = tick.timestamp.time()
            logger.info(f"Processing tick at IST {current_time.strftime('%H:%M:%S')} for {self.instrument_key}")
            if current_time >= MARKET_END_TIME:
                logger.info(f"🏁 Market hours ended at 3:30 PM IST - Finalizing last candles")
                if self.current_candle:
                    self._finalize_current_candle()
                if self.current_5min_candle:
                    self._finalize_5min_candle()
                global shutdown_event
                shutdown_event.set()
                return
            candle_minute = self.get_candle_minute(tick.timestamp)
            if self.current_minute and self.current_minute != candle_minute:
                self._finalize_current_candle()
            if self.current_minute != candle_minute:
                self._initialize_candle(candle_minute, tick)
            self._update_candle_ultra_fast(tick)
            self._manage_active_trades_tick(tick)
            self.processed_ticks += 1
            self.previous_tick = tick
        except Exception as e:
            logger.error(f"Error processing tick: {e}")

    def _manage_active_trades_tick(self, tick: LiveTick):
        current_time = tick.timestamp.time()
        if self.active_trade_1min:
            result = self._check_trade_exit(self.active_trade_1min, tick.ltp, current_time)
            if result:
                self.active_trade_1min = None
                trend_data = {
                    'timestamp': tick.timestamp,
                    'candle_interval': '1min',
                    'trend_value': 0,
                    'buy_recommendation': result['type'],
                    'entry_price': result['entry_price'],
                    'target': result['target'],
                    'sl': result['sl'],
                    'profit_loss': result['profit_loss']
                }
                self.db_manager.save_trend_instant(trend_data)
        if self.active_trade_5min:
            result = self._check_trade_exit(self.active_trade_5min, tick.ltp, current_time)
            if result:
                self.active_trade_5min = None
                trend_data = {
                    'timestamp': tick.timestamp,
                    'candle_interval': '5min',
                    'trend_value': 0,
                    'buy_recommendation': result['type'],
                    'entry_price': result['entry_price'],
                    'target': result['target'],
                    'sl': result['sl'],
                    'profit_loss': result['profit_loss']
                }
                self.db_manager.save_trend_instant(trend_data)

    def _check_trade_exit(self, trade, current_price, current_time):
        if trade['status'] != 'active':
            return None
        if not trade.get('trail_triggered') and current_price >= trade['entry_price'] + 4:
            trade['sl'] = trade['entry_price'] + 2
            trade['trail_triggered'] = True
            logger.info(f"Trailing SL shifted to {trade['sl']:.2f} for {trade['type']} at {current_time}")
        if current_price >= trade['target']:
            profit_loss = 4875
            logger.info(f"Target Hit: Profit {profit_loss} for {trade['type']} at {current_time}")
            trade['status'] = 'completed'
            trade['profit_loss'] = profit_loss
            return trade
        if current_price <= trade['sl']:
            if trade.get('trail_triggered'):
                profit_loss = 1950
                logger.info(f"📉 Trailing SL Hit: Profit {profit_loss} for {trade['type']} at {current_time}")
            else:
                profit_loss = -4875
                logger.info(f"📉 SL Hit: Loss {profit_loss} for {trade['type']} at {current_time}")
            trade['status'] = 'completed'
            trade['profit_loss'] = profit_loss
            return trade
        return None

    def _initialize_candle(self, minute: datetime, tick: LiveTick):
        self.current_minute = minute
        vtt_open = tick.vtt if tick.vtt is not None else 0.0
        volume = 0 if self.instrument_type == "INDEX" else 0
        self.current_candle = LiveCandle(
            instrument_key=self.instrument_key,
            timestamp=minute,
            open=tick.ltp, high=tick.ltp, low=tick.ltp, close=tick.ltp,
            volume=volume, atp=tick.atp or tick.ltp,
            vtt_open=vtt_open, vtt_close=vtt_open
        )
        self.previous_vtt = vtt_open
        _5min_start = self.get_5min_bucket(minute)
        if self.current_5min_start != _5min_start:
            self._initialize_5min_candle(_5min_start, tick)

    def _initialize_5min_candle(self, _5min_start: datetime, tick: LiveTick):
        self.current_5min_start = _5min_start
        self.min_candles_in_5min = 0
        self.agg = {
            'high': tick.ltp, 'low': tick.ltp, 'volume': 0,
            'delta': 0, 'min_delta': 0, 'max_delta': 0,
            'atp_sum': 0.0, 'atp_volume': 0
        }
        self.current_5min_candle = LiveCandle(
            instrument_key=self.instrument_key,
            timestamp=_5min_start,
            open=tick.ltp, high=tick.ltp, low=tick.ltp, close=tick.ltp,
            volume=0, atp=tick.atp or tick.ltp
        )

    def _update_candle_ultra_fast(self, tick: LiveTick):
        if not self.current_candle:
            return
        if (self.previous_tick and
            tick.ltp == self.previous_tick.ltp and
            tick.vtt == self.previous_tick.vtt):
            return
        self.current_candle.close = tick.ltp
        if tick.ltp > self.current_candle.high:
            self.current_candle.high = tick.ltp
        if tick.ltp < self.current_candle.low:
            self.current_candle.low = tick.ltp
        self.current_candle.atp = tick.atp or tick.ltp
        self.current_candle.tick_count += 1
        if self.instrument_type != "INDEX" and tick.volume is not None:
            self.current_candle.volume = tick.volume
        if tick.vtt is not None:
            self.current_candle.vtt_close = tick.vtt
        if self.current_candle.volume == 0 and self.instrument_type != "INDEX":
            calculated_volume = int(self.current_candle.vtt_close - self.current_candle.vtt_open)
            self.current_candle.volume = max(0, calculated_volume)
        if (self.config.get("process_delta", False) and self.instrument_type == "FUTURE" and
            self.previous_tick and self.previous_vtt > 0 and self.previous_tick.ltp is not None):
            vtt_change = tick.vtt - self.previous_vtt
            if vtt_change > 0:
                if tick.ltp > self.previous_tick.ltp:
                    self.current_candle.buy_volume += int(vtt_change)
                    self.current_candle.delta += int(vtt_change)
                elif tick.ltp < self.previous_tick.ltp:
                    self.current_candle.sell_volume += int(vtt_change)
                    self.current_candle.delta -= int(vtt_change)
                else:
                    half_volume = int(vtt_change / 2)
                    self.current_candle.buy_volume += half_volume
                    self.current_candle.sell_volume += (int(vtt_change) - half_volume)
                if self.current_candle.delta < self.current_candle.min_delta:
                    self.current_candle.min_delta = self.current_candle.delta
                if self.current_candle.delta > self.current_candle.max_delta:
                    self.current_candle.max_delta = self.current_candle.delta
        elif (self.config.get("process_delta", False) and
              self.previous_tick and self.previous_vtt > 0 and self.previous_tick.ltp is not None):
            vtt_change = tick.vtt - self.previous_vtt
            if vtt_change > 0:
                if tick.ltp > self.previous_tick.ltp:
                    self.current_candle.buy_volume += int(vtt_change)
                elif tick.ltp < self.previous_tick.ltp:
                    self.current_candle.sell_volume += int(vtt_change)
                new_delta = self.current_candle.buy_volume - self.current_candle.sell_volume
                self.current_candle.delta = new_delta
                if new_delta < self.current_candle.min_delta:
                    self.current_candle.min_delta = new_delta
                if new_delta > self.current_candle.max_delta:
                    self.current_candle.max_delta = new_delta
        self.previous_vtt = tick.vtt

    def _finalize_current_candle(self):
        if not self.current_candle:
            return
        try:
            self._process_regular_candle("1min")
            if self.config.get("process_heikin_ashi", False):
                self._process_heikin_ashi("1min")
            self._process_trend_and_recommendation("1min")
            if self.current_5min_candle:
                self._aggregate_to_5min()
                if self.current_minute and (self.current_minute.minute + 1) % 5 == 0:
                    self._finalize_5min_candle()
            self.completed_candles += 1
        except Exception as e:
            logger.error(f"Error finalizing candle: {e}")
        finally:
            self.current_candle = None

    def _aggregate_to_5min(self):
        candle = self.current_candle
        self.min_candles_in_5min += 1
        if self.min_candles_in_5min == 1:
            self.current_5min_candle.open = candle.open
            self.agg['min_delta'] = candle.min_delta
            self.agg['max_delta'] = candle.max_delta
        self.current_5min_candle.close = candle.close
        self.agg['high'] = max(self.agg['high'], candle.high)
        self.agg['low'] = min(self.agg['low'], candle.low)
        self.agg['volume'] += candle.volume
        self.agg['delta'] += candle.delta
        self.agg['min_delta'] = min(self.agg['min_delta'], candle.min_delta)
        self.agg['max_delta'] = max(self.agg['max_delta'], candle.max_delta)
        if candle.volume > 0:
            self.agg['atp_sum'] += candle.atp * candle.volume
            self.agg['atp_volume'] += candle.volume

    def _finalize_5min_candle(self):
        if not self.current_5min_candle:
            return
        try:
            self.current_5min_candle.high = self.agg['high']
            self.current_5min_candle.low = self.agg['low']
            self.current_5min_candle.volume = self.agg['volume']
            self.current_5min_candle.delta = self.agg['delta']
            self.current_5min_candle.min_delta = self.agg['min_delta']
            self.current_5min_candle.max_delta = self.agg['max_delta']
            if self.agg['atp_volume'] > 0:
                self.current_5min_candle.atp = self.agg['atp_sum'] / self.agg['atp_volume']
            else:
                self.current_5min_candle.atp = self.current_5min_candle.close
            self._process_regular_candle("5min", self.current_5min_candle)
            if self.config.get("process_heikin_ashi", False):
                self._process_heikin_ashi("5min", self.current_5min_candle)
            self._process_trend_and_recommendation("5min", self.current_5min_candle)
            self.completed_5min_candles += 1
        except Exception as e:
            logger.error(f"Error finalizing 5min candle: {e}")
        finally:
            self.current_5min_candle = None
            self.current_5min_start = None
            self.min_candles_in_5min = 0

    def _process_regular_candle(self, interval: str, candle: LiveCandle = None):
        candle_to_process = candle if candle else self.current_candle
        if not candle_to_process:
            return
        self.db_manager.save_candle_instant(candle_to_process, interval)
        symbol = self.instrument_key.split('|')[1] if '|' in self.instrument_key else self.instrument_key
        logger.info(f"V3 {interval.upper()} CANDLE [{symbol}] {candle_to_process.timestamp.strftime('%H:%M')} | "
                    f"OHLC: {candle_to_process.open:.2f}/{candle_to_process.high:.2f}/"
                    f"{candle_to_process.low:.2f}/{candle_to_process.close:.2f} | "
                    f"Vol: {candle_to_process.volume} | DELTA: {candle_to_process.delta}")

    def _process_heikin_ashi(self, interval: str, candle: LiveCandle = None):
        candle_to_process = candle if candle else self.current_candle
        if not candle_to_process:
            return
        ha_candle = self._calculate_heikin_ashi_fast(candle_to_process, interval)
        if not ha_candle:
            return
        if interval == "1min":
            if self.sar_calc:
                _, sar_trend = self.sar_calc.update(ha_candle.ha_high, ha_candle.ha_low, ha_candle.ha_close)
                ha_candle.sar_trend = sar_trend
                ha_candle.macd = None
                ha_candle.macd_signal = None
        else:
            if self.sar_calc_5min:
                _, sar_trend = self.sar_calc_5min.update(ha_candle.ha_high, ha_candle.ha_low, ha_candle.ha_close)
                ha_candle.sar_trend = sar_trend
                ha_candle.macd = None
                ha_candle.macd_signal = None
        self.db_manager.save_ha_candle_instant(ha_candle, interval)
        symbol = self.instrument_key.split('|')[1] if '|' in self.instrument_key else self.instrument_key
        logger.info(f"V3 {interval.upper()} HA+INDICATORS [{symbol}] {ha_candle.timestamp.strftime('%H:%M')} | "
                    f"HA Close: {ha_candle.ha_close:.2f} | HLC3: {ha_candle.hlc3:.2f} | "
                    f"SAR: {'UP' if ha_candle.sar_trend == 1 else 'DOWN'}")
        if interval == "1min":
            self.previous_ha_candle = ha_candle
        else:
            self.previous_ha_candle_5min = ha_candle

    def _calculate_heikin_ashi_fast(self, candle: LiveCandle, interval: str) -> Optional[HeikinAshiCandle]:
        try:
            prev_ha = self.previous_ha_candle if interval == "1min" else self.previous_ha_candle_5min
            ha_close = (candle.open + candle.high + candle.low + candle.close) * 0.25
            if prev_ha is None:
                ha_open = (candle.open + candle.close) * 0.5
            else:
                ha_open = (prev_ha.ha_open + prev_ha.ha_close) * 0.5
            ha_high = max(candle.high, ha_open, ha_close)
            ha_low = min(candle.low, ha_open, ha_close)
            hlc3 = (ha_high + ha_low + ha_close) / 3.0
            return HeikinAshiCandle(
                instrument_key=self.instrument_key,
                timestamp=candle.timestamp,
                ha_open=ha_open, ha_high=ha_high, ha_low=ha_low, ha_close=ha_close,
                volume=candle.volume, hlc3=hlc3
            )
        except Exception as e:
            logger.error(f"Error calculating Heikin Ashi: {e}")
            return None

    def _process_trend_and_recommendation(self, interval: str, candle: LiveCandle = None):
        candle_to_process = candle if candle else self.current_candle
        if not candle_to_process:
            return
        nifty_index_proc = self.all_processors.get(INSTRUMENTS["NIFTY_INDEX"]["key"])
        nifty_future_proc = self.all_processors.get(INSTRUMENTS["NIFTY_FUTURE"]["key"])
        if not nifty_index_proc or not nifty_future_proc:
            return
        if interval == "1min":
            index_ha = nifty_index_proc.previous_ha_candle
            future_ha = nifty_future_proc.previous_ha_candle
            future_delta = nifty_future_proc.current_candle.delta if nifty_future_proc.current_candle else 0
            # Use cash flow calculator to get CE/PE metrics instead of individual processors
            ce_delta = pe_delta = 0  # Not needed for trend calculation with cash flow
        else:
            index_ha = nifty_index_proc.previous_ha_candle_5min
            future_ha = nifty_future_proc.previous_ha_candle_5min
            future_delta = nifty_future_proc.current_5min_candle.delta if nifty_future_proc.current_5min_candle else 0
            # Use cash flow calculator to get CE/PE metrics instead of individual processors
            ce_delta = pe_delta = 0  # Not needed for trend calculation with cash flow
        if not index_ha or not future_ha:
            trend_data = {
                'timestamp': candle_to_process.timestamp,
                'candle_interval': interval,
                'trend_value': 0,
                'buy_recommendation': None,
                'entry_price': None,
                'target': None,
                'sl': None,
                'profit_loss': None
            }
            self.db_manager.save_trend_instant(trend_data)
            logger.info(f"TREND [{interval}] {candle_to_process.timestamp.strftime('%H:%M')}: NEUTRAL (HA not available)")
            return
        index_up = index_ha.ha_open < index_ha.ha_close and index_ha.sar_trend == 1
        index_down = index_ha.ha_open > index_ha.ha_close and index_ha.sar_trend == -1
        future_up = future_delta > 0 and future_ha.sar_trend == 1 and future_ha.ha_open < future_ha.ha_close
        future_down = future_delta < 0 and future_ha.sar_trend == -1 and future_ha.ha_open > future_ha.ha_close
        trend_value = 0
        if index_up and future_up:
            trend_value = 1
        elif index_down and future_down:
            trend_value = -1
        if interval == "1min":
            self.previous_trend_1min = self.present_trend_1min
            self.present_trend_1min = trend_value
            previous_trend = self.previous_trend_1min
            present_trend = self.present_trend_1min
            active_trade = self.active_trade_1min
        else:
            self.previous_trend_5min = self.present_trend_5min
            self.present_trend_5min = trend_value
            previous_trend = self.previous_trend_5min
            present_trend = self.present_trend_5min
            active_trade = self.active_trade_5min
        trend_str = "UP" if trend_value == 1 else "DOWN" if trend_value == -1 else "NEUTRAL"
        current_time = candle_to_process.timestamp.time()
        log_msg = f"TREND [{interval}] {current_time.strftime('%H:%M')}: {trend_str}"
        buy_recommendation = None
        entry_price = None
        target = None
        sl = None
        profit_loss = None
        # Buy recommendations will be implemented later with cash flow logic from 60 options
        if active_trade:
            if active_trade['status'] == 'waiting_entry':
                active_trade['entry_price'] = candle_to_process.open
                active_trade['target'] = active_trade['entry_price'] + 5
                active_trade['sl'] = active_trade['entry_price'] - 5
                active_trade['trail_triggered'] = False
                active_trade['status'] = 'active'
                entry_price = active_trade['entry_price']
                target = active_trade['target']
                sl = active_trade['sl']
                log_msg += f" | Entered {active_trade['type']} at {entry_price:.2f} | Target: {target:.2f} | SL: {sl:.2f}"
            elif active_trade['status'] == 'active':
                price = candle_to_process.close
                if not active_trade['trail_triggered'] and price >= active_trade['entry_price'] + 4:
                    active_trade['sl'] = active_trade['entry_price'] + 2
                    active_trade['trail_triggered'] = True
                    sl = active_trade['sl']
                    log_msg += f" | Trailing SL Updated to {sl:.2f} for {active_trade['type']}"
                if price >= active_trade['target']:
                    profit_loss = 4875
                    log_msg += f" | Target Hit: Profit {profit_loss} for {active_trade['type']}"
                    active_trade = None
                elif price <= active_trade['sl']:
                    if active_trade['trail_triggered']:
                        profit_loss = 1950
                        log_msg += f" | Trailing SL Hit: Profit {profit_loss} for {active_trade['type']}"
                    else:
                        profit_loss = -4875
                        log_msg += f" | SL Hit: Loss {profit_loss} for {active_trade['type']}"
                    active_trade = None
        logger.info(log_msg)
        # Generate buy signals based on cash flow for options
        if cash_flow_calculator and interval == "1min":  # Only generate signals on 1min candles
            cash_flow_metrics = cash_flow_calculator.get_current_cash_metrics()
            current_cash = cash_flow_metrics['cash']

            if self.instrument_key == INSTRUMENTS["NIFTY_INDEX"]["key"]:  # Only process buy signals on NIFTY_INDEX
                if current_cash > 0:  # Cash positive = Buy CE
                    itm_options = cash_flow_calculator.get_itm_options(candle_to_process.close)
                    if itm_options['itm_ce']:
                        buy_recommendation = "BUY_CE"
                        entry_price = itm_options['itm_ce']['last_price']
                        target = entry_price + 10  # Fixed target for options
                        sl = entry_price - 5  # Fixed SL for options
                        logger.info(f"🎯 BUY SIGNAL [{interval}]: CE {itm_options['itm_ce']['strike']} @ {entry_price:.2f} | Cash: {current_cash:.4f}")

                elif current_cash < 0:  # Cash negative = Buy PE
                    itm_options = cash_flow_calculator.get_itm_options(candle_to_process.close)
                    if itm_options['itm_pe']:
                        buy_recommendation = "BUY_PE"
                        entry_price = itm_options['itm_pe']['last_price']
                        target = entry_price + 10  # Fixed target for options
                        sl = entry_price - 5  # Fixed SL for options
                        logger.info(f"🎯 BUY SIGNAL [{interval}]: PE {itm_options['itm_pe']['strike']} @ {entry_price:.2f} | Cash: {current_cash:.4f}")

        trend_data = {
            'timestamp': candle_to_process.timestamp,
            'candle_interval': interval,
            'trend_value': trend_value,
            'buy_recommendation': buy_recommendation,
            'entry_price': entry_price,
            'target': target,
            'sl': sl,
            'profit_loss': profit_loss
        }
        self.db_manager.save_trend_instant(trend_data)
        self._update_latest_candle(candle_to_process, interval, trend_data)
        if interval == "1min":
            self.active_trade_1min = active_trade
        else:
            self.active_trade_5min = active_trade

    def _update_latest_candle(self, candle: LiveCandle, interval: str, trend_data: dict):
        try:
            instrument_config = next((config for config in INSTRUMENTS.values() if config["key"] == self.instrument_key), None)
            if not instrument_config:
                return
            instrument_name = self.instrument_key.split('|')[1] if '|' in self.instrument_key else self.instrument_key
            if instrument_config["type"] == "OPTION":
                option_type = instrument_config.get("option_type", "")
                strike_price = instrument_config.get("strike_price", 0)
                instrument_name = f"{instrument_name} {option_type} {strike_price}"
            price_change = candle.close - candle.open
            price_change_pct = (price_change / candle.open * 100) if candle.open > 0 else 0
            vwap = candle.atp if candle.atp > 0 else candle.close
            delta_pct = 0
            if instrument_config["type"] == "OPTION" and candle.delta != 0:
                delta_pct = (candle.delta / candle.volume * 100) if candle.volume > 0 else 0
            latest_candle_data = {
                'instrument_key': self.instrument_key,
                'instrument_name': instrument_name,
                'instrument_type': instrument_config["type"],
                'strike_price': instrument_config.get("strike_price"),
                'option_type': instrument_config.get("option_type"),
                'timestamp': candle.timestamp,
                'open': candle.open,
                'high': candle.high,
                'low': candle.low,
                'close': candle.close,
                'volume': candle.volume,
                'atp': candle.atp,
                'vwap': vwap,
                'price_change': price_change,
                'price_change_pct': price_change_pct,
                'delta': candle.delta,
                'delta_pct': delta_pct,
                'min_delta': candle.min_delta,
                'max_delta': candle.max_delta,
                'buy_volume': candle.buy_volume,
                'sell_volume': candle.sell_volume,
                'tick_count': candle.tick_count,
                'vtt_open': candle.vtt_open,
                'vtt_close': candle.vtt_close,
                'candle_interval': interval,
                'trend_value': trend_data.get('trend_value', 0),
                'buy_recommendation': trend_data.get('buy_recommendation'),
                'entry_price': trend_data.get('entry_price'),
                'target': trend_data.get('target'),
                'sl': trend_data.get('sl'),
                'profit_loss': trend_data.get('profit_loss'),
                'prev_close': getattr(candle, 'prev_close', None) or candle.open,
                'intraday_high': candle.high,
                'intraday_low': candle.low,
                'last_updated': datetime.now(IST)
            }
            self.db_manager.save_latest_candle_instant(latest_candle_data)
        except Exception as e:
            logger.error(f"Error updating latest candle: {e}")

# === ENHANCED OPTION TRACKER WITH SPECIFIC P&L LOGIC ===
class OptionTracker:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.active_positions = {}  # {option_key: position_data}
        self.quantity = 975  # Fixed quantity as specified

    def start_tracking(self, signal_data, option_key):
        """Start tracking a bought option with specific P&L rules"""
        # Get current option price as entry price
        entry_price = self._get_option_current_price(option_key)

        if entry_price <= 0:
            logger.error(f"❌ Cannot track {option_key}: Invalid entry price")
            return

        position = {
            'signal_id': signal_data.get('id'),
            'option_key': option_key,
            'entry_price': entry_price,
            'target': entry_price + 5,      # Entry + 5
            'sl': entry_price - 5,          # Entry - 5
            'trailing_sl': entry_price - 5, # Initial SL
            'trail_triggered': False,
            'quantity': self.quantity,
            'status': 'ACTIVE',
            'entry_time': datetime.now(IST)
        }

        self.active_positions[option_key] = position

        logger.info(f"📊 TRACKING STARTED: {option_key}")
        logger.info(f"   💰 Entry: ₹{entry_price:.2f} | Target: ₹{position['target']:.2f} | SL: ₹{position['sl']:.2f}")
        logger.info(f"   📦 Quantity: {self.quantity}")

    def check_all_positions(self, processors):
        """Check all active positions for target/SL hits"""
        for option_key, position in list(self.active_positions.items()):
            if position['status'] != 'ACTIVE':
                continue

            # Get current option LTP (NOT index/future price)
            current_ltp = self._get_option_current_price(option_key)
            if current_ltp <= 0:
                continue

            # Check for trailing SL trigger (price reaches entry + 4)
            if not position['trail_triggered'] and current_ltp >= (position['entry_price'] + 4):
                position['trailing_sl'] = position['entry_price'] + 2  # Set trailing SL to entry + 2
                position['trail_triggered'] = True
                logger.info(f"🔄 TRAILING SL ACTIVATED: {option_key} @ ₹{current_ltp:.2f}")
                logger.info(f"   📈 New Trailing SL: ₹{position['trailing_sl']:.2f}")

            # Check target hit
            if current_ltp >= position['target']:
                profit = self.quantity * 5  # 975 * 5 = 4,875
                self._close_position(option_key, current_ltp, 'TARGET_HIT', profit)

            # Check stop loss hit
            elif current_ltp <= position['trailing_sl']:
                if position['trail_triggered']:
                    profit = self.quantity * 2  # 975 * 2 = 1,950 (trailing SL profit)
                    self._close_position(option_key, current_ltp, 'TRAILING_SL_HIT', profit)
                else:
                    loss = self.quantity * -5  # 975 * -5 = -4,875 (original SL loss)
                    self._close_position(option_key, current_ltp, 'SL_HIT', loss)

    def _get_option_current_price(self, option_key):
        """Get current LTP of specific option"""
        global processors

        # Get the processor for this option
        processor = processors.get(option_key)
        if not processor:
            logger.error(f"❌ No processor found for option: {option_key}")
            return 0

        # Get current candle LTP (NO ATR calculation needed)
        if hasattr(processor, 'current_candle') and processor.current_candle:
            return processor.current_candle.close

        logger.warning(f"⚠️ No current price available for option: {option_key}")
        return 0

    def _close_position(self, option_key, exit_price, exit_reason, pnl):
        """Close position and record P&L"""
        position = self.active_positions[option_key]

        # Update position
        position['status'] = exit_reason
        position['exit_price'] = exit_price
        position['pnl'] = pnl
        position['exit_time'] = datetime.now(IST)

        # Calculate holding time
        holding_time = (position['exit_time'] - position['entry_time']).total_seconds() / 60

        # Save to database
        tracking_data = {
            'signal_id': position['signal_id'],
            'timestamp': position['exit_time'].isoformat(),
            'current_price': exit_price,
            'pnl': pnl,
            'status': exit_reason
        }

        conn = sqlite3.connect(TRADING_DB, timeout=10.0)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO option_tracking (signal_id, timestamp, current_price, pnl, status)
            VALUES (?, ?, ?, ?, ?)
        """, (
            tracking_data['signal_id'],
            tracking_data['timestamp'],
            tracking_data['current_price'],
            tracking_data['pnl'],
            tracking_data['status']
        ))

        # Update buy_signals table with final P&L
        cursor.execute("""
            UPDATE buy_signals
            SET status = ?, entry_price = ?, target = ?, sl = ?
            WHERE id = ?
        """, (
            exit_reason,
            position['entry_price'],
            position['target'],
            position['trailing_sl'],
            position['signal_id']
        ))

        conn.commit()
        conn.close()

        logger.info(f"🎯 {exit_reason}: {option_key}")
        logger.info(f"   💰 Entry: ₹{position['entry_price']:.2f} → Exit: ₹{exit_price:.2f}")
        logger.info(f"   📊 P&L: ₹{pnl:.2f} | Holding: {holding_time:.1f} min")

        # Remove from active tracking
        del self.active_positions[option_key]

# === ENHANCED BUY/SELL SIGNAL GENERATOR ===
class BuySignalGenerator:
    def __init__(self, db_manager, cash_flow_calculator):
        self.db_manager = db_manager
        self.cash_flow_calculator = cash_flow_calculator
        self.active_signals = {}
        self.previous_trend = 0  # Track previous trend for change detection
        self.last_signal_time = None
        self.signal_cooldown = 60  # 60 seconds between signals

    def check_and_generate_signals(self, nifty_price, current_trend, interval='1min'):
        """Enhanced signal generation with trend change detection and interval-specific thresholds"""
        cash_metrics = self.cash_flow_calculator.get_current_cash_metrics()
        current_cash = cash_metrics['cash']
        min_cash = cash_metrics['min_cash']
        max_cash = cash_metrics['max_cash']

        # Set thresholds based on interval
        if interval == '1min':
            buy_threshold = 100000  # ±100K for 1-minute signals
        else:  # 5min
            buy_threshold = 500000  # ±500K for 5-minute signals

        # Check for cooldown period
        current_time = datetime.now(IST)
        if (self.last_signal_time and
            (current_time - self.last_signal_time).total_seconds() < self.signal_cooldown):
            return

        # Detect trend change (only for 1-minute signals to avoid duplicates)
        if interval == '1min':
            trend_changed = self.previous_trend != current_trend

            if trend_changed:
                logger.info(f"🔄 TREND CHANGE DETECTED: {self.previous_trend} → {current_trend}")

                # SELL SIGNAL LOGIC based on trend change and cash position
                if self.previous_trend == 1 and current_trend == -1:
                    # Trend changed from POSITIVE to NEGATIVE
                    self._check_sell_signal_positive_to_negative(current_cash, min_cash, max_cash, nifty_price)

                elif self.previous_trend == -1 and current_trend == 1:
                    # Trend changed from NEGATIVE to POSITIVE
                    self._check_sell_signal_negative_to_positive(current_cash, min_cash, max_cash, nifty_price)

                # NEUTRAL TRANSITION SIGNALS (BUY TERMS)
                elif self.previous_trend == 1 and current_trend == 0:
                    # Trend changed from POSITIVE to NEUTRAL
                    self._check_neutral_signal_positive_to_neutral(current_cash, min_cash, max_cash, nifty_price)

                elif self.previous_trend == -1 and current_trend == 0:
                    # Trend changed from NEGATIVE to NEUTRAL
                    self._check_neutral_signal_negative_to_neutral(current_cash, min_cash, max_cash, nifty_price)

        # BUY SIGNAL LOGIC with dynamic thresholds
        if current_cash > buy_threshold:  # Positive cash = CE signal
            itm_options = self.cash_flow_calculator.get_itm_options(nifty_price)
            if itm_options['itm_ce']:
                self._generate_ce_signal(itm_options['itm_ce'], current_cash, 'BUY', interval)

        elif current_cash < -buy_threshold:  # Negative cash = PE signal
            itm_options = self.cash_flow_calculator.get_itm_options(nifty_price)
            if itm_options['itm_pe']:
                self._generate_pe_signal(itm_options['itm_pe'], current_cash, 'BUY', interval)

        # Update previous trend for next comparison (only for 1-minute)
        if interval == '1min':
            self.previous_trend = current_trend

    def _check_sell_signal_positive_to_negative(self, current_cash, min_cash, max_cash, nifty_price):
        """Check sell signal when trend changes from positive to negative"""
        # Check if cash is negative AND near minimum (not maximum)
        if current_cash < 0:
            # Calculate distance from min and max
            distance_from_min = abs(current_cash - min_cash)
            distance_from_max = abs(current_cash - max_cash)

            # Cash should be closer to min than to max
            if distance_from_min <= distance_from_max:
                # Generate SELL signal for PE (since cash is negative and near min)
                itm_options = self.cash_flow_calculator.get_itm_options(nifty_price)
                if itm_options['itm_pe']:
                    self._generate_pe_signal(itm_options['itm_pe'], current_cash, 'SELL')
                    logger.info(f"🔴 SELL PE SIGNAL: Trend +→- | Cash: {current_cash:.0f} (near min: {min_cash:.0f})")

    def _check_sell_signal_negative_to_positive(self, current_cash, min_cash, max_cash, nifty_price):
        """Check sell signal when trend changes from negative to positive"""
        # Check if cash is positive AND near maximum (not minimum)
        if current_cash > 0:
            # Calculate distance from min and max
            distance_from_min = abs(current_cash - min_cash)
            distance_from_max = abs(current_cash - max_cash)

            # Cash should be closer to max than to min
            if distance_from_max <= distance_from_min:
                # Generate SELL signal for CE (since cash is positive and near max)
                itm_options = self.cash_flow_calculator.get_itm_options(nifty_price)
                if itm_options['itm_ce']:
                    self._generate_ce_signal(itm_options['itm_ce'], current_cash, 'SELL')
                    logger.info(f"🟢 SELL CE SIGNAL: Trend -→+ | Cash: {current_cash:.0f} (near max: {max_cash:.0f})")

    def _generate_ce_signal(self, ce_option, cash, action_type):
        """Generate CE signal (BUY or SELL)"""
        signal_data = {
            'timestamp': datetime.now(IST).isoformat(),
            'signal_type': f'{action_type}_CE',
            'option_key': ce_option['instrument_key'],
            'strike': ce_option['strike'],
            'cash_flow': cash,
            'status': 'ACTIVE',
            'action': action_type
        }

        # Save to database
        conn = sqlite3.connect(TRADING_DB, timeout=10.0)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO buy_signals (timestamp, signal_type, option_key, strike, entry_price, target, sl, status, cash_flow)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            signal_data['timestamp'],
            signal_data['signal_type'],
            signal_data['option_key'],
            signal_data['strike'],
            0,  # Entry price
            0,  # Target
            0,  # Stop Loss
            signal_data['status'],
            signal_data['cash_flow']
        ))
        conn.commit()
        conn.close()

        self.last_signal_time = datetime.now(IST)
        logger.info(f"🟢 {action_type} CE SIGNAL: {ce_option['strike']} @ Cash: {cash:.2f}")

    def _generate_pe_signal(self, pe_option, cash, action_type):
        """Generate PE signal (BUY or SELL)"""
        signal_data = {
            'timestamp': datetime.now(IST).isoformat(),
            'signal_type': f'{action_type}_PE',
            'option_key': pe_option['instrument_key'],
            'strike': pe_option['strike'],
            'cash_flow': cash,
            'status': 'ACTIVE',
            'action': action_type
        }

        # Save to database
        conn = sqlite3.connect(TRADING_DB, timeout=10.0)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO buy_signals (timestamp, signal_type, option_key, strike, entry_price, target, sl, status, cash_flow)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            signal_data['timestamp'],
            signal_data['signal_type'],
            signal_data['option_key'],
            signal_data['strike'],
            0,  # Entry price
            0,  # Target
            0,  # Stop Loss
            signal_data['status'],
            signal_data['cash_flow']
        ))
        conn.commit()
        conn.close()

        self.last_signal_time = datetime.now(IST)
        logger.info(f"🔴 {action_type} PE SIGNAL: {pe_option['strike']} @ Cash: {cash:.2f}")

    def _check_neutral_signal_positive_to_neutral(self, current_cash, min_cash, max_cash, nifty_price):
        """Check neutral transition signal when trend changes from positive to neutral"""
        # From positive trend to neutral: Check if cash is negative and near maximum
        if current_cash < 0:
            distance_from_min = abs(current_cash - min_cash)
            distance_from_max = abs(current_cash - max_cash)

            # Cash should be closer to max than to min
            if distance_from_max <= distance_from_min:
                itm_options = self.cash_flow_calculator.get_itm_options(nifty_price)
                if itm_options['itm_pe']:
                    self._generate_pe_signal(itm_options['itm_pe'], current_cash, 'BUY')
                    logger.info(f"🔵 NEUTRAL SIGNAL [+→0]: BUY PE {itm_options['itm_pe']['strike']} | Cash: {current_cash:.0f} (near max: {max_cash:.0f})")

    def _check_neutral_signal_negative_to_neutral(self, current_cash, min_cash, max_cash, nifty_price):
        """Check neutral transition signal when trend changes from negative to neutral"""
        # From negative trend to neutral: Check if cash is positive and near minimum
        if current_cash > 0:
            distance_from_min = abs(current_cash - min_cash)
            distance_from_max = abs(current_cash - max_cash)

            # Cash should be closer to min than to max
            if distance_from_min <= distance_from_max:
                itm_options = self.cash_flow_calculator.get_itm_options(nifty_price)
                if itm_options['itm_ce']:
                    self._generate_ce_signal(itm_options['itm_ce'], current_cash, 'BUY')
                    logger.info(f"🔵 NEUTRAL SIGNAL [-→0]: BUY CE {itm_options['itm_ce']['strike']} | Cash: {current_cash:.0f} (near min: {min_cash:.0f})")

# === OPTIONS CASH FLOW CALCULATOR ===
class OptionsTickCashFlowCalculator:
    """Cash flow calculator for options based on VTT changes"""
    def __init__(self, selected_options_df):
        self.options = {row['instrument_key']: row for idx, row in selected_options_df.iterrows()}
        # 1-minute data
        self.cash = 0.0
        self.min_cash = float('inf')
        self.max_cash = float('-inf')
        self.last_ltp = {}
        self.last_vtt = {}
        self.current_minute = None
        self.open = None
        self.high = float('-inf')
        self.low = float('inf')
        self.close = None
        # 5-minute aggregated data
        self.cash_5min = 0.0
        self.min_cash_5min = float('inf')
        self.max_cash_5min = float('-inf')
        self.current_5min_start = None
        logger.info("✅ Cash Flow Calculator initialized (1-min & 5-min)")

    def process_option_tick(self, instrument_key, ltp, vtt, timestamp):
        """Process individual option tick and calculate cash flow using VTT changes"""
        if instrument_key not in self.options:
            return
        # Check for minute boundary
        minute = timestamp.replace(second=0, microsecond=0)
        if self.current_minute != minute:
            if self.current_minute:
                self._save_minute_data()
            self._reset_minute(minute)
        # Get previous values
        prev_ltp = self.last_ltp.get(instrument_key, ltp)
        prev_vtt = self.last_vtt.get(instrument_key, vtt)
        vtt_change = vtt - prev_vtt
        # Only process when VTT changes (VTT detection)
        if vtt_change > 0:
            ltp_change = ltp - prev_ltp
            option_type = self.options.get(instrument_key, {}).get('option_type', 'N/A')
            if option_type == 'CE':
                if ltp_change > 0:  # CE buy side
                    cash_change = ltp * vtt_change
                    self.cash += cash_change
                elif ltp_change < 0:  # CE sell side
                    cash_change = ltp * vtt_change
                    self.cash -= cash_change
            elif option_type == 'PE':
                if ltp_change > 0:  # PE buy side
                    cash_change = ltp * vtt_change
                    self.cash -= cash_change
                elif ltp_change < 0:  # PE sell side
                    cash_change = ltp * vtt_change
                    self.cash += cash_change
            # Update min/max tracking
            if self.cash < self.min_cash:
                self.min_cash = self.cash
            if self.cash > self.max_cash:
                self.max_cash = self.cash
        # Update previous values
        self.last_ltp[instrument_key] = ltp
        self.last_vtt[instrument_key] = vtt

    def update_nifty_tick(self, timestamp, price):
        """Update NIFTY index OHLC for the minute"""
        minute = timestamp.replace(second=0, microsecond=0)
        if self.current_minute != minute:
            if self.current_minute:
                self._save_minute_data()
            self._reset_minute(minute)
        if self.open is None:
            self.open = price
        self.close = price
        self.high = max(self.high, price)
        self.low = min(self.low, price)

    def _reset_minute(self, minute):
        """Reset values for new minute"""
        self.current_minute = minute
        self.cash = 0.0
        self.min_cash = 0.0 if self.min_cash == float('inf') else self.min_cash
        self.max_cash = 0.0 if self.max_cash == float('-inf') else self.max_cash
        self.open = None
        self.high = float('-inf')
        self.low = float('inf')
        self.close = None

        # Handle 5-minute bucket reset and aggregation
        _5min_start = minute.replace(minute=(minute.minute // 5) * 5, second=0, microsecond=0)
        if self.current_5min_start != _5min_start:
            if self.current_5min_start:
                self._aggregate_to_5min()  # Aggregate cash before saving
                self._save_5min_data()
            self._reset_5min(_5min_start)

    def _aggregate_to_5min(self):
        """Aggregate 1-minute cash flow to 5-minute bucket"""
        # Accumulate cash from the current minute to the 5-minute bucket
        self.cash_5min += self.cash
        # Update min and max for the 5-minute period
        if self.min_cash < self.min_cash_5min:
            self.min_cash_5min = self.min_cash
        if self.max_cash > self.max_cash_5min:
            self.max_cash_5min = self.max_cash

    def _reset_5min(self, _5min_start):
        """Reset 5-minute bucket"""
        self.current_5min_start = _5min_start
        self.cash_5min = 0.0
        self.min_cash_5min = float('inf')
        self.max_cash_5min = float('-inf')

    def _save_5min_data(self):
        """Save 5-minute aggregated cash flow data"""
        if not self.current_5min_start:
            return
        try:
            conn = sqlite3.connect(TRADING_DB, timeout=10.0)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO options_cash_flow (timestamp, interval_type, cash, min_cash, max_cash, total_options) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    self.current_5min_start.strftime('%Y-%m-%d %H:%M:%S'),
                    '5min',
                    round(self.cash_5min, 4),
                    round(self.min_cash_5min, 4),
                    round(self.max_cash_5min, 4),
                    len(self.options)
                )
            )
            conn.commit()
            conn.close()
            logger.debug(f"Saved 5-min cash flow: {self.cash_5min:.4f} at {self.current_5min_start}")
        except Exception as e:
            logger.error(f"Database error in 5-min cash flow save: {e}")

    def _save_minute_data(self):
        """Save current minute data to database"""
        if not self.current_minute or self.open is None:
            return
        try:
            # Direct database connection using TRADING_DB path
            conn = sqlite3.connect(TRADING_DB, timeout=10.0)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO options_cash_flow (timestamp, interval_type, cash, min_cash, max_cash, total_options) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    self.current_minute.strftime('%Y-%m-%d %H:%M:%S'),
                    '1min',
                    round(self.cash, 4),
                    round(self.min_cash, 4),
                    round(self.max_cash, 4),
                    len(self.options)
                )
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Database error in cash flow save: {e}")
            logger.error(f"Cash data: timestamp={self.current_minute}, cash={self.cash}")

    def get_current_cash_metrics(self):
        """Get current cash flow metrics"""
        return {
            'cash': self.cash,
            'min_cash': self.min_cash if self.min_cash != float('inf') else 0.0,
            'max_cash': self.max_cash if self.max_cash != float('-inf') else 0.0
        }

    def get_itm_options(self, nifty_price):
        """Get 1st ITM CE and PE based on current NIFTY price"""
        ce_options = [(key, row) for key, row in self.options.items()
                     if row['option_type'] == 'CE' and row['strike'] < nifty_price]
        pe_options = [(key, row) for key, row in self.options.items()
                     if row['option_type'] == 'PE' and row['strike'] > nifty_price]

        # 1st ITM CE = highest strike below NIFTY
        itm_ce = max(ce_options, key=lambda x: x[1]['strike']) if ce_options else None

        # 1st ITM PE = lowest strike above NIFTY
        itm_pe = min(pe_options, key=lambda x: x[1]['strike']) if pe_options else None

        return {
            'itm_ce': itm_ce[1] if itm_ce else None,
            'itm_pe': itm_pe[1] if itm_pe else None
        }

# === MAIN WEBSOCKET CONNECTION MANAGER ===
async def websocket_v3_connection_manager():
    global db_manager, processors, cash_flow_calculator, buy_signal_generator
    db_manager = LockFreeDatabaseManager()

    # ENHANCED TOKEN VALIDATION - Wait for daily token update before starting pipeline
    daily_token_updated = False
    today = datetime.now(IST).date()

    while not shutdown_event.is_set():
        config = load_config()
        ACCESS_TOKEN = config.get('ACCESS_TOKEN', '')
        token_update_date = config.get('TOKEN_UPDATE_DATE', '')

        # Check if token was updated today
        if (token_update_date == today.isoformat() and
            ACCESS_TOKEN and
            ACCESS_TOKEN != "update-daily-in-admin-panel"):
            daily_token_updated = True
            logger.info(f"✅ Daily token validated for {today}")
            break
        else:
            current_time = datetime.now(IST).time()
            if current_time >= dt_time(9, 15):  # After market open
                logger.warning(f"⚠️ Market opened but token not updated for {today}")
                logger.info("💡 Update token at: https://trendvision2004.com/admin/login")
            else:
                logger.info(f"⏳ Waiting for daily token update for {today}")
            await asyncio.sleep(30)  # Check every 30 seconds

    if not daily_token_updated:
        logger.error("❌ Daily token not updated - Pipeline stopped")
        return

    # Initialize cash flow calculator with selected options
    selected_options, _ = auto_select_options('extracted_data.csv')
    if not selected_options.empty:
        cash_flow_calculator = OptionsTickCashFlowCalculator(selected_options)
        # Initialize buy signal generator
        buy_signal_generator = BuySignalGenerator(db_manager, cash_flow_calculator)
        # Initialize option tracker
        option_tracker = OptionTracker(db_manager)
        logger.info(f"✅ Cash flow calculator initialized with {len(selected_options)} options")
        logger.info("✅ Buy signal generator initialized")
        logger.info("✅ Option tracker initialized")
    else:
        cash_flow_calculator = None
        buy_signal_generator = None
        option_tracker = None
        logger.warning("❌ No options selected - cash flow calculator and signal generator not initialized")

    processors = {}
    for name, config in INSTRUMENTS.items():
        processors[config["key"]] = LockFreeTickProcessor(config["key"], config, db_manager, processors)
    logger.info(f"Lock-free processors ready for {len(processors)} instruments")
    connection_attempts = 0
    max_attempts = 5
    retry_delay = 1  # Initial retry delay for exponential backoff
    while not shutdown_event.is_set() and connection_attempts < max_attempts:
        # Watch time: If outside hours, wait and check every minute
        while not shutdown_event.is_set() and not is_market_hours():
            now = datetime.now(IST)
            logger.info(f"Outside market hours (IST {now.strftime('%H:%M:%S')}) - Watching for open. Next check in 1 min.")
            await asyncio.sleep(60)
            if now.weekday() >= 5:  # Skip weekends
                logger.info("Weekend - Pausing watch until Monday")
                await asyncio.sleep(3600)  # Check hourly on weekends
        try:
            connection_attempts += 1
            logger.info(f"V3 WebSocket connection attempt {connection_attempts}")
            auth_response = get_market_data_feed_authorize_v3()
            if not auth_response or 'data' not in auth_response:
                logger.error("Failed to get V3 WebSocket authorization - Check access token")
                await asyncio.sleep(5)
                continue
            ws_url = auth_response['data']['authorized_redirect_uri']
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            async with websockets.connect(
                ws_url,
                ssl=ssl_context,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
                max_queue=None,
                max_size=None,
                compression=None
            ) as websocket:
                logger.info("V3 WebSocket connected successfully")
                connection_attempts = 0
                retry_delay = 1  # Reset backoff
                await asyncio.sleep(1)
                subscription_msg = create_v3_subscription_message(INSTRUMENT_KEYS, "full")
                await websocket.send(subscription_msg)
                logger.info(f"V3 Subscribed to {len(INSTRUMENT_KEYS)} instruments - Check keys if no data")
                message_count = 0
                last_stats_time = time.time()
                while not shutdown_event.is_set():
                    now = datetime.now(IST).time()
                    logger.debug(f"Current IST time: {now.strftime('%H:%M:%S')} | Market open: {is_market_hours()}")
                    if now >= MARKET_END_TIME:
                        logger.info("🕒 Market close at 3:30 PM IST - Initiating shutdown")
                        shutdown_event.set()
                        for proc in processors.values():
                            if proc.current_candle:
                                proc._finalize_current_candle()
                            if proc.current_5min_candle:
                                proc._finalize_5min_candle()
                        break
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                        if not message:
                            continue
                        decoded_data = decode_v3_message(message)
                        if not decoded_data:
                            continue
                        if 'feeds' in decoded_data:
                            # Process NIFTY index tick for OHLC and cash flow calculator
                            if INSTRUMENTS["NIFTY_INDEX"]["key"] in decoded_data['feeds']:
                                nifty_feed = decoded_data['feeds'][INSTRUMENTS["NIFTY_INDEX"]["key"]]
                                nifty_tick = None
                                if 'fullFeed' in nifty_feed:
                                    feed = nifty_feed['fullFeed']
                                    if 'marketFF' in feed or 'indexFF' in feed:
                                        ltpc = feed.get('indexFF', {}).get('ltpc') or feed.get('marketFF', {}).get('ltpc')
                                        if ltpc:
                                            timestamp = processors[INSTRUMENTS["NIFTY_INDEX"]["key"]].decode_timestamp(ltpc.get('ltt'))
                                            ltp = float(ltpc.get('ltp', 0))
                                            if timestamp and ltp > 0:
                                                if cash_flow_calculator:
                                                    cash_flow_calculator.update_nifty_tick(timestamp, ltp)

                            # Process all feeds including options for cash flow calculator
                            for key in decoded_data['feeds']:
                                if key in processors:
                                    processors[key].process_tick(decoded_data)

                                    # If this is an option, also process through cash flow calculator
                                    if cash_flow_calculator:
                                        instrument_config = next((config for config in INSTRUMENTS.values() if config["key"] == key), None)
                                        if instrument_config and instrument_config["type"] == "OPTION":
                                            feed_data = decoded_data['feeds'][key]
                                            tick = processors[key].extract_tick_data_v3(decoded_data)
                                            if tick:
                                                cash_flow_calculator.process_option_tick(
                                                    key, tick.ltp, tick.vtt, tick.timestamp
                                                )

                        # Generate buy signals based on cash flow every minute
                        if buy_signal_generator and cash_flow_calculator:
                            nifty_processor = processors.get(INSTRUMENTS["NIFTY_INDEX"]["key"])
                            if nifty_processor and hasattr(nifty_processor, 'current_candle') and nifty_processor.current_candle:
                                current_nifty_price = nifty_processor.current_candle.close
                                current_trend = nifty_processor.present_trend_1min  # Get current trend
                                buy_signal_generator.check_and_generate_signals(current_nifty_price, current_trend)
                        message_count += 1
                        current_time = time.time()
                        if current_time - last_stats_time > 60:
                            total_ticks = sum(p.processed_ticks for p in processors.values())
                            candle_q, ha_q, trend_q, latest_q = db_manager.get_queue_sizes()
                            logger.info(f"LOCK-FREE Stats: Messages: {message_count} | Ticks: {total_ticks} | Queues: C={candle_q}, HA={ha_q}, Trend={trend_q}, Latest={latest_q}")
                            last_stats_time = current_time
                    except asyncio.TimeoutError:
                        logger.warning("No messages received in 10 seconds - Check token/keys or market data flow")
                    except websockets.ConnectionClosed as e:
                        logger.warning(f"WebSocket closed: {e} - Reconnecting")
                        break
                    except Exception as e:
                        logger.error(f"Error in receive loop: {e}")
        except Exception as e:
            logger.error(f"Connection error (attempt {connection_attempts}): {e}")
            if not shutdown_event.is_set():
                logger.info(f"Reconnecting in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)  # Exponential backoff, max 60s
    if processors:
        total_ticks = sum(p.processed_ticks for p in processors.values())
        total_1m = sum(p.completed_candles for p in processors.values())
        total_5m = sum(p.completed_5min_candles for p in processors.values())
        logger.info(f"LOCK-FREE Final Stats: Ticks: {total_ticks} | 1m: {total_1m} | 5m: {total_5m}")
    db_manager.shutdown()

def is_market_hours():
    now = datetime.now(IST)
    is_open = MARKET_START_TIME <= now.time() < MARKET_END_TIME and now.weekday() < 5
    logger.debug(f"Market hours check: {is_open} (IST {now.strftime('%H:%M:%S')})")
    return is_open

# === MAIN ENTRY POINT ===
async def main_async():
    global shutdown_event
    shutdown_event = asyncio.Event()
    def signal_handler(signum, frame):
        logger.info(f"Signal {signum} received, shutting down")
        shutdown_event.set()
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    except ValueError:
        logger.info("Running in thread - signal handlers not available")
    await websocket_v3_connection_manager()

def main():
    logger.info("🏆 UPSTOX LOCK-FREE TRADING SYSTEM V3 - CASH FLOW DRIVEN")
    logger.info("=" * 80)
    logger.info("🎯 Key Features:")
    logger.info("   ✨ 60 Dynamic Options Selection & Cash Flow Calculation")
    logger.info("   ✨ No MACD - SAR Only Trend Analysis")
    logger.info("   ✨ Real-time Buy Signals (CE/PE based on cash flow)")
    logger.info("   ✨ Lock-free Database Architecture")
    logger.info("   ✨ Production Ready for Mumbai VM")
    logger.info("=" * 80)

    # Show instrument summary
    instrument_summary = {}
    for name, config in INSTRUMENTS.items():
        instrument_type = config["type"]
        if instrument_type not in instrument_summary:
            instrument_summary[instrument_type] = 0
        instrument_summary[instrument_type] += 1

    for instrument_type, count in instrument_summary.items():
        logger.info(f" • {instrument_type}: {count}")

    logger.info("=" * 80)
    logger.info("📊 Data Flow: WebSocket → Tick Processing → Cash Flow → Buy Signals → Dashboard")
    logger.info("=" * 80)

    try:
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("⏹️  Keyboard interrupt detected - Graceful shutdown")
    finally:
        logger.info("✅ System terminated cleanly")
        logger.info(f"💾 Database: {TRADING_DB}")
