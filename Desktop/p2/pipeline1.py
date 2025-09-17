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
from zoneinfo import ZoneInfo  # For IST
from concurrent.futures import ThreadPoolExecutor
import queue

load_dotenv()

# Import the V3 protobuf
import MarketDataFeedV3_pb2 as pb

# Constants and Configurations
CONFIG_PATH = 'config/config.json'
UPSTOX_V3_AUTH_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
TRADING_DB = os.getenv('TRADING_DB', 'database/upstox_v3_live_trading.db')
BACKUP_DB_PATH = os.getenv('BACKUP_DB', 'database/upstox_v3_backup.db')

def load_config():
    """Dynamic config loading with auto-recovery"""
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = json.load(f)
        logger.info("✅ Config loaded successfully")
        return config
    except FileNotFoundError:
        logger.warning("⚠️ Config file not found, creating default config")
        return create_default_config()
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON in config file: {e}")
        return create_default_config()

def create_default_config():
    """Create default config with minimal settings"""
    default_config = {
        "ACCESS_TOKEN": "",
        "NIFTY_FUTURE_key": "NSE_FO|5001",
        "ITM_CE_key": "NSE_FO|40577",
        "ITM_CE_strike": 24500,
        "ITM_PE_key": "NSE_FO|40580",
        "ITM_PE_strike": 24550
    }
    try:
        os.makedirs('config', exist_ok=True)
        with open(CONFIG_PATH, 'w') as f:
            json.dump(default_config, f, indent=2)
        logger.info("✅ Default config created successfully")
    except Exception as e:
        logger.error(f"❌ Failed to create default config: {e}")
    return default_config

def reload_instrument_config():
    """Reload instrument configuration from config file"""
    config = load_config()
    global INSTRUMENTS
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
    logger.info("🔄 Instrument configuration reloaded")

# Load initial config
reload_instrument_config()
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
        "key": config.get('ITM_CE_key', "NSE_FO|40577"),  # Update this for different CE strike
        "type": "OPTION",
        "option_type": "CE",
        "strike_price": config.get('ITM_CE_strike', 24500),  # Easy strike configuration
        "has_volume": True,
        "process_delta": True,
        "process_heikin_ashi": False,
        "process_indicators": False,
        "trading_enabled": True,
        "table_suffix": "ce_option"
    },
    "ITM_PE": {
        "key": config.get('ITM_PE_key', "NSE_FO|40580"),  # Update this for different PE strike
        "type": "OPTION",
        "option_type": "PE",
        "strike_price": config.get('ITM_PE_strike', 24550),  # Easy strike configuration
        "has_volume": True,
        "process_delta": True,
        "process_heikin_ashi": False,
        "process_indicators": False,
        "trading_enabled": True,
        "table_suffix": "pe_option"
    }
}

INSTRUMENT_KEYS = [cfg["key"] for cfg in INSTRUMENTS.values()]

# Technical Indicator Settings
SAR_START = 0.4
SAR_INCREMENT = 0.3
SAR_MAXIMUM = 0.7
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# Time Configuration (all in IST)
IST = ZoneInfo("Asia/Kolkata")
MARKET_START_TIME = dt_time(9, 15)  # 9:15 AM IST
MARKET_END_TIME = dt_time(15, 30)  # 3:30 PM IST
TRADING_START_TIME = dt_time(10, 0)  # 10:00 AM IST - Buy recommendations start
NO_NEW_TRADES_TIME = dt_time(15, 0)  # 3:00 PM IST - No new trade recommendations

# Optimized Database Settings
DB_BATCH_SIZE = 50
DB_COMMIT_INTERVAL = 0.5  # seconds (faster for multiprocessing)
MAX_WORKERS = 8  # More workers for multiprocessing

# Multiprocessing Settings
RECEIVE_QUEUE_SIZE = 10000  # Large queue for inter-process communication
PROCESSING_QUEUE_SIZE = 10000  # Separate processing queue
CPU_COUNT = min(mp.cpu_count(), 4)  # Use up to 4 cores but minimum 2

# Global variables
shutdown_event = None
db_manager = None
processors = {}

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

# === ULTRA-FAST OPTIMIZED DATABASE MANAGER WITH PRE-CREATED TABLES ===
class UltraFastDatabaseManager:
    def __init__(self):
        self.live_db_path = TRADING_DB
        self.running = True

        # Ensure database directory exists
        os.makedirs(os.path.dirname(self.live_db_path), exist_ok=True)

        # Multiple fast queues for different data types
        self.candle_queue = queue.Queue()  # Non-blocking queue
        self.ha_queue = queue.Queue()
        self.trend_queue = queue.Queue()
        self.latest_candle_queue = queue.Queue()

        # Pre-create all tables at startup
        self._pre_create_all_tables()

        # Multiple database connections for parallel processing
        self.db_connections = []
        for i in range(MAX_WORKERS):
            conn = self._create_optimized_connection()
            self.db_connections.append(conn)

        # Thread pool for parallel database operations
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="DBWorker")

        # Start background commit scheduler
        self.commit_scheduler = threading.Thread(target=self._scheduled_commit, daemon=True)
        self.commit_scheduler.start()

        logger.info("Ultra-fast database manager initialized")

    def _create_optimized_connection(self):
        """Create highly optimized SQLite connection"""
        conn = sqlite3.connect(
            self.live_db_path,
            timeout=60.0,
            detect_types=sqlite3.PARSE_DECLTYPES,
            check_same_thread=False,
            isolation_level=None  # Auto-commit mode for speed
        )

        # Ultra-fast settings
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA locking_mode = NORMAL")
        conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA mmap_size = 268435456")  # 256MB mmap
        conn.execute("PRAGMA page_size = 8192")  # Larger page size
        conn.execute("PRAGMA wal_autocheckpoint = 1000")
        conn.execute("PRAGMA busy_timeout = 30000")  # 30 second timeout
        conn.execute("PRAGMA optimize")  # Run optimize

        return conn

    def _pre_create_all_tables(self):
        """Pre-create all possible tables to eliminate runtime delays"""
        try:
            # Create directory if needed
            os.makedirs(os.path.dirname(self.live_db_path), exist_ok=True)

            # Temporary connection for pre-creation
            temp_conn = self._create_optimized_connection()
            temp_cursor = temp_conn.cursor()

            # Create registry table
            temp_cursor.execute('''
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
            temp_cursor.execute('''
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

            # Create latest_candles table
            temp_cursor.execute('''
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

            # Pre-create tables for all instruments and timeframes
            today = datetime.now(IST).date()
            intervals = ['1min', '5min']

            for interval in intervals:
                for instrument_name, config in INSTRUMENTS.items():
                    data_types = []
                    if config.get("process_heikin_ashi", False):
                        data_types.append(f"heikin_ashi{'5' if interval == '5min' else ''}")
                    data_types.append(f"candles{'5' if interval == '5min' else ''}")

                    for data_type in data_types:
                        table_name = self._get_table_name(config["key"], data_type, today)
                        self._create_table_if_not_exists(temp_cursor, table_name, config, data_type, interval)

            # Create indexes for performance
            temp_cursor.execute('CREATE INDEX IF NOT EXISTS idx_trend_timestamp ON trend(timestamp)')
            temp_cursor.execute('CREATE INDEX IF NOT EXISTS idx_trend_interval ON trend(candle_interval)')
            temp_cursor.execute('CREATE INDEX IF NOT EXISTS idx_latest_candles_instrument ON latest_candles(instrument_key)')
            temp_cursor.execute('CREATE INDEX IF NOT EXISTS idx_latest_candles_updated ON latest_candles(updated_at)')

            temp_conn.commit()
            temp_conn.close()

            logger.info("All database tables pre-created successfully")

        except Exception as e:
            logger.error(f"Error pre-creating tables: {e}")
            raise

    def _create_table_if_not_exists(self, cursor, table_name: str, instrument_config: dict, data_type: str, interval: str):
        """Create specific table with optimized schema"""
        try:
            is_index = instrument_config["type"] == "INDEX"
            volume_default = " DEFAULT 0" if is_index else ""

            if "candle" in data_type:
                # Create candles table
                cursor.execute(f'''
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
                cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp)')

            elif "heikin_ashi" in data_type:
                # Create heikin ashi table
                cursor.execute(f'''
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
                cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp)')

            # Register table
            cursor.execute('''
            INSERT OR IGNORE INTO table_registry
            (table_name, instrument_key, data_type, trade_date)
            VALUES (?, ?, ?, ?)
            ''', (table_name, instrument_config["key"], data_type, datetime.now(IST).date()))

        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")

    def _get_table_name(self, instrument_key: str, data_type: str, trade_date: date = None) -> str:
        if trade_date is None:
            trade_date = datetime.now(IST).date()
        instrument_suffix = "unknown"
        for name, config in INSTRUMENTS.items():
            if config["key"] == instrument_key:
                instrument_suffix = config["table_suffix"]
                break
        date_str = trade_date.strftime("%Y%m%d")
        return f"{data_type}_{instrument_suffix}_{date_str}"

    def _scheduled_commit(self):
        """Scheduled commit to ensure data persistence"""
        while self.running:
            try:
                time.sleep(DB_COMMIT_INTERVAL)
                # Process pending batches in parallel
                futures = []
                if not self.candle_queue.empty():
                    futures.append(self.executor.submit(self._process_candle_batch_parallel))
                if not self.ha_queue.empty():
                    futures.append(self.executor.submit(self._process_ha_batch_parallel))
                if not self.trend_queue.empty():
                    futures.append(self.executor.submit(self._process_trend_batch_parallel))
                if not self.latest_candle_queue.empty():
                    futures.append(self.executor.submit(self._process_latest_candle_batch_parallel))

                # Wait for all to complete (but don't block main thread)
                for future in futures:
                    try:
                        future.result(timeout=0.1)  # Non-blocking wait
                    except:
                        pass  # Continue if timeout

            except Exception as e:
                logger.error(f"Scheduled commit error: {e}")

    def _process_candle_batch_parallel(self):
        """Process candle batches in parallel"""
        try:
            batch = []
            # Collect batch (non-blocking)
            while len(batch) < DB_BATCH_SIZE and not self.candle_queue.empty():
                try:
                    batch.append(self.candle_queue.get_nowait())
                except queue.Empty:
                    break

            if not batch:
                return

            # Distribute to available connections
            conn = self.db_connections[len(batch) % len(self.db_connections)]
            cursor = conn.cursor()

            table_batches = {}
            for candle, interval in batch:
                data_type = f"candles{'5' if interval == '5min' else ''}"
                table_name = self._get_table_name(candle.instrument_key, data_type)
                if table_name not in table_batches:
                    table_batches[table_name] = []
                table_batches[table_name].append(candle)

            # Batch insert per table
            for table_name, candles in table_batches.items():
                insert_data = []
                for candle in candles:
                    insert_data.append((
                        candle.instrument_key, candle.timestamp, candle.open, candle.high,
                        candle.low, candle.close, candle.volume, candle.atp,
                        candle.delta, candle.min_delta, candle.max_delta,
                        candle.buy_volume, candle.sell_volume, candle.tick_count,
                        candle.vtt_open, candle.vtt_close
                    ))

                cursor.executemany(
                    f'''
                    INSERT OR REPLACE INTO {table_name}
                    (instrument_key, timestamp, open, high, low, close, volume, atp,
                    delta, min_delta, max_delta, buy_volume, sell_volume, tick_count, vtt_open, vtt_close)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    insert_data
                )

        except Exception as e:
            logger.error(f"Error processing candle batch: {e}")

    def _process_ha_batch_parallel(self):
        """Process Heikin Ashi batches in parallel"""
        try:
            batch = []
            while len(batch) < DB_BATCH_SIZE and not self.ha_queue.empty():
                try:
                    batch.append(self.ha_queue.get_nowait())
                except queue.Empty:
                    break

            if not batch:
                return

            conn = self.db_connections[len(batch) % len(self.db_connections)]
            cursor = conn.cursor()

            table_batches = {}
            for ha_candle, interval in batch:
                data_type = f"heikin_ashi{'5' if interval == '5min' else ''}"
                table_name = self._get_table_name(ha_candle.instrument_key, data_type)
                if table_name not in table_batches:
                    table_batches[table_name] = []
                table_batches[table_name].append(ha_candle)

            for table_name, ha_candles in table_batches.items():
                insert_data = []
                for ha_candle in ha_candles:
                    insert_data.append((
                        ha_candle.instrument_key, ha_candle.timestamp,
                        ha_candle.ha_open, ha_candle.ha_high, ha_candle.ha_low, ha_candle.ha_close,
                        ha_candle.volume, ha_candle.hlc3,
                        ha_candle.sar_trend, ha_candle.macd, ha_candle.macd_signal
                    ))

                cursor.executemany(f'''
                    INSERT OR REPLACE INTO {table_name}
                    (instrument_key, timestamp, ha_open, ha_high, ha_low, ha_close,
                    volume, hlc3, sar_trend, macd, macd_signal)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', insert_data)

        except Exception as e:
            logger.error(f"Error processing HA batch: {e}")

    def _process_trend_batch_parallel(self):
        """Process trend batches in parallel"""
        try:
            batch = []
            while len(batch) < DB_BATCH_SIZE and not self.trend_queue.empty():
                try:
                    batch.append(self.trend_queue.get_nowait())
                except queue.Empty:
                    break

            if not batch:
                return

            conn = self.db_connections[len(batch) % len(self.db_connections)]
            cursor = conn.cursor()

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

            cursor.executemany('''
                INSERT INTO trend
                (timestamp, candle_interval, trend_value, buy_recommendation, entry_price, target, sl, profit_loss)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', insert_data)

        except Exception as e:
            logger.error(f"Error processing trend batch: {e}")

    def _process_latest_candle_batch_parallel(self):
        """Process latest candle batches in parallel"""
        try:
            batch = []
            while len(batch) < DB_BATCH_SIZE and not self.latest_candle_queue.empty():
                try:
                    batch.append(self.latest_candle_queue.get_nowait())
                except queue.Empty:
                    break

            if not batch:
                return

            conn = self.db_connections[len(batch) % len(self.db_connections)]
            cursor = conn.cursor()

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
                    candle_data.get('low'),
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

            cursor.executemany('''
                INSERT OR REPLACE INTO latest_candles
                (instrument_key, instrument_name, instrument_type, strike_price, option_type,
                timestamp, open, high, low, close, volume, atp, vwap, price_change, price_change_pct,
                delta, delta_pct, min_delta, max_delta, buy_volume, sell_volume, tick_count,
                vtt_open, vtt_close, candle_interval, trend_value, buy_recommendation,
                entry_price, target, sl, profit_loss, prev_close, intraday_high, intraday_low, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', insert_data)

        except Exception as e:
            logger.error(f"Error processing latest candle batch: {e}")

    def save_candle_ultra_fast(self, candle: LiveCandle, interval: str = "1min"):
        """Ultra-fast candle saving with no blocking"""
        try:
            self.candle_queue.put_nowait((candle, interval))
        except queue.Full:
            logger.warning("Candle queue full - dropping oldest")
            try:
                self.candle_queue.get_nowait()  # Remove oldest
                self.candle_queue.put_nowait((candle, interval))
            except:
                pass

    def save_ha_ultra_fast(self, ha_candle: HeikinAshiCandle, interval: str = "1min"):
        """Ultra-fast HA saving with no blocking"""
        try:
            self.ha_queue.put_nowait((ha_candle, interval))
        except queue.Full:
            try:
                self.ha_queue.get_nowait()
                self.ha_queue.put_nowait((ha_candle, interval))
            except:
                pass

    def save_trend_ultra_fast(self, trend_data: dict):
        """Ultra-fast trend saving with no blocking"""
        try:
            self.trend_queue.put_nowait(trend_data)
        except queue.Full:
            try:
                self.trend_queue.get_nowait()
                self.trend_queue.put_nowait(trend_data)
            except:
                pass

    def save_latest_candle_ultra_fast(self, candle_data: dict):
        """Ultra-fast latest candle saving with no blocking"""
        try:
            self.latest_candle_queue.put_nowait(candle_data)
        except queue.Full:
            try:
                self.latest_candle_queue.get_nowait()
                self.latest_candle_queue.put_nowait(candle_data)
            except:
                pass

    def get_queue_sizes(self):
        """Get current queue sizes for monitoring"""
        return (
            self.candle_queue.qsize(),
            self.ha_queue.qsize(),
            self.trend_queue.qsize(),
            self.latest_candle_queue.qsize()
        )

    def shutdown(self):
        """Clean shutdown"""
        self.running = False
        time.sleep(0.1)  # Allow pending operations to complete

        # Close all connections
        for conn in self.db_connections:
            try:
                conn.close()
            except:
                pass

        # Shutdown executor
        self.executor.shutdown(wait=True)
        logger.info("Database manager shut down cleanly")

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

class SimpleMacd:
    def __init__(self, fast_period=MACD_FAST, slow_period=MACD_SLOW, signal_period=MACD_SIGNAL):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.fast_ema = None
        self.slow_ema = None
        self.signal_ema = None

    def calculate_ema(self, current_price, previous_ema, period):
        alpha = 2.0 / (period + 1)
        if previous_ema is None:
            return current_price
        return alpha * current_price + (1 - alpha) * previous_ema

    def update(self, hlc3_price: float) -> Tuple[float, float]:
        try:
            self.fast_ema = self.calculate_ema(hlc3_price, self.fast_ema, self.fast_period)
            self.slow_ema = self.calculate_ema(hlc3_price, self.slow_ema, self.slow_period)
            macd_line = self.fast_ema - self.slow_ema
            self.signal_ema = self.calculate_ema(macd_line, self.signal_ema, self.signal_period)
            return macd_line, self.signal_ema
        except Exception:
            return 0.0, 0.0

# === ULTRA-FAST OPTIMIZED TICK PROCESSOR ===
class UltraFastTickProcessor:
    def __init__(self, instrument_key: str, config: dict, db_manager: UltraFastDatabaseManager, all_processors: dict):
        self.instrument_key = instrument_key
        self.config = config
        self.db_manager = db_manager
        self.all_processors = all_processors
        self.instrument_type = config["type"]

        # Optimized state variables - use memory-efficient structures
        self.current_candle = None
        self.current_minute = None
        self.previous_tick = None
        self.previous_vtt = 0.0

        # 5-min optimized state
        self.current_5min_candle = None
        self.current_5min_start = None
        self.min_candles_in_5min = 0
        self.agg_stats = {}

        # Heikin Ashi optimized state
        self.prev_ha_candle = None
        self.prev_ha_candle_5min = None

        # Fast indicators (created only if needed)
        if self.config.get("process_indicators", False):
            self.sar_calc = FastSAR()
            self.macd_calc = SimpleMacd()
            self.sar_calc_5min = FastSAR()
            self.macd_calc_5min = SimpleMacd()

        # Trend and trade state (per timeframe)
        self.previous_trend_1min = 0
        self.present_trend_1min = 0
        self.previous_trend_5min = 0
        self.present_trend_5min = 0
        self.active_trade_1min = None
        self.active_trade_5min = None

        # Statistics
        self.processed_ticks = 0
        self.completed_candles = 0
        self.completed_5min_candles = 0

        logger.info(f"Ultra-fast processor initialized for {instrument_key}")

    def extract_tick_data_ultra_fast(self, feed_data) -> Optional[LiveTick]:
        """Ultra-fast tick extraction with minimal overhead"""
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
                    timestamp_str = ltpc.get('ltt')

                    if not timestamp_str:
                        return None

                    # Optimized timestamp parsing
                    timestamp_ms = int(timestamp_str)
                    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                    timestamp = dt_utc.astimezone(IST)

                    ltp = float(ltpc.get('ltp', 0))
                    atp = float(market_ff.get('atp', ltp))
                    vtt = full_feed.get('vtt', market_ff.get('vtt'))

                    # Volume extraction
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
                    timestamp_str = ltpc.get('ltt')

                    if not timestamp_str:
                        return None

                    timestamp_ms = int(timestamp_str)
                    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                    timestamp = dt_utc.astimezone(IST)

                    ltp = float(ltpc.get('ltp', 0))
                    return LiveTick(
                        instrument_key=self.instrument_key,
                        instrument_type=self.instrument_type,
                        timestamp=timestamp,
                        ltp=ltp,
                        atp=ltp,
                        prev_close=float(ltpc.get('cp', 0))
                    )

            # Handle LTPC only - simplified path
            elif 'ltpc' in instrument_feed:
                ltpc = instrument_feed['ltpc']
                timestamp_str = ltpc.get('ltt')

                if not timestamp_str:
                    return None

                timestamp_ms = int(timestamp_str)
                dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                timestamp = dt_utc.astimezone(IST)

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
        """Ultra-fast minute calculation"""
        adjusted_time = timestamp - timedelta(seconds=1)
        return adjusted_time.replace(second=0, microsecond=0)

    def get_5min_bucket(self, timestamp: datetime) -> datetime:
        """Ultra-fast 5-minute bucket calculation"""
        bucket_minute = (timestamp.minute // 5) * 5
        return timestamp.replace(minute=bucket_minute, second=0, microsecond=0)

    def process_tick_ultra_fast(self, feed_data):
        """Main tick processing with zero delays"""
        try:
            tick = self.extract_tick_data_ultra_fast(feed_data)
            if not tick:
                return

            current_time = tick.timestamp.time()

            # Early exit if outside market hours
            if current_time >= MARKET_END_TIME:
                logger.info("Market close reached - finalizing candles")
                if self.current_candle:
                    self._finalize_current_candle_ultra_fast()
                if self.current_5min_candle:
                    self._finalize_5min_candle_ultra_fast()
                global shutdown_event
                shutdown_event.set()
                return

            candle_minute = self.get_candle_minute(tick.timestamp)

            # Handle minute transitions
            if self.current_minute and self.current_minute != candle_minute:
                self._finalize_current_candle_ultra_fast()

            if self.current_minute != candle_minute:
                self._initialize_candle_ultra_fast(candle_minute, tick)

            # Update candle data ultra-fast
            self._update_candle_ultra_lightning(tick)

            # Process trading signals
            self._manage_active_trades_ultra_fast(tick)

            self.processed_ticks += 1

        except Exception as e:
            logger.error(f"Error processing tick: {e}")

    def _manage_active_trades_ultra_fast(self, tick: LiveTick):
        """Ultra-fast trade management"""
        current_time = tick.timestamp.time()
        if self.active_trade_1min:
            result = self._check_trade_exit_ultra_fast(self.active_trade_1min, tick.ltp, current_time)
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
                self.db_manager.save_trend_ultra_fast(trend_data)

        if self.active_trade_5min:
            result = self._check_trade_exit_ultra_fast(self.active_trade_5min, tick.ltp, current_time)
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
                self.db_manager.save_trend_ultra_fast(trend_data)

    def _check_trade_exit_ultra_fast(self, trade, current_price, current_time):
        """Ultra-fast trade exit check"""
        if trade['status'] != 'active':
            return None

        if not trade.get('trail_triggered') and current_price >= trade['entry_price'] + 4:
            trade['sl'] = trade['entry_price'] + 2
            trade['trail_triggered'] = True
            logger.info(f"Trailing SL shifted to {trade['sl']:.2f}")

        if current_price >= trade['target']:
            profit_loss = 4875
            logger.info(f"🎯 Target Hit: Profit {profit_loss}")
            trade['status'] = 'completed'
            trade['profit_loss'] = profit_loss
            return trade
        elif current_price <= trade['sl']:
            if trade.get('trail_triggered'):
                profit_loss = 1950
                logger.info(f"📉 Trailing SL Hit: Profit {profit_loss}")
            else:
                profit_loss = -4875
                logger.info(f"📉 SL Hit: Loss {profit_loss}")
            trade['status'] = 'completed'
            trade['profit_loss'] = profit_loss
            return trade
        return None

    def _initialize_candle_ultra_fast(self, minute: datetime, tick: LiveTick):
        """Ultra-fast candle initialization"""
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

        # Initialize 5-min candle if needed
        _5min_start = self.get_5min_bucket(minute)
        if self.current_5min_start != _5min_start:
            self._initialize_5min_candle_ultra_fast(_5min_start, tick)

    def _initialize_5min_candle_ultra_fast(self, _5min_start: datetime, tick: LiveTick):
        """Ultra-fast 5-min candle initialization"""
        self.current_5min_start = _5min_start
        self.min_candles_in_5min = 0
        self.agg_stats = {
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

    def _update_candle_ultra_lightning(self, tick: LiveTick):
        """Lightning-fast candle updates with minimal checks"""
        if not self.current_candle:
            return

        # Skip duplicate updates (most common case)
        if (self.previous_tick and
            tick.ltp == self.previous_tick.ltp and
            tick.vtt == self.previous_tick.vtt):
            return

        # Update OHLC
        self.current_candle.close = tick.ltp
        if tick.ltp > self.current_candle.high:
            self.current_candle.high = tick.ltp
        elif tick.ltp < self.current_candle.low:
            self.current_candle.low = tick.ltp

        self.current_candle.atp = tick.atp or tick.ltp
        self.current_candle.tick_count += 1

        # Volume update
        if self.instrument_type != "INDEX" and tick.volume is not None:
            self.current_candle.volume = tick.volume

        if tick.vtt is not None:
            self.current_candle.vtt_close = tick.vtt

        # Calculated volume if needed
        if self.current_candle.volume == 0 and self.instrument_type != "INDEX":
            calculated_volume = int(self.current_candle.vtt_close - self.current_candle.vtt_open)
            self.current_candle.volume = max(0, calculated_volume)

        # Delta processing only if configured
        if (self.config.get("process_delta", False) and self.instrument_type == "FUTURE"):
            if (self.previous_tick and self.previous_vtt > 0 and self.previous_tick.ltp is not None):
                vtt_change = tick.vtt - self.previous_vtt
                if vtt_change > 0:
                    if tick.ltp > self.previous_tick.ltp:
                        self.current_candle.delta += int(vtt_change)
                        self.current_candle.buy_volume += int(vtt_change)
                    elif tick.ltp < self.previous_tick.ltp:
                        self.current_candle.delta -= int(vtt_change)
                        self.current_candle.sell_volume += int(vtt_change)
                    else:
                        half_volume = int(vtt_change / 2)
                        self.current_candle.buy_volume += half_volume
                        self.current_candle.sell_volume += (int(vtt_change) - half_volume)

                # Track min/max delta
                if self.current_candle.delta < self.current_candle.min_delta:
                    self.current_candle.min_delta = self.current_candle.delta
                elif self.current_candle.delta > self.current_candle.max_delta:
                    self.current_candle.max_delta = self.current_candle.delta

        self.previous_vtt = tick.vtt
        self.previous_tick = tick

    def _finalize_current_candle_ultra_fast(self):
        """Ultra-fast candle finalization"""
        if not self.current_candle:
            return

        try:
            # Process regular candle
            self._process_regular_candle_ultra_fast("1min")

            # Process Heikin Ashi if configured
            if self.config.get("process_heikin_ashi", False):
                self._process_heikin_ashi_ultra_fast("1min")

            # Process trend and recommendations
            self._process_trend_and_recommendation_ultra_fast("1min")

            # Aggregate to 5-min
            if self.current_5min_candle:
                self._aggregate_to_5min_ultra_fast()
                if self.current_minute and (self.current_minute.minute + 1) % 5 == 0:
                    self._finalize_5min_candle_ultra_fast()

            self.completed_candles += 1

        except Exception as e:
            logger.error(f"Error finalizing candle: {e}")
        finally:
            self.current_candle = None

    def _aggregate_to_5min_ultra_fast(self):
        """Ultra-fast 5-min aggregation"""
        candle = self.current_candle
        self.min_candles_in_5min += 1

        if self.min_candles_in_5min == 1:
            self.current_5min_candle.open = candle.open
            self.agg_stats['min_delta'] = candle.min_delta
            self.agg_stats['max_delta'] = candle.max_delta

        self.current_5min_candle.close = candle.close
        self.agg_stats['high'] = max(self.agg_stats['high'], candle.high)
        self.agg_stats['low'] = min(self.agg_stats['low'], candle.low)
        self.agg_stats['volume'] += candle.volume
        self.agg_stats['delta'] += candle.delta
        self.agg_stats['min_delta'] = min(self.agg_stats['min_delta'], candle.min_delta)
        self.agg_stats['max_delta'] = max(self.agg_stats['max_delta'], candle.max_delta)

        if candle.volume > 0:
            self.agg_stats['atp_sum'] += candle.atp * candle.volume
            self.agg_stats['atp_volume'] += candle.volume

    def _finalize_5min_candle_ultra_fast(self):
        """Ultra-fast 5-min candle finalization"""
        if not self.current_5min_candle:
            return

        try:
            self.current_5min_candle.high = self.agg_stats['high']
            self.current_5min_candle.low = self.agg_stats['low']
            self.current_5min_candle.volume = self.agg_stats['volume']
            self.current_5min_candle.delta = self.agg_stats['delta']
            self.current_5min_candle.min_delta = self.agg_stats['min_delta']
            self.current_5min_candle.max_delta = self.agg_stats['max_delta']

            if self.agg_stats['atp_volume'] > 0:
                self.current_5min_candle.atp = self.agg_stats['atp_sum'] / self.agg_stats['atp_volume']
            else:
                self.current_5min_candle.atp = self.current_5min_candle.close

            # Process all analyses for 5-min
            self._process_regular_candle_ultra_fast("5min", self.current_5min_candle)

            if self.config.get("process_heikin_ashi", False):
                self._process_heikin_ashi_ultra_fast("5min", self.current_5min_candle)

            self._process_trend_and_recommendation_ultra_fast("5min", self.current_5min_candle)

            self.completed_5min_candles += 1

        except Exception as e:
            logger.error(f"Error finalizing 5min candle: {e}")
        finally:
            self.current_5min_candle = None
            self.current_5min_start = None
            self.min_candles_in_5min = 0

    def _process_regular_candle_ultra_fast(self, interval: str, candle: LiveCandle = None):
        """Ultra-fast regular candle processing"""
        candle_to_process = candle if candle else self.current_candle
        if not candle_to_process:
            return

        # Save candle to database (non-blocking)
        self.db_manager.save_candle_ultra_fast(candle_to_process, interval)

        # Log only every 10th candle to reduce I/O
        if self.completed_candles % 10 == 0:
            symbol = self.instrument_key.split('|')[1] if '|' in self.instrument_key else self.instrument_key
            logger.info(f"✅ {interval.upper()} CANDLE [{symbol}] {candle_to_process.timestamp.strftime('%H:%M')} | "
                        f"OHLC: {candle_to_process.open:.2f}/{candle_to_process.high:.2f}/"
                        f"{candle_to_process.low:.2f}/{candle_to_process.close:.2f} | "
                        f"Vol: {candle_to_process.volume} | DELTA: {candle_to_process.delta}")

    def _process_heikin_ashi_ultra_fast(self, interval: str, candle: LiveCandle = None):
        """Ultra-fast Heikin Ashi processing"""
        candle_to_process = candle if candle else self.current_candle
        if not candle_to_process:
            return

        ha_candle = self._calculate_heikin_ashi_ultra_fast(candle_to_process, interval)
        if not ha_candle:
            return

        # Apply indicators
        if interval == "1min":
            if self.sar_calc and self.macd_calc:
                _, sar_trend = self.sar_calc.update(ha_candle.ha_high, ha_candle.ha_low, ha_candle.ha_close)
                macd, macd_signal = self.macd_calc.update(ha_candle.hlc3)
                ha_candle.sar_trend = sar_trend
                ha_candle.macd = macd
                ha_candle.macd_signal = macd_signal
        else:
            if self.sar_calc_5min and self.macd_calc_5min:
                _, sar_trend = self.sar_calc_5min.update(ha_candle.ha_high, ha_candle.ha_low, ha_candle.ha_close)
                macd, macd_signal = self.macd_calc_5min.update(ha_candle.hlc3)
                ha_candle.sar_trend = sar_trend
                ha_candle.macd = macd
                ha_candle.macd_signal = macd_signal

        # Save HA candle (non-blocking)
        self.db_manager.save_ha_ultra_fast(ha_candle, interval)

        # Log only significant indicators
        if ha_candle.sar_trend and ha_candle.macd and ha_candle.macd_signal:
            symbol = self.instrument_key.split('|')[1] if '|' in self.instrument_key else self.instrument_key
            logger.info(f"📊 {interval.upper()} HA [{symbol}] {ha_candle.timestamp.strftime('%H:%M')} | "
                        f"HA Close: {ha_candle.ha_close:.2f} | HLC3: {ha_candle.hlc3:.2f} | "
                        f"SAR: {'UP' if ha_candle.sar_trend == 1 else 'DOWN'} | "
                        f"MACD: {ha_candle.macd:.4f}")

        # Update previous HA candle
        if interval == "1min":
            self.prev_ha_candle = ha_candle
        else:
            self.prev_ha_candle_5min = ha_candle

    def _calculate_heikin_ashi_ultra_fast(self, candle: LiveCandle, interval: str) -> Optional[HeikinAshiCandle]:
        """Ultra-fast Heikin Ashi calculation"""
        try:
            prev_ha = self.prev_ha_candle if interval == "1min" else self.prev_ha_candle_5min

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

    def _process_trend_and_recommendation_ultra_fast(self, interval: str, candle: LiveCandle = None):
        """Ultra-fast trend and recommendation processing"""
        candle_to_process = candle if candle else self.current_candle
        if not candle_to_process:
            return

        # Get other processors for multi-symbol analysis
        nifty_index_proc = self.all_processors.get(INSTRUMENTS["NIFTY_INDEX"]["key"])
        nifty_future_proc = self.all_processors.get(INSTRUMENTS["NIFTY_FUTURE"]["key"])
        itm_ce_proc = self.all_processors.get(INSTRUMENTS["ITM_CE"]["key"])
        itm_pe_proc = self.all_processors.get(INSTRUMENTS["ITM_PE"]["key"])

        if not nifty_index_proc or not nifty_future_proc:
            return

        # Get HA candles from other symbols
        if interval == "1min":
            index_ha = nifty_index_proc.prev_ha_candle
            future_ha = nifty_future_proc.prev_ha_candle
            future_delta = nifty_future_proc.current_candle.delta if nifty_future_proc.current_candle else 0
            ce_delta = itm_ce_proc.current_candle.delta if itm_ce_proc and itm_ce_proc.current_candle else 0
            pe_delta = itm_pe_proc.current_candle.delta if itm_pe_proc and itm_pe_proc.current_candle else 0
        else:
            index_ha = nifty_index_proc.prev_ha_candle_5min
            future_ha = nifty_future_proc.prev_ha_candle_5min
            future_delta = nifty_future_proc.current_5min_candle.delta if nifty_future_proc.current_5min_candle else 0
            ce_delta = itm_ce_proc.current_5min_candle.delta if itm_ce_proc and itm_ce_proc.current_5min_candle else 0
            pe_delta = itm_pe_proc.current_5min_candle.delta if itm_pe_proc and itm_pe_proc.current_5min_candle else 0

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
            self.db_manager.save_trend_ultra_fast(trend_data)
            return

        # Calculate trend
        index_up = index_ha.ha_open < index_ha.ha_close and index_ha.sar_trend == 1
        index_down = index_ha.ha_open > index_ha.ha_close and index_ha.sar_trend == -1
        future_up = future_delta > 0 and future_ha.sar_trend == 1 and future_ha.ha_open < future_ha.ha_close
        future_down = future_delta < 0 and future_ha.sar_trend == -1 and future_ha.ha_open > future_ha.ha_close

        trend_value = 0
        if index_up and future_up:
            trend_value = 1
        elif index_down and future_down:
            trend_value = -1

        # Update trend state
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

        # Trading logic
        trend_str = "UP" if trend_value == 1 else "DOWN" if trend_value == -1 else "NEUTRAL"
        current_time = candle_to_process.timestamp.time()
        buy_recommendation = None
        entry_price = None
        target = None
        sl = None

        can_recommend = TRADING_START_TIME <= current_time < NO_NEW_TRADES_TIME
        if can_recommend and not active_trade:
            macd = index_ha.macd if index_ha.macd is not None else 0
            macd_signal = index_ha.macd_signal if index_ha.macd_signal is not None else 0

            if (macd > macd_signal and (previous_trend in (-1, 0)) and present_trend == 1 and ce_delta > 0 and pe_delta < 0):
                buy_recommendation = "Buy CALL Option"
                active_trade = {
                    'type': 'CALL',
                    'status': 'waiting_entry',
                    'entry_time': candle_to_process.timestamp
                }
            elif (macd < macd_signal and (previous_trend in (1, 0)) and present_trend == -1 and ce_delta < 0 and pe_delta > 0):
                buy_recommendation = "Buy PUT Option"
                active_trade = {
                    'type': 'PUT',
                    'status': 'waiting_entry',
                    'entry_time': candle_to_process.timestamp
                }

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
                logger.info(f"📈 Entered {active_trade['type']} at {entry_price:.2f} | Target: {target:.2f} | SL: {sl:.2f}")
            elif active_trade['status'] == 'active':
                price = candle_to_process.close
                if not active_trade['trail_triggered'] and price >= active_trade['entry_price'] + 4:
                    active_trade['sl'] = active_trade['entry_price'] + 2
                    active_trade['trail_triggered'] = True
                    sl = active_trade['sl']
                    logger.info(f"🛡️ Trailing SL Updated to {sl:.2f}")
                if price >= active_trade['target']:
                    profit_loss = 4875
                    logger.info(f"🎯 Target Hit: Profit {profit_loss}")
                    active_trade = None
                elif price <= active_trade['sl']:
                    if active_trade['trail_triggered']:
                        profit_loss = 1950
                        logger.info(f"📉 Trailing SL Hit: Profit {profit_loss}")
                    else:
                        profit_loss = -4875
                        logger.info(f"📉 SL Hit: Loss {profit_loss}")
                    active_trade = None

        logger.info(f"📊 TREND [{interval}] {current_time.strftime('%H:%M')}: {trend_str}")

        trend_data = {
            'timestamp': candle_to_process.timestamp,
            'candle_interval': interval,
            'trend_value': trend_value,
            'buy_recommendation': buy_recommendation,
            'entry_price': entry_price,
            'target': target,
            'sl': sl,
            'profit_loss': None  # This gets set in trade exit
        }

        self.db_manager.save_trend_ultra_fast(trend_data)
        self._update_latest_candle_ultra_fast(candle_to_process, interval, trend_data)

        if interval == "1min":
            self.active_trade_1min = active_trade
        else:
            self.active_trade_5min = active_trade

    def _update_latest_candle_ultra_fast(self, candle: LiveCandle, interval: str, trend_data: dict):
        """Ultra-fast latest candle update"""
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

            self.db_manager.save_latest_candle_ultra_fast(latest_candle_data)

        except Exception as e:
            logger.error(f"Error updating latest candle: {e}")

# === OPTIMIZED V3 API FUNCTIONS ===
def get_market_data_feed_authorize_v3():
    """Optimized V3 authorization with better error handling"""
    try:
        headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
        response = requests.get(UPSTOX_V3_AUTH_URL, headers=headers, timeout=5.0)
        if response.status_code == 200:
            logger.info("✅ V3 WebSocket URL retrieved successfully")
            return response.json()
        else:
            logger.error(f"❌ Failed to get V3 WebSocket URL: HTTP {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"❌ Error getting V3 WebSocket URL: {e}")
        return None

def create_v3_subscription_message(instrument_keys: List[str], mode: str = "full"):
    """Create optimized V3 subscription message"""
    try:
        data = {
            "guid": "upstox-v3-ultra-fast-system",
            "method": "sub",
            "data": {"mode": mode, "instrumentKeys": instrument_keys}
        }
        return json.dumps(data).encode('utf-8')
    except Exception as e:
        logger.error(f"❌ Error creating V3 subscription message: {e}")
        return b""

def decode_v3_message(data):
    """Ultra-fast V3 message decoding"""
    try:
        if isinstance(data, bytes):
            try:
                response = pb.FeedResponse()
                response.ParseFromString(data)
                return MessageToDict(response)
            except Exception as e:
                logger.debug(f"Protobuf decode failed, trying JSON: {e}")
                try:
                    return json.loads(data.decode('utf-8'))
                except Exception as e:
                    logger.debug(f"JSON decode failed: {e}")
                    return None
        else:
            try:
                return json.loads(data)
            except Exception as e:
                logger.debug(f"JSON decode failed: {e}")
                return None
        return None
    except Exception as e:
        logger.error(f"❌ Error decoding V3 message: {e}")
        return None

# === DUAL-CPU PROCESS ARCHITECTURE ===
def websocket_receiver_process(receive_queue: MPQueue, shutdown_event: MPEvent):
    """CPU 1: WebSocket Receiver Process - Handles all incoming data"""
    logger.info(f"🔄 CPU 1: Starting WebSocket Receiver Process (PID: {os.getpid()})")

    async def websocket_receiver():
        connection_attempts = 0
        max_attempts = 5

        while not shutdown_event.is_set() and connection_attempts < max_attempts:
            # Market hours check
            if not is_market_hours_v3():
                now = datetime.now(IST)
                logger.info(f"⏰ Outside market hours - Next check in 1 min (IST {now.strftime('%H:%M:%S')})")
                await asyncio.sleep(60)
                continue

            try:
                connection_attempts += 1
                logger.info(f"🔌 V3 WebSocket connection attempt {connection_attempts}")

                auth_response = get_market_data_feed_authorize_v3()
                if not auth_response or 'data' not in auth_response:
                    logger.error("❌ Failed to get V3 WebSocket authorization")
                    await asyncio.sleep(5)
                    continue

                ws_url = auth_response['data']['authorized_redirect_uri']

                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                async with websockets.connect(
                    ws_url,
                    ssl=ssl_context,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                    max_queue=None,
                    max_size=2**20
                ) as websocket:
                    logger.info("✅ V3 WebSocket connected successfully")
                    connection_attempts = 0

                    await asyncio.sleep(1)

                    subscription_msg = create_v3_subscription_message(INSTRUMENT_KEYS, "full")
                    await websocket.send(subscription_msg)
                    logger.info(f"📡 V3 Subscribed to {len(INSTRUMENT_KEYS)} instruments")

                    message_count = 0
                    stats_time = time.time()

                    while not shutdown_event.is_set():
                        now = datetime.now(IST).time()

                        # Market close check
                        if now >= MARKET_END_TIME:
                            logger.info("🏁 Market close reached - Sending final message")
                            # Send shutdown signal to processing process
                            receive_queue.put({"shutdown": True, "market_close": True}, timeout=5)
                            shutdown_event.set()
                            break

                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                            if not message:
                                continue

                            decoded_data = decode_v3_message(message)
                            if decoded_data and 'feeds' in decoded_data:
                                # Send data to processing process (non-blocking)
                                try:
                                    receive_queue.put(decoded_data, timeout=0.1)
                                    message_count += 1
                                except:
                                    logger.warning("Queue full - dropping message to prevent blocking")

                            current_time = time.time()
                            if current_time - stats_time > 10:  # Frequent stats
                                now_str = datetime.now(IST).strftime('%H:%M:%S')
                                logger.info(f"📡 CPU 1 Stats: Messages received: {message_count} | "
                                           f"Queue size: {receive_queue.qsize()} | Time: {now_str}")
                                stats_time = current_time

                        except asyncio.TimeoutError:
                            logger.warning("⏰ No messages in 5 seconds - checking connection")
                        except websockets.ConnectionClosed as e:
                            logger.warning(f"🔌 WebSocket closed: {e} - reconnecting")
                            break
                        except Exception as e:
                            logger.error(f"❌ Error in receiver loop: {e}")

            except Exception as e:
                logger.error(f"🔌 Connection error (attempt {connection_attempts}): {e}")
                if not shutdown_event.is_set():
                    await asyncio.sleep(min(2 ** connection_attempts, 60))

    # Run the async receiver
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(websocket_receiver())
    logger.info("🛑 CPU 1: WebSocket Receiver Process terminated")

def data_processor_process(receive_queue: MPQueue, shutdown_event: MPEvent):
    """CPU 2: Data Processing Process - Handles all data processing and database operations"""
    logger.info(f"⚡ CPU 2: Starting Data Processing Process (PID: {os.getpid()})")

    try:
        # Set CPU affinity for this process if possible
        try:
            import psutil
            p = psutil.Process(os.getpid())
            cpu_count = min(mp.cpu_count(), 4)
            # Assign different CPU core to this process
            p.cpu_affinity([1 % cpu_count])  # CPU 1 for processing
        except:
            pass

        # Initialize database manager with optimized settings for this CPU
        db_manager = UltraFastDatabaseManager()
        processors = {}

        # Initialize processors
        for name, config in INSTRUMENTS.items():
            processors[config["key"]] = UltraFastTickProcessor(config["key"], config, db_manager, processors)

        logger.info(f"🚀 CPU 2: Processors ready for {len(processors)} instruments")
        logger.info("🎯 CPU 2: Ready to process tick data...")

        processed_messages = 0
        dropped_messages = 0
        start_time = time.time()
        stats_time = start_time

        while not shutdown_event.is_set():
            try:
                # Non-blocking queue retrieval with timeout
                if receive_queue.empty():
                    time.sleep(0.001)  # Minimal CPU usage when idle
                    continue

                try:
                    data = receive_queue.get(timeout=0.1)
                except:
                    continue

                # Check for shutdown signals
                if isinstance(data, dict) and data.get("shutdown"):
                    logger.info("🛑 CPU 2: Shutdown signal received")
                    if data.get("market_close"):
                        logger.info("🏁 Market closed - Finalizing all positions")
                        for proc in processors.values():
                            if proc.current_candle:
                                proc._finalize_current_candle_ultra_fast()
                            if proc.current_5min_candle:
                                proc._finalize_5min_candle_ultra_fast()
                    break

                # Process market data
                if 'feeds' in data:
                    # Process all feeds concurrently
                    feed_items = list(data['feeds'].items())

                    # Process feeds in batches to prevent overloading
                    batch_size = 4  # Process 4 symbols simultaneously
                    for i in range(0, len(feed_items), batch_size):
                        batch = dict(feed_items[i:i + batch_size])

                        # Process current batch
                        for key in batch.keys():
                            if key in processors and not shutdown_event.is_set():
                                try:
                                    processors[key].process_tick_ultra_fast(data)
                                    processed_messages += 1
                                except Exception as e:
                                    logger.error(f"Error processing tick for {key}: {e}")

                current_time = time.time()

                # Check for queue overflow
                if receive_queue.qsize() > RECEIVE_QUEUE_SIZE * 0.9:
                    logger.warning(f"⚠️ Queue size critical: {receive_queue.qsize()}/{RECEIVE_QUEUE_SIZE}")

                # Stats every 10 seconds
                if current_time - stats_time > 10 and not shutdown_event.is_set():
                    total_ticks = sum(p.processed_ticks for p in processors.values())
                    total_1m = sum(p.completed_candles for p in processors.values())
                    total_5m = sum(p.completed_5min_candles for p in processors.values())
                    queue_sizes = db_manager.get_queue_sizes()

                    processing_time = current_time - start_time
                    msg_rate = processed_messages / processing_time if processing_time > 0 else 0

                    logger.info(f"⚡ CPU 2 Stats: "
                               f"Processed: {processed_messages} | "
                               f"Rate: {msg_rate:.0f}/sec | "
                               f"Ticks: {total_ticks} | Candles: {total_1m} 1m, {total_5m} 5m | "
                               f"Queues: C={queue_sizes[0]}, HA={queue_sizes[1]}, "
                               f"Trend={queue_sizes[2]}, Latest={queue_sizes[3]} | "
                               f"Receive Queue: {receive_queue.qsize()}")
                    stats_time = current_time

            except KeyboardInterrupt:
                logger.info("⌨️ CPU 2: Keyboard interrupt received")
                break
            except Exception as e:
                logger.error(f"❌ Error in processing loop: {e}")
                time.sleep(0.1)  # Prevent tight error loops

        # Final stats
        total_ticks = sum(p.processed_ticks for p in processors.values())
        total_1m = sum(p.completed_candles for p in processors.values())
        total_5m = sum(p.completed_5min_candles for p in processors.values())

        final_time = time.time() - start_time
        final_msg_rate = processed_messages / final_time if final_time > 0 else 0

        logger.info("📈 CPU 2: Final Stats:"
                   f" | Processed: {processed_messages} messages"
                   f" | Rate: {final_msg_rate:.0f}/sec"
                   f" | Ticks: {total_ticks}"
                   f" | Candles: {total_1m} 1m, {total_5m} 5m"
                   f" | Runtime: {final_time:.1f} sec")

        # Clean shutdown
        db_manager.shutdown()
        logger.info("✅ CPU 2: Data Processing Process terminated cleanly")

    except Exception as e:
        logger.error(f"❌ Fatal error in CPU 2: {e}")
        raise

def is_market_hours_v3():
    """Optimized market hours check"""
    now = datetime.now(IST)
    current_time = now.time()
    is_open = (MARKET_START_TIME <= current_time < MARKET_END_TIME and
               now.weekday() < 5)
    return is_open

# === MAIN ASYNC ENTRY POINT ===
async def main_ultra_fast_async():
    """Main ultra-fast async entry point"""
    global shutdown_event
    shutdown_event = asyncio.Event()

    def signal_handler(signum, frame):
        logger.info(f"🛑 Signal {signum} received - clean shutdown initiated")
        shutdown_event.set()

    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    except ValueError:
        logger.info("⚠️ Running in thread - signal handlers not available")

    # Run the main dual CPU system
    main_dual_cpu_ultra_fast()

def main_dual_cpu_ultra_fast():
    """Main dual-CPU ultra-fast entry point"""
    logger.info("🚀🐆 UPSTOX V3 DUAL-CPU ULTRA-FAST TRADING SYSTEM")
    logger.info("=" * 70)
    logger.info(f"🔥 Using {CPU_COUNT} CPU cores for maximum performance")
    logger.info(f"💽 Database: {TRADING_DB}")
    logger.info("=" * 70)

    for name, config in INSTRUMENTS.items():
        strike = f"(Strike: {config.get('strike_price', 'N/A')})"
        logger.info(f" • {name}: {config['key']} {strike}")
    logger.info("=" * 70)

    # Create multiprocessing synchronization objects
    shutdown_event = MPEvent()
    receive_queue = MPQueue(maxsize=RECEIVE_QUEUE_SIZE)

    try:
        # Start shutdown signal handler
        def signal_handler(signum, frame):
            logger.info(f"🛑 Received signal {signum} - Initiating clean shutdown...")
            shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("🚀 Launching CPU processes...")

        # CPU 1: WebSocket Receiver Process
        receiver_process = Process(
            target=websocket_receiver_process,
            args=(receive_queue, shutdown_event),
            name="WebSocketReceiver-Process",
            daemon=False
        )

        # CPU 2: Data Processing Process
        processor_process = Process(
            target=data_processor_process,
            args=(receive_queue, shutdown_event),
            name="DataProcessor-Process",
            daemon=False
        )

        # Launch both processes
        logger.info("📡 Starting CPU 1: WebSocket Receiver Process...")
        receiver_process.start()
        time.sleep(2)  # Allow receiver to initialize

        logger.info("⚡ Starting CPU 2: Data Processing Process...")
        processor_process.start()

        logger.info("✅ Both CPU processes started successfully!")
        logger.info(f"📊 Monitoring dual-CPU system performance...")
        logger.info("💡 Press Ctrl+C to shutdown cleanly")
        # Monitor processes
        sleep_interval = 30
        start_time = time.time()

        while not shutdown_event.is_set():
            try:
                time.sleep(sleep_interval)
                elapsed = time.time() - start_time

                # Check if processes are still alive
                if not receiver_process.is_alive():
                    logger.error("❌ CPU 1: WebSocket Receiver Process died unexpectedly!")
                    shutdown_event.set()
                    break

                if not processor_process.is_alive():
                    logger.error("❌ CPU 2: Data Processing Process died unexpectedly!")
                    shutdown_event.set()
                    break

                logger.info(f"🔄 System Status: Running {elapsed:.0f}s | "
                           f"CPU 1: Alive | CPU 2: Alive | "
                           f"Queue: {receive_queue.qsize()} items")

            except KeyboardInterrupt:
                logger.info("⌨️ Keyboard interrupt detected")
                shutdown_event.set()
                break

        logger.info("🛑 Shutdown initiated - Waiting for processes to terminate...")

        # Wait for processes to terminate (with timeout)
        receiver_process.join(timeout=10)
        processor_process.join(timeout=10)

        # Force terminate if needed
        if receiver_process.is_alive():
            logger.warning("⚡ Force terminating CPU 1 receiver process...")
            receiver_process.terminate()

        if processor_process.is_alive():
            logger.warning("⚡ Force terminating CPU 2 processing process...")
            processor_process.terminate()

        # Clean up queue
        try:
            while not receive_queue.empty():
                receive_queue.get_nowait()
        except:
            pass

        logger.info("✅ DUAL-CPU ULTRA-FAST SYSTEM TERMINATED CLEANLY")

        # Final status
        logger.info("=" * 70)
        logger.info("🎉 Final System Statistics:")
        logger.info(f"   ⏱️  Total Runtime: {time.time() - start_time:.1f} seconds")
        logger.info(f"   💻 CPU Cores Used: {CPU_COUNT}")
        logger.info(f"   📁 Database: {TRADING_DB}")
        logger.info(f"   🚀 Process Names: {receiver_process.name}, {processor_process.name}")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"❌ Fatal error in dual-CPU main: {e}", exc_info=True)
        shutdown_event.set()

        # Emergency cleanup
        try:
            receiver_process.terminate()
        except:
            pass
        try:
            processor_process.terminate()
        except:
            pass

        raise

# === LEGACY SINGLE-CPU ENTRY POINT (for backward compatibility) ===
def main_ultra_fast():
    """Legacy single-CPU ultra-fast entry point - DEPRECATED"""
    logger.warning("⚠️  DEPRECATED: Use main_dual_cpu_ultra_fast() for optimal performance!")
    logger.info("🚀 UPSTOX V3 ULTRA-FAST TRADING SYSTEM (SINGLE CPU)")
    logger.info("=" * 60)
    for name, config in INSTRUMENTS.items():
        logger.info(f" • {name}: {config['key']} (Strike: {config.get('strike_price', 'N/A')})")
    logger.info("=" * 60)

    try:
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main_ultra_fast_async())
    except KeyboardInterrupt:
        logger.info("⌨️ Keyboard interrupt - shutting down")
    finally:
        logger.info("✅ Ultra-fast system terminated cleanly")
        logger.info(f"📁 Database: {TRADING_DB}")

if __name__ == "__main__":
    # Use the optimized dual-CPU version by default
    main_dual_cpu_ultra_fast()

    # Uncomment below to use single CPU version:
    # main_ultra_fast()
