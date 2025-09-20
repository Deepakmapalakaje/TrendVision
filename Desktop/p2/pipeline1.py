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
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
import threading
import pandas as pd
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

load_dotenv()

import MarketDataFeedV3_pb2 as pb

# --- Constants and Configurations ---
CONFIG_PATH = 'config/config.json'
TRADING_DB = os.getenv('TRADING_DB', 'database/upstox_v3_live_trading.db')
UPSTOX_V3_AUTH_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"

# --- Logging Setup ---
def setup_logging():
    log_format = '%(asctime)s - %(levelname)s - %(threadName)s - %(funcName)s:%(lineno)d - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format, handlers=[
        logging.FileHandler('upstox_v3_trading.log', mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ])
    return logging.getLogger('TrendVisionV3')

logger = setup_logging()

# --- Global State ---
shutdown_event = asyncio.Event()
db_manager = None
processors: Dict[str, Any] = {}
INSTRUMENTS: Dict[str, Dict[str, Any]] = {}
INSTRUMENT_KEYS: List[str] = []
ACCESS_TOKEN = ''

# --- Time & Market Hours ---
IST = ZoneInfo("Asia/Kolkata")
MARKET_START_TIME = dt_time(9, 15)
MARKET_END_TIME = dt_time(15, 30)

# --- Dataclasses ---
@dataclass
class LiveTick:
    instrument_key: str
    ltp: float
    timestamp: datetime
    vtt: Optional[float] = None
    option_type: Optional[str] = None

# --- Database Manager ---
class LockFreeDatabaseManager:
    def __init__(self):
        self.live_db_path = TRADING_DB
        self.running = True
        self.queues = {
            'cash_flow': deque(),
            'buy_signal': deque(),
            'option_tracking': deque(),
            'latest_instrument': deque()
        }
        self._queue_lock = threading.Lock()
        self._initialize_database()
        self.db_thread = threading.Thread(target=self._single_thread_db_writer, name="DBWriter", daemon=True)
        self.db_thread.start()
        logger.info("Database manager initialized.")

    def _initialize_database(self):
        try:
            os.makedirs(os.path.dirname(self.live_db_path), exist_ok=True)
            with sqlite3.connect(self.live_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA journal_mode = WAL")
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS options_cash_flow (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL UNIQUE,
                    cash REAL NOT NULL, min_cash REAL NOT NULL, max_cash REAL NOT NULL
                );
                ''')
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS buy_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL, signal_type TEXT NOT NULL, option_key TEXT NOT NULL,
                    entry_price REAL NOT NULL, target REAL NOT NULL, sl REAL NOT NULL, status TEXT NOT NULL
                );
                ''')
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS option_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id INTEGER REFERENCES buy_signals(id),
                    timestamp TEXT NOT NULL, current_price REAL NOT NULL, pnl REAL NOT NULL, status TEXT NOT NULL
                );
                ''')
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS latest_instrument_data (
                    instrument_key TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    ltp REAL NOT NULL
                );
                ''')
                logger.info("Database schema verified/initialized.")
        except Exception as e:
            logger.critical(f"FATAL: Failed to initialize database: {e}", exc_info=True)
            raise

    def _single_thread_db_writer(self):
        with sqlite3.connect(self.live_db_path, timeout=15.0) as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            logger.info("DB writer thread started.")
            while self.running:
                try:
                    batches = {}
                    with self._queue_lock:
                        for name, queue in self.queues.items():
                            if queue:
                                batches[name] = list(queue)
                                queue.clear()
                    if not batches:
                        time.sleep(0.05)
                        continue
                    
                    cursor = conn.cursor()
                    if 'cash_flow' in batches:
                        cursor.executemany('INSERT OR REPLACE INTO options_cash_flow (timestamp, cash, min_cash, max_cash) VALUES (?, ?, ?, ?)', 
                                            [(d['timestamp'], d['cash'], d['min_cash'], d['max_cash']) for d in batches['cash_flow']])
                    if 'buy_signal' in batches:
                        cursor.executemany('INSERT INTO buy_signals (timestamp, signal_type, option_key, entry_price, target, sl, status) VALUES (?, ?, ?, ?, ?, ?, ?)', 
                                            [(d['timestamp'], d['signal_type'], d['option_key'], d['entry_price'], d['target'], d['sl'], d['status']) for d in batches['buy_signal']])
                    if 'option_tracking' in batches:
                        cursor.executemany('INSERT INTO option_tracking (signal_id, timestamp, current_price, pnl, status) VALUES (?, ?, ?, ?, ?)', 
                                            [(d['signal_id'], d['timestamp'], d['current_price'], d['pnl'], d['status']) for d in batches['option_tracking']])
                    if 'latest_instrument' in batches:
                        cursor.executemany('INSERT OR REPLACE INTO latest_instrument_data (instrument_key, timestamp, ltp) VALUES (?, ?, ?)', 
                                            [(d['instrument_key'], d['timestamp'], d['ltp']) for d in batches['latest_instrument']])
                    conn.commit()
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e):
                        logger.warning("Database is locked, retrying...")
                        time.sleep(0.5)
                    else:
                        logger.error(f"DB Error: {e}", exc_info=True)
                except Exception as e:
                    logger.error(f"Exception in DB writer: {e}", exc_info=True)
        logger.info("DB writer thread terminated.")

    def save_data(self, queue_name: str, data: Dict):
        with self._queue_lock:
            self.queues[queue_name].append(data)

    def shutdown(self):
        logger.info("Shutting down database manager...")
        self.running = False
        if self.db_thread.is_alive():
            self.db_thread.join(timeout=5)

# --- Core Logic Classes ---
class OptionsTickCashFlowCalculator:
    def __init__(self):
        self.cash = 0.0
        self.min_cash = 0.0
        self.max_cash = 0.0
        self.last_ltp: Dict[str, float] = {}
        self.last_vtt: Dict[str, float] = {}
        self.lock = threading.Lock()

    def process_option_tick(self, tick: LiveTick):
        with self.lock:
            if tick.vtt is None: return
            prev_ltp = self.last_ltp.get(tick.instrument_key, tick.ltp)
            prev_vtt = self.last_vtt.get(tick.instrument_key, tick.vtt)
            vtt_change = tick.vtt - prev_vtt
            if vtt_change > 0:
                ltp_change = tick.ltp - prev_ltp
                cash_change = tick.ltp * vtt_change
                if tick.option_type == 'CE':
                    self.cash += cash_change if ltp_change >= 0 else -cash_change
                elif tick.option_type == 'PE':
                    self.cash -= cash_change if ltp_change >= 0 else -cash_change
            self.min_cash = min(self.min_cash, self.cash)
            self.max_cash = max(self.max_cash, self.cash)
            self.last_ltp[tick.instrument_key] = tick.ltp
            self.last_vtt[tick.instrument_key] = tick.vtt

    def get_state_and_reset(self):
        with self.lock:
            state = {'cash': self.cash, 'min_cash': self.min_cash, 'max_cash': self.max_cash}
            self.min_cash, self.max_cash = self.cash, self.cash
            return state

class BuySignalGenerator:
    def __init__(self, db_manager: LockFreeDatabaseManager, tracker: 'OptionTracker'):
        self.db_manager = db_manager
        self.tracker = tracker

    def generate_signals(self, cash: float, nifty_price: float):
        if self.tracker.has_active_position(): return
        
        option_to_buy = None
        signal_type = None
        if cash > 500000: # Threshold to generate a signal
            option_to_buy = self._get_first_itm_option('CE', nifty_price)
            signal_type = 'BUY_CE'
        elif cash < -500000:
            option_to_buy = self._get_first_itm_option('PE', nifty_price)
            signal_type = 'BUY_PE'
        
        if option_to_buy and option_to_buy['key'] in processors:
            entry_price = processors[option_to_buy['key']].get_ltp()
            if entry_price > 0:
                signal = {
                    'timestamp': datetime.now(IST).isoformat(), 'signal_type': signal_type, 'option_key': option_to_buy['key'],
                    'entry_price': entry_price, 'target': entry_price * 1.2, 'sl': entry_price * 0.8,
                    'status': 'ACTIVE'
                }
                self.db_manager.save_data('buy_signal', signal)
                self.tracker.start_tracking(signal)
                logger.info(f"ACTION: {signal_type} for {option_to_buy['symbol']} at {entry_price:.2f}")

    def _get_first_itm_option(self, option_type: str, nifty_price: float) -> Optional[Dict]:
        options = [v for v in INSTRUMENTS.values() if v.get('type') == 'OPTION' and v.get('option_type') == option_type]
        if not options: return None
        if option_type == 'CE':
            options = [o for o in options if o['strike_price'] < nifty_price]
            return max(options, key=lambda x: x['strike_price']) if options else None
        else: # PE
            options = [o for o in options if o['strike_price'] > nifty_price]
            return min(options, key=lambda x: x['strike_price']) if options else None

class OptionTracker:
    def __init__(self, db_manager: LockFreeDatabaseManager):
        self.db_manager = db_manager
        self.active_trade: Optional[Dict] = None
        self.lock = threading.Lock()

    def has_active_position(self) -> bool:
        with self.lock:
            return self.active_trade is not None

    def start_tracking(self, signal: Dict):
        with self.lock:
            if not self.active_trade:
                self.active_trade = signal
                logger.info(f"TRACKING STARTED for {signal['option_key']}")

    def check_position(self):
        with self.lock:
            if not self.active_trade: return
            
            key = self.active_trade['option_key']
            if key not in processors: return

            ltp = processors[key].get_ltp()
            if ltp <= 0: return

            status = ''
            if ltp >= self.active_trade['target']:
                status = 'TARGET_HIT'
            elif ltp <= self.active_trade['sl']:
                status = 'SL_HIT'
            
            if status:
                pnl = (ltp - self.active_trade['entry_price']) * 50 # Assuming lot size 50
                logger.info(f"ACTION: {status} for {key}. P&L: {pnl:.2f}")
                self.db_manager.save_data('option_tracking', {
                    'signal_id': self.active_trade.get('id', -1), # Requires getting ID after insert
                    'timestamp': datetime.now(IST).isoformat(),
                    'current_price': ltp, 'pnl': pnl, 'status': status
                })
                self.active_trade = None # Stop tracking

# --- Main Application Logic ---
async def minute_aggregator_task(cash_flow_calculator, buy_signal_generator, tracker):
    logger.info("Minute aggregator task started.")
    while not shutdown_event.is_set():
        await asyncio.sleep(60 - time.time() % 60)
        now = datetime.now(IST)
        if not (MARKET_START_TIME <= now.time() < MARKET_END_TIME):
            continue
        
        cash_flow_state = cash_flow_calculator.get_state_and_reset()
        db_manager.save_data('cash_flow', {
            'timestamp': now.replace(second=0, microsecond=0).isoformat(),
            **cash_flow_state
        })
        logger.info(f"1-Min Cash Flow: {cash_flow_state['cash']:.2f}")

        nifty_processor = processors.get(INSTRUMENTS["NIFTY_INDEX"]["key"])
        if nifty_processor and nifty_processor.get_ltp() > 0:
            buy_signal_generator.generate_signals(cash_flow_state['cash'], nifty_processor.get_ltp())
        
        tracker.check_position()

async def websocket_v3_connection_manager():
    global db_manager, processors, ACCESS_TOKEN
    
    while not ACCESS_TOKEN:
        logger.error("Access token not found in config. Retrying in 10s...")
        await asyncio.sleep(10)
        config = load_config()
        ACCESS_TOKEN = config.get('ACCESS_TOKEN', '')

    db_manager = LockFreeDatabaseManager()
    cash_flow_calculator = OptionsTickCashFlowCalculator()
    tracker = OptionTracker(db_manager)
    buy_signal_generator = BuySignalGenerator(db_manager, tracker)

    for name, config_item in INSTRUMENTS.items():
        is_option = config_item.get('type') == 'OPTION'
        processors[config_item["key"]] = LockFreeTickProcessor(
            config_item["key"], config_item, db_manager, cash_flow_calculator if is_option else None
        )

    aggregator = asyncio.create_task(minute_aggregator_task(cash_flow_calculator, buy_signal_generator, tracker))

    ws_url = get_market_data_feed_authorize_v3()['data']['authorized_redirect_uri']
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    while not shutdown_event.is_set():
        try:
            async with websockets.connect(ws_url, ssl=ssl_context, ping_interval=20) as websocket:
                logger.info("WebSocket connected.")
                await websocket.send(create_v3_subscription_message(INSTRUMENT_KEYS, "full"))
                logger.info(f"Subscribed to {len(INSTRUMENT_KEYS)} instruments.")
                while not shutdown_event.is_set():
                    message = await websocket.recv()
                    decoded_data = decode_v3_message(message)
                    if decoded_data and 'feeds' in decoded_data:
                        for key, feed in decoded_data['feeds'].items():
                            if key in processors:
                                processors[key].process_tick(feed)
        except (websockets.ConnectionClosed, ConnectionRefusedError) as e:
            logger.warning(f"WebSocket connection closed: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"WebSocket error: {e}", exc_info=True)
            await asyncio.sleep(5)
    
    aggregator.cancel()
    db_manager.shutdown()

# --- Helper & Entry Point ---
class LockFreeTickProcessor:
    def __init__(self, instrument_key: str, config: Dict, db_manager: LockFreeDatabaseManager, cash_flow_calculator: Optional[OptionsTickCashFlowCalculator]):
        self.instrument_key = instrument_key
        self.config = config
        self.db_manager = db_manager
        self.instrument_type = config.get('type')
        self.option_type = config.get('option_type')
        self.cash_flow_calculator = cash_flow_calculator
        self.ltp = 0.0
        self.vtt = 0.0
        self.previous_tick: Optional[LiveTick] = None

    def process_tick(self, feed: Dict):
        try:
            ltpc = feed.get('ff', {}).get('marketFF', {}).get('ltpc') or feed.get('ff', {}).get('indexFF', {}).get('ltpc') or feed.get('ltpc', {})
            if not ltpc: return

            ltp = ltpc.get('ltp')
            if ltp is None: return

            self.ltp = ltp
            tick = LiveTick(
                instrument_key=self.instrument_key, ltp=ltp, 
                timestamp=datetime.fromtimestamp(int(ltpc['ltt']) / 1000, tz=IST),
                vtt=feed.get('ff', {}).get('marketFF', {}).get('vtt'),
                option_type=self.option_type
            )
            if self.instrument_type == 'OPTION' and self.cash_flow_calculator:
                self.cash_flow_calculator.process_option_tick(tick)
            
            if self.instrument_type != 'OPTION':
                self.db_manager.save_data('latest_instrument', {
                    'instrument_key': self.instrument_key,
                    'timestamp': tick.timestamp.isoformat(),
                    'ltp': tick.ltp
                })

            self.previous_tick = tick
        except Exception as e:
            logger.error(f"Error processing tick for {self.instrument_key}: {e}", exc_info=True)

    def get_ltp(self) -> float:
        return self.ltp

def get_market_data_feed_authorize_v3() -> Dict:
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
    try:
        response = requests.get(UPSTOX_V3_AUTH_URL, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.critical(f"Failed to get WebSocket URL: {e}", exc_info=True)
        raise

def create_v3_subscription_message(keys: List[str], mode: str) -> bytes:
    return json.dumps({"guid": "trendvision-v3", "method": "sub", "data": {"mode": mode, "instrumentKeys": keys}}).encode('utf-8')

def decode_v3_message(data: bytes) -> Optional[Dict]:
    try:
        return MessageToDict(pb.FeedResponse.FromString(data))
    except Exception:
        try:
            return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to decode message: {e}")
            return None

def main():
    global ACCESS_TOKEN, INSTRUMENTS, INSTRUMENT_KEYS
    config = load_config()
    ACCESS_TOKEN = config.get('ACCESS_TOKEN', '')
    BASE_INSTRUMENTS = {
        "NIFTY_INDEX": {"key": "NSE_INDEX|Nifty 50", "type": "INDEX", "symbol": "NIFTY 50"},
        "NIFTY_FUTURE": {"key": config.get('NIFTY_FUTURE_key', "NSE_FO|53001"), "type": "FUTURE", "symbol": "NIFTY Future"}
    }
    options = load_options_from_csv()
    option_instruments = {opt['symbol']: {**opt, 'type': 'OPTION'} for opt in options}
    INSTRUMENTS = {**BASE_INSTRUMENTS, **option_instruments}
    INSTRUMENT_KEYS = [v['key'] for v in INSTRUMENTS.values()]
    
    loop = asyncio.get_event_loop()
    def handle_shutdown_signal():
        logger.info("Shutdown signal received.")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown_signal)

    try:
        loop.run_until_complete(websocket_v3_connection_manager())
    finally:
        loop.close()
        if db_manager:
            db_manager.shutdown()
        logger.info("Application terminated cleanly.")

def load_options_from_csv(csv_path='extracted_data.csv'):
    # ... (implementation as before)
    pass

if __name__ == "__main__":
    main()