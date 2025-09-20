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
from typing import Optional, List, Dict, Any
import threading
import pandas as pd
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# Load environment variables
load_dotenv()

# Import protobuf message definitions
import MarketDataFeedV3_pb2 as pb

# --- Constants and Configurations ---
CONFIG_PATH = 'config/config.json'
TRADING_DB = os.getenv('DATABASE_PATH', 'database/upstox_v3_live_trading.db')
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

@dataclass
class HeikinAshiCandle:
    open: float
    high: float
    low: float
    close: float
    timestamp: datetime
    instrument_key: str

# --- Database Manager ---
class LockFreeDatabaseManager:
    def __init__(self):
        self.live_db_path = TRADING_DB
        self.running = True
        self.queues = {
            'nifty_1min': deque(), 'nifty_5min': deque(),
            'future_1min': deque(), 'future_5min': deque(),
            'cash_flow': deque(), 'buy_signal': deque()
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
                # Candle Tables
                cursor.execute('''CREATE TABLE IF NOT EXISTS nifty_candles_1min (id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL UNIQUE, open REAL, high REAL, low REAL, close REAL, ha_open REAL, ha_high REAL, ha_low REAL, ha_close REAL, sar REAL, trend INTEGER, volume INTEGER, tick_count INTEGER);''')
                cursor.execute('''CREATE TABLE IF NOT EXISTS nifty_candles_5min (id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL UNIQUE, open REAL, high REAL, low REAL, close REAL, ha_open REAL, ha_high REAL, ha_low REAL, ha_close REAL, sar REAL, trend INTEGER, volume INTEGER, tick_count INTEGER);''')
                cursor.execute('''CREATE TABLE IF NOT EXISTS future_candles_1min (id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL UNIQUE, open REAL, high REAL, low REAL, close REAL, ha_open REAL, ha_high REAL, ha_low REAL, ha_close REAL, sar REAL, trend INTEGER, delta REAL, volume INTEGER, tick_count INTEGER);''')
                cursor.execute('''CREATE TABLE IF NOT EXISTS future_candles_5min (id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL UNIQUE, open REAL, high REAL, low REAL, close REAL, ha_open REAL, ha_high REAL, ha_low REAL, ha_close REAL, sar REAL, trend INTEGER, delta REAL, volume INTEGER, tick_count INTEGER);''')
                # Cash Flow & Signal Tables
                cursor.execute('''CREATE TABLE IF NOT EXISTS options_cash_flow (id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL UNIQUE, cash REAL, min_cash REAL, max_cash REAL);''')
                cursor.execute('''CREATE TABLE IF NOT EXISTS buy_signals (id INTEGER PRIMARY KEY, timestamp TEXT, signal_type TEXT, option_key TEXT, entry_price REAL, target REAL, sl REAL, status TEXT);''')
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
                    if 'nifty_1min' in batches: cursor.executemany('INSERT OR REPLACE INTO nifty_candles_1min VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?)', [tuple(d.values()) for d in batches['nifty_1min']])
                    if 'nifty_5min' in batches: cursor.executemany('INSERT OR REPLACE INTO nifty_candles_5min VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?)', [tuple(d.values()) for d in batches['nifty_5min']])
                    if 'future_1min' in batches: cursor.executemany('INSERT OR REPLACE INTO future_candles_1min VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [tuple(d.values()) for d in batches['future_1min']])
                    if 'future_5min' in batches: cursor.executemany('INSERT OR REPLACE INTO future_candles_5min VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [tuple(d.values()) for d in batches['future_5min']])
                    if 'cash_flow' in batches: cursor.executemany('INSERT OR REPLACE INTO options_cash_flow VALUES (NULL,?,?,?,?)', [(d['timestamp'], d['cash'], d['min_cash'], d['max_cash']) for d in batches['cash_flow']])
                    if 'buy_signal' in batches: cursor.executemany('INSERT INTO buy_signals VALUES (NULL,?,?,?,?,?,?,?)', [(d['timestamp'], d['signal_type'], d['option_key'], d['entry_price'], d['target'], d['sl'], d['status']) for d in batches['buy_signal']])
                    conn.commit()
                except Exception as e:
                    logger.error(f"Exception in DB writer: {e}", exc_info=True)
        logger.info("DB writer thread terminated.")

    def save_data(self, queue_name: str, data: Dict):
        with self._queue_lock:
            self.queues[queue_name].append(data)

    def shutdown(self):
        logger.info("Shutting down database manager...")
        self.running = False
        if self.db_thread.is_alive(): self.db_thread.join(timeout=5)

# --- Core Logic Classes ---
class HeikinAshiCalculator:
    def __init__(self):
        self.previous_ha: Optional[HeikinAshiCandle] = None
    def calculate(self, ohlc: Dict) -> HeikinAshiCandle:
        if self.previous_ha is None:
            ha_open = (ohlc['open'] + ohlc['close']) / 2
        else:
            ha_open = (self.previous_ha.open + self.previous_ha.close) / 2
        ha_close = (ohlc['open'] + ohlc['high'] + ohlc['low'] + ohlc['close']) / 4
        ha_high = max(ohlc['high'], ha_open, ha_close)
        ha_low = min(ohlc['low'], ha_open, ha_close)
        ha_candle = HeikinAshiCandle(open=ha_open, high=ha_high, low=ha_low, close=ha_close, timestamp=ohlc['timestamp'], instrument_key=ohlc['instrument_key'])
        self.previous_ha = ha_candle
        return ha_candle

class FastSAR:
    def __init__(self, acceleration=0.02, maximum=0.2):
        self.acceleration = acceleration
        self.maximum = maximum
        self.reset()
    def reset(self):
        self.sar: Optional[float] = None
        self.trend: Optional[int] = None
        self.af = self.acceleration
        self.ep: Optional[float] = None
    def update(self, high: float, low: float) -> Optional[float]:
        if self.sar is None:
            self.sar = low
            self.trend = 1
            self.ep = high
            return self.sar
        new_sar = self.sar + self.af * (self.ep - self.sar)
        if self.trend == 1:
            if low <= new_sar:
                self.trend = -1; self.sar = self.ep; self.ep = low; self.af = self.acceleration
            else:
                self.sar = new_sar
                if high > self.ep: self.ep = high; self.af = min(self.af + self.acceleration, self.maximum)
        else:
            if high >= new_sar:
                self.trend = 1; self.sar = self.ep; self.ep = high; self.af = self.acceleration
            else:
                self.sar = new_sar
                if low < self.ep: self.ep = low; self.af = min(self.af + self.acceleration, self.maximum)
        return self.sar

class CandleAggregator:
    def __init__(self, instrument_key: str, interval_minutes: int, db_manager: LockFreeDatabaseManager, is_future: bool = False):
        self.instrument_key = instrument_key
        self.interval_minutes = interval_minutes
        self.db_manager = db_manager
        self.is_future = is_future
        self.current_candle: Optional[Dict] = None
        self.ha_calculator = HeikinAshiCalculator()
        self.sar_calculator = FastSAR()
        self.last_vtt = 0.0
        self.delta = 0.0

    def process_tick(self, tick: LiveTick):
        minute = (tick.timestamp.minute // self.interval_minutes) * self.interval_minutes
        minute_timestamp = tick.timestamp.replace(minute=minute, second=0, microsecond=0)
        if self.current_candle is None or self.current_candle['timestamp'] != minute_timestamp:
            if self.current_candle: self._finalize_candle()
            self._start_new_candle(tick, minute_timestamp)
        else:
            self._update_candle(tick)

    def _start_new_candle(self, tick: LiveTick, timestamp: datetime):
        self.current_candle = {'timestamp': timestamp, 'open': tick.ltp, 'high': tick.ltp, 'low': tick.ltp, 'close': tick.ltp, 'volume': 0, 'tick_count': 1, 'instrument_key': tick.instrument_key}
        if self.is_future: self.delta = 0.0; self.last_vtt = tick.vtt or 0.0

    def _update_candle(self, tick: LiveTick):
        if self.current_candle:
            self.current_candle['high'] = max(self.current_candle['high'], tick.ltp)
            self.current_candle['low'] = min(self.current_candle['low'], tick.ltp)
            self.current_candle['close'] = tick.ltp
            self.current_candle['tick_count'] += 1
            if self.is_future and tick.vtt and self.last_vtt > 0:
                vtt_change = tick.vtt - self.last_vtt
                if vtt_change > 0:
                    ltp_change = tick.ltp - self.current_candle['close']
                    if ltp_change > 0: self.delta += vtt_change
                    elif ltp_change < 0: self.delta -= vtt_change
                    self.current_candle['volume'] += vtt_change
                self.last_vtt = tick.vtt

    def _finalize_candle(self):
        if not self.current_candle: return
        ha_candle = self.ha_calculator.calculate(self.current_candle)
        sar_value = self.sar_calculator.update(self.current_candle['high'], self.current_candle['low'])
        table_name = f"{'future' if self.is_future else 'nifty'}_candles_{self.interval_minutes}min"
        candle_data = {'timestamp': self.current_candle['timestamp'].isoformat(), 'open': self.current_candle['open'], 'high': self.current_candle['high'], 'low': self.current_candle['low'], 'close': self.current_candle['close'], 'ha_open': ha_candle.open, 'ha_high': ha_candle.high, 'ha_low': ha_candle.low, 'ha_close': ha_candle.close, 'sar': sar_value, 'trend': self.sar_calculator.trend, 'volume': self.current_candle['volume'], 'tick_count': self.current_candle['tick_count']}
        if self.is_future: candle_data['delta'] = self.delta
        self.db_manager.save_data(table_name, candle_data)

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

class LockFreeTickProcessor:
    def __init__(self, instrument_key: str, config: Dict, db_manager: LockFreeDatabaseManager, cash_flow_calculator: Optional[OptionsTickCashFlowCalculator]):
        self.instrument_key = instrument_key
        self.instrument_type = config.get('type')
        self.option_type = config.get('option_type')
        self.cash_flow_calculator = cash_flow_calculator
        self.ltp = 0.0
        if self.instrument_type != 'OPTION':
            self.candle_1min = CandleAggregator(instrument_key, 1, db_manager, self.instrument_type == 'FUTURE')
            self.candle_5min = CandleAggregator(instrument_key, 5, db_manager, self.instrument_type == 'FUTURE')

    def process_tick(self, feed: Dict):
        try:
            ltpc = feed.get('ff', {}).get('marketFF', {}).get('ltpc') or feed.get('ff', {}).get('indexFF', {}).get('ltpc') or feed.get('ltpc', {})
            if not ltpc or ltpc.get('ltp') is None: return
            tick = LiveTick(instrument_key=self.instrument_key, ltp=ltpc['ltp'], timestamp=datetime.fromtimestamp(int(ltpc['ltt']) / 1000, tz=IST), vtt=feed.get('ff', {}).get('marketFF', {}).get('vtt'), option_type=self.option_type)
            self.ltp = tick.ltp
            if self.instrument_type == 'OPTION':
                if self.cash_flow_calculator: self.cash_flow_calculator.process_option_tick(tick)
            else:
                self.candle_1min.process_tick(tick)
                self.candle_5min.process_tick(tick)
        except Exception as e:
            logger.error(f"Error processing tick for {self.instrument_key}: {e}", exc_info=True)

    def get_ltp(self) -> float:
        return self.ltp

# --- Main Application Logic ---
async def minute_aggregator_task(cash_flow_calculator, buy_signal_generator):
    logger.info("Minute aggregator task started.")
    while not shutdown_event.is_set():
        await asyncio.sleep(60 - time.time() % 60)
        now = datetime.now(IST)
        if not (MARKET_START_TIME <= now.time() < MARKET_END_TIME):
            continue
        cash_flow_state = cash_flow_calculator.get_state_and_reset()
        db_manager.save_data('cash_flow', {'timestamp': now.replace(second=0, microsecond=0).isoformat(), **cash_flow_state})
        logger.info(f"1-Min Cash Flow: {cash_flow_state['cash']:.2f}")
        nifty_processor = processors.get(INSTRUMENTS["NIFTY_INDEX"]["key"])
        if nifty_processor and nifty_processor.get_ltp() > 0:
            buy_signal_generator.generate_signals(cash_flow_state['cash'], nifty_processor.get_ltp())

async def websocket_v3_connection_manager():
    global db_manager, processors, ACCESS_TOKEN
    while not ACCESS_TOKEN:
        logger.error("Access token not found. Retrying in 10s...")
        await asyncio.sleep(10)
        config = load_config()
        ACCESS_TOKEN = config.get('ACCESS_TOKEN', '')

    db_manager = LockFreeDatabaseManager()
    cash_flow_calculator = OptionsTickCashFlowCalculator()
    buy_signal_generator = BuySignalGenerator(db_manager)

    for name, config_item in INSTRUMENTS.items():
        processors[config_item["key"]] = LockFreeTickProcessor(config_item["key"], config_item, db_manager, cash_flow_calculator if config_item.get('type') == 'OPTION' else None)

    aggregator = asyncio.create_task(minute_aggregator_task(cash_flow_calculator, buy_signal_generator))

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
                            if key in processors: processors[key].process_tick(feed)
        except (websockets.ConnectionClosed, ConnectionRefusedError) as e:
            logger.warning(f"WebSocket connection closed: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"WebSocket error: {e}", exc_info=True)
            await asyncio.sleep(5)
    
    aggregator.cancel()
    db_manager.shutdown()

# --- Helper & Entry Point ---
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
    try: return MessageToDict(pb.FeedResponse.FromString(data))
    except Exception: 
        try: return json.loads(data.decode('utf-8'))
        except Exception as e: logger.error(f"Failed to decode message: {e}"); return None

def load_config():
    try:
        with open(CONFIG_PATH, 'r') as f: return json.load(f)
    except FileNotFoundError: logger.error(f"Config file not found: {CONFIG_PATH}"); return {}
    except json.JSONDecodeError as e: logger.error(f"Invalid JSON in config file: {e}"); return {}

def load_options_from_csv(csv_path='extracted_data.csv'):
    try:
        df = pd.read_csv(csv_path)
        all_strikes = sorted(df['strike'].unique(), key=float)
        ce_options = df[df['option_type'] == 'CE']
        pe_options = df[df['option_type'] == 'PE']
        common_strikes = list(set(ce_options['strike']) & set(pe_options['strike']))
        if common_strikes:
            atm_strike = min(common_strikes, key=lambda s: abs(ce_options[ce_options['strike'] == s]['last_price'].values[0] - pe_options[pe_options['strike'] == s]['last_price'].values[0]))
        else:
            atm_strike = sorted(common_strikes, key=float)[len(common_strikes) // 2] if common_strikes else 25000.0
        current_level = round(float(atm_strike) / 50) * 50
        if float(current_level) in all_strikes: center_index = all_strikes.index(float(current_level))
        else: center_index = min(range(len(all_strikes)), key=lambda i: abs(all_strikes[i] - float(current_level)))
        ce_itm_strikes = [all_strikes[i] for i in range(center_index - 1, -1, -1) if df[(df['option_type'] == 'CE') & (df['strike'] == all_strikes[i])].shape[0] > 0][:15]
        ce_otm_strikes = [all_strikes[i] for i in range(center_index + 1, len(all_strikes)) if df[(df['option_type'] == 'CE') & (df['strike'] == all_strikes[i])].shape[0] > 0][:15]
        pe_otm_strikes = [all_strikes[i] for i in range(center_index - 1, -1, -1) if df[(df['option_type'] == 'PE') & (df['strike'] == all_strikes[i])].shape[0] > 0][:15]
        pe_itm_strikes = [all_strikes[i] for i in range(center_index + 1, len(all_strikes)) if df[(df['option_type'] == 'PE') & (df['strike'] == all_strikes[i])].shape[0] > 0][:15]
        ce_selected = df[(df['option_type'] == 'CE') & (df['strike'].isin(ce_itm_strikes + ce_otm_strikes))]
        pe_selected = df[(df['option_type'] == 'PE') & (df['strike'].isin(pe_itm_strikes + pe_otm_strikes))]
        selected = pd.concat([ce_selected, pe_selected]).sort_values(['option_type', 'strike'])
        options_list = [{ 'key': row['instrument_key'], 'symbol': row['symbol'], 'option_type': row['option_type'], 'strike_price': float(row['strike']), 'expiry': row.get('expiry', ''), 'last_price': float(row.get('last_price', 0))} for _, row in selected.iterrows()]
        logger.info(f"Loaded {len(options_list)} options from CSV.")
        return options_list
    except FileNotFoundError: logger.error(f"CSV file not found: {csv_path}"); return []
    except Exception as e: logger.error(f"Error loading options from CSV: {e}", exc_info=True); return []

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
    def handle_shutdown_signal(): logger.info("Shutdown signal received."); shutdown_event.set()
    for sig in (signal.SIGINT, signal.SIGTERM): loop.add_signal_handler(sig, handle_shutdown_signal)
    try: loop.run_until_complete(websocket_v3_connection_manager())
    finally:
        loop.close()
        if db_manager: db_manager.shutdown()
        logger.info("Application terminated cleanly.")

if __name__ == "__main__":
    main()
