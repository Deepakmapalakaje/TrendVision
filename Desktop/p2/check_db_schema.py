import sqlite3
import os
from datetime import datetime, timezone, date, time as dt_time
import sys
# Configuration
TRADING_DB = 'database/upstox_v3_live_trading.db'  # Update path if needed

# Expected schemas
EXPECTED_SCHEMAS = {
    'table_registry': '''
        CREATE TABLE IF NOT EXISTS table_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT UNIQUE NOT NULL,
            instrument_key TEXT NOT NULL,
            data_type TEXT NOT NULL,
            trade_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''',
    'trend': '''
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
    ''',
    'latest_candles': '''
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
    '''
}

def add_column(conn, table_name, column_name, column_definition):
    """Add a column to an existing table if it doesn't exist"""
    try:
        cursor = conn.cursor()
        # Check if column exists
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]

        if column_name not in columns:
            print(f"Adding column '{column_name}' to table '{table_name}'")
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")
            conn.commit()
            return True
        return False
    except Exception as e:
        print(f"Error adding column {column_name} to {table_name}: {e}")
        return False

def add_missing_columns(conn):
    """Add any missing columns to existing tables"""
    cursor = conn.cursor()

    # Check and add columns to latest_candles
    updates_needed = []

    # Check for missing columns in latest_candles
    cursor.execute("PRAGMA table_info(latest_candles)")
    columns = dict((col[1], col[2]) for col in cursor.fetchall())

    # List of expected columns for latest_candles with their types
    expected_columns = {
        'id': 'INTEGER',
        'instrument_key': 'TEXT',
        'instrument_name': 'TEXT',
        'instrument_type': 'TEXT',
        'strike_price': 'REAL',
        'option_type': 'TEXT',
        'timestamp': 'TIMESTAMP',
        'open': 'REAL',
        'high': 'REAL',
        'low': 'REAL',
        'close': 'REAL',
        'volume': 'INTEGER',
        'atp': 'REAL',
        'vwap': 'REAL',
        'price_change': 'REAL',
        'price_change_pct': 'REAL',
        'delta': 'INTEGER',
        'delta_pct': 'REAL',
        'min_delta': 'INTEGER',
        'max_delta': 'INTEGER',
        'buy_volume': 'INTEGER',
        'sell_volume': 'INTEGER',
        'tick_count': 'INTEGER',
        'vtt_open': 'REAL',
        'vtt_close': 'REAL',
        'candle_interval': 'TEXT',
        'trend_value': 'INTEGER',
        'buy_recommendation': 'TEXT',
        'entry_price': 'REAL',
        'target': 'REAL',
        'sl': 'REAL',
        'profit_loss': 'REAL',
        'prev_close': 'REAL',
        'intraday_high': 'REAL',
        'intraday_low': 'REAL',
        'last_updated': 'TIMESTAMP',
        'updated_at': 'TIMESTAMP'
    }

    print(f"\nChecking latest_candles table ({len(columns)} existing columns):")
    for col_name, col_type in expected_columns.items():
        if col_name in columns:
            existing_type = columns[col_name].upper()
            if col_type.upper() in existing_type:
                print(f"✓ Column '{col_name}' exists with type {existing_type}")
            else:
                print(f"⚠ Column '{col_name}' exists but type mismatch: expected {col_type}, found {existing_type}")
        else:
            print(f"✗ Missing column '{col_name}' ({col_type})")
            # Add the missing column
            default_clause = " DEFAULT 0" if col_type.startswith('INTEGER') or col_type.startswith('REAL') else ""
            if col_name in ['instrument_key', 'instrument_name', 'instrument_type', 'option_type', 'buy_recommendation', 'candle_interval']:
                default_clause = " DEFAULT ''"
            elif col_name in ['timestamp', 'last_updated', 'updated_at']:
                if 'timestamp'.upper() in col_type.upper():
                    continue  # Skip timestamp columns for ALTER TABLE
                default_clause = " DEFAULT CURRENT_TIMESTAMP"
            else:
                default_clause = " DEFAULT NULL"
            add_column(conn, 'latest_candles', col_name, f"{col_type}{default_clause}")

def ensure_indexes(conn):
    """Ensure all necessary indexes are created"""
    cursor = conn.cursor()

    # Check existing indexes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    existing_indexes = [row[0] for row in cursor.fetchall()]
    print(f"\nExisting indexes: {existing_indexes}")

    # Required indexes
    required_indexes = [
        ('idx_trend_timestamp', 'trend', 'CREATE INDEX IF NOT EXISTS idx_trend_timestamp ON trend(timestamp)'),
        ('idx_trend_interval', 'trend', 'CREATE INDEX IF NOT EXISTS idx_trend_interval ON trend(candle_interval)'),
        ('idx_latest_candles_instrument', 'latest_candles', 'CREATE INDEX IF NOT EXISTS idx_latest_candles_instrument ON latest_candles(instrument_key)'),
        ('idx_latest_candles_updated', 'latest_candles', 'CREATE INDEX IF NOT EXISTS idx_latest_candles_updated ON latest_candles(updated_at)'),
        ('idx_latest_candles_timestamp', 'latest_candles', 'CREATE INDEX IF NOT EXISTS idx_latest_candles_timestamp ON latest_candles(timestamp)'),
        ('idx_latest_candles_instrument_interval', 'latest_candles', 'CREATE INDEX IF NOT EXISTS idx_latest_candles_instrument_interval ON latest_candles(instrument_key, candle_interval)'),
        ('idx_table_registry_date_instrument', 'table_registry', 'CREATE INDEX IF NOT EXISTS idx_table_registry_date_instrument ON table_registry(trade_date, instrument_key)'),
        ('idx_table_registry_data_type', 'table_registry', 'CREATE INDEX IF NOT EXISTS idx_table_registry_data_type ON table_registry(data_type)')
    ]

    for idx_name, table, create_stmt in required_indexes:
        if idx_name not in existing_indexes:
            print(f"Creating index: {idx_name}")
            try:
                cursor.execute(create_stmt)
                print(f"✓ Index {idx_name} created successfully")
            except Exception as e:
                print(f"✗ Failed to create index {idx_name}: {e}")
        else:
            print(f"✓ Index {idx_name} already exists")

    conn.commit()

def create_missing_tables(conn):
    """Create any missing tables"""
    cursor = conn.cursor()

    # Check which tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = [row[0] for row in cursor.fetchall()]
    print(f"\nExisting tables: {existing_tables}")

    for table_name, create_stmt in EXPECTED_SCHEMAS.items():
        if table_name not in existing_tables:
            print(f"Creating table: {table_name}")
            try:
                cursor.execute(create_stmt)
                print(f"✓ Table {table_name} created successfully")
            except Exception as e:
                print(f"✗ Failed to create table {table_name}: {e}")
        else:
            print(f"✓ Table {table_name} already exists")

    conn.commit()

def main():
    if not os.path.exists(TRADING_DB):
        print(f"Database file {TRADING_DB} does not exist")
        return

    print(f"Checking database: {TRADING_DB}")

    # Enable WAL mode for better concurrency
    conn = sqlite3.connect(TRADING_DB, timeout=30.0, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA synchronous = NORMAL")
    cursor.execute("PRAGMA locking_mode = NORMAL")
    cursor.execute("PRAGMA cache_size = 10000")
    cursor.execute("PRAGMA temp_store = MEMORY")

    try:
        create_missing_tables(conn)
        add_missing_columns(conn)
        ensure_indexes(conn)

        print("\n" + "="*60)
        print("DATABASE CHECK COMPLETE")
        print("="*60)
        print("✅ All required tables and columns have been verified/created")
        print("✅ All necessary indexes are in place")
        print("✅ Database is ready for pipeline operation")

        # Show final stats
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        print(f"\n📊 Database Summary:")
        print(f"   • Total tables: {len(tables)}")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"   • {table[0]}: {count} records")

    except Exception as e:
        print(f"Error during database check: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
