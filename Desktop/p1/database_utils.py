import os
import time
import sqlite3
import pymysql
import threading
from queue import Queue, Empty, Full

# Centralized check for production environment (e.g., on a VM)
IS_PRODUCTION = os.getenv('FLASK_ENV') == 'production'

# --- Connection Pooling ---
_pools = {}
_pools_lock = threading.Lock()

def get_cloud_sql_connection(db_name='trading'):
    """Get Cloud SQL connection with proper configuration"""
    db_user = os.getenv('CLOUD_SQL_USER')
    db_password = os.getenv('CLOUD_SQL_PASSWORD')
    db_name_resolved = os.getenv('CLOUD_SQL_DATABASE') if db_name == 'trading' else os.getenv('CLOUD_SQL_USER_DATABASE')

    # Universal connection logic:
    # 1. If on App Engine, use the secure Unix socket.
    # 2. If on a VM or local, use the TCP host/port.
    if os.getenv('GAE_ENV', '').startswith('standard'):
        # App Engine Standard uses a Unix socket for secure, low-latency connections.
        unix_socket = f"/cloudsql/{os.getenv('CLOUD_SQL_CONNECTION_NAME')}"
        config = {'unix_socket': unix_socket}
    else:
        # GCE VMs or local development connect via TCP.
        # Ensure CLOUD_SQL_HOST is set to the instance's public or private IP.
        db_host = os.getenv('CLOUD_SQL_HOST')
        config = {'host': db_host, 'port': 3306}

    # Add common connection parameters
    config.update({
        'user': db_user,
        'password': db_password,
        'database': db_name_resolved,
        'charset': 'utf8mb4', # Ensure proper character encoding
        'cursorclass': pymysql.cursors.DictCursor, # Return rows as dictionaries
        'connect_timeout': 10 # Add a 10-second connection timeout for robustness
    })
    return pymysql.connect(**config)

def _create_connection_pool(db_type='trading', max_connections=10):
    """Creates a thread-safe connection pool."""
    with _pools_lock:
        if db_type not in _pools:
            pool = Queue(maxsize=max_connections)
            for _ in range(max_connections):
                try:
                    # Directly call the connection creator, bypassing the pooled functions
                    if IS_PRODUCTION:
                        conn = get_cloud_sql_connection(db_name=db_type)
                    else:
                        conn = _get_local_sqlite_connection(db_name=db_type)
                    pool.put_nowait(conn)
                except Exception as e:
                    print(f"Error creating connection for pool {db_type}: {e}")
            _pools[db_type] = pool

def get_pooled_connection(db_type='trading', fallback_sqlite=True):
    """Gets a connection from the specified pool."""
    if db_type not in _pools:
        _create_connection_pool(db_type)

    try:
        # Get a connection from the queue, wait up to 1 second
        return _pools[db_type].get(timeout=1)
    except Empty:
        if fallback_sqlite and db_type in ['users', 'trading']:
            print(f"⚠️ MySQL connection pool is empty for '{db_type}', falling back to SQLite...")
            return _get_local_sqlite_connection(db_name=db_type)
        else:
            raise Exception(f"Connection pool for '{db_type}' is empty. Could not get a connection.")

def return_pooled_connection(conn, db_type='trading'):
    """Returns a connection to the specified pool."""
    if db_type in _pools:
        try:
            # Put the connection back into the queue without blocking
            _pools[db_type].put_nowait(conn)
        except Full:
            # If the pool is full, just close the connection
            try:
                conn.close()
            except:
                pass

def get_db_connection(use_pool=True):
    """Get database connection - Cloud SQL in production, SQLite in development"""
    if IS_PRODUCTION and use_pool:
        return get_pooled_connection('trading')
    elif IS_PRODUCTION: # Not using pool (e.g., for pool creation)
        return get_cloud_sql_connection(db_name='trading')
    else: # Local development - skip pooling to avoid connection issues
        return _get_local_sqlite_connection('trading')

def get_user_db_connection(use_pool=True):
    """Get user database connection - Fallback to SQLite in production"""
    # Always use SQLite for users database to avoid MySQL connection issues
    if not IS_PRODUCTION or not use_pool:
        return _get_local_sqlite_connection('users')

    # Try pooled connection first, fallback to SQLite if needed
    try:
        return get_pooled_connection('users', fallback_sqlite=True)
    except Exception as e:
        print(f"⚠️ User database falling back to SQLite: {e}")
        return _get_local_sqlite_connection('users')

def _get_local_sqlite_connection(db_name='trading'):
    """Helper to get a local SQLite connection."""
    def dict_factory(cursor, row):
        """Convert sqlite3 rows to dictionaries for consistent API"""
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    db_filename = 'upstox_v3_live_trading.db' if db_name == 'trading' else 'users.db'
    db_path = os.path.join('.', db_filename)
    conn = sqlite3.connect(db_path, timeout=10.0)
    conn.row_factory = dict_factory  # Return rows as dictionaries
    return conn

def execute_query(query, params=None, fetch=False, retry_count=3):
    """Execute query using a pooled connection for high performance."""
    last_error = None
    
    for attempt in range(retry_count):
        conn = None
        try:
            conn = get_pooled_connection('trading')
            with conn.cursor() as cursor:
                # Use the correct placeholder style for the database
                is_mysql = isinstance(conn, pymysql.connections.Connection)
                if is_mysql and params:
                    query = query.replace('?', '%s')
    
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                if fetch:
                    result = cursor.fetchall()
                    return result
                else:
                    conn.commit()
                    return cursor.rowcount
                    
        except (pymysql.Error, sqlite3.Error) as e:
            last_error = e
            print(f"Database error (attempt {attempt + 1}/{retry_count}): {e}")
            # If connection is broken, don't return it to the pool
            if conn:
                conn.close()
                conn = None # Mark as closed
            time.sleep(0.2 * (2 ** attempt))
            continue
            
        finally:
            if conn:
                return_pooled_connection(conn, 'trading')
                    
    raise Exception(f"Database operation failed after {retry_count} attempts. Last error: {last_error}")
