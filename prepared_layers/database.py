import time
import psycopg2
from prepared_layers.utils import (
    get_logger, POSTGRES_HOST, POSTGRES_PORT, 
    POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB_ANALYTICS
)

logger = get_logger(__name__)

def get_db_connection(database: str):
    """Create and return a database connection."""
    for attempt in range(10):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=database
            )
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to {database} (attempt {attempt + 1}/10): {e}")
            time.sleep(5)
    raise Exception(f"Could not connect to {database} after 10 attempts")


def create_prepared_layer_tables():
    """Create all prepared layer tables if they don't exist."""
    conn = get_db_connection(POSTGRES_DB_ANALYTICS)
    cursor = conn.cursor()
    
    try:

        cursor.execute("CREATE SCHEMA IF NOT EXISTS prepared_layers;")
        
        # CDR Data - 1: Time-based summaries (15min, 30min, 1hr)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.cdr_usage_summary_15min (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                msisdn VARCHAR(20) NOT NULL,
                -- Call statistics by type
                voice_call_count INTEGER DEFAULT 0,
                video_call_count INTEGER DEFAULT 0,
                total_call_duration_sec INTEGER DEFAULT 0,
                -- Data statistics by type
                video_data_up_bytes BIGINT DEFAULT 0,
                video_data_down_bytes BIGINT DEFAULT 0,
                audio_data_up_bytes BIGINT DEFAULT 0,
                audio_data_down_bytes BIGINT DEFAULT 0,
                image_data_up_bytes BIGINT DEFAULT 0,
                image_data_down_bytes BIGINT DEFAULT 0,
                text_data_up_bytes BIGINT DEFAULT 0,
                text_data_down_bytes BIGINT DEFAULT 0,
                application_data_up_bytes BIGINT DEFAULT 0,
                application_data_down_bytes BIGINT DEFAULT 0,
                total_up_bytes BIGINT DEFAULT 0,
                total_down_bytes BIGINT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(datetime, msisdn)
            );
            CREATE INDEX IF NOT EXISTS idx_cdr_summary_15min_datetime ON prepared_layers.cdr_usage_summary_15min(datetime);
            CREATE INDEX IF NOT EXISTS idx_cdr_summary_15min_msisdn ON prepared_layers.cdr_usage_summary_15min(msisdn);
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.cdr_usage_summary_30min (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                msisdn VARCHAR(20) NOT NULL,
                voice_call_count INTEGER DEFAULT 0,
                video_call_count INTEGER DEFAULT 0,
                total_call_duration_sec INTEGER DEFAULT 0,
                video_data_up_bytes BIGINT DEFAULT 0,
                video_data_down_bytes BIGINT DEFAULT 0,
                audio_data_up_bytes BIGINT DEFAULT 0,
                audio_data_down_bytes BIGINT DEFAULT 0,
                image_data_up_bytes BIGINT DEFAULT 0,
                image_data_down_bytes BIGINT DEFAULT 0,
                text_data_up_bytes BIGINT DEFAULT 0,
                text_data_down_bytes BIGINT DEFAULT 0,
                application_data_up_bytes BIGINT DEFAULT 0,
                application_data_down_bytes BIGINT DEFAULT 0,
                total_up_bytes BIGINT DEFAULT 0,
                total_down_bytes BIGINT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(datetime, msisdn)
            );
            CREATE INDEX IF NOT EXISTS idx_cdr_summary_30min_datetime ON prepared_layers.cdr_usage_summary_30min(datetime);
            CREATE INDEX IF NOT EXISTS idx_cdr_summary_30min_msisdn ON prepared_layers.cdr_usage_summary_30min(msisdn);
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.cdr_usage_summary_1hr (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                msisdn VARCHAR(20) NOT NULL,
                voice_call_count INTEGER DEFAULT 0,
                video_call_count INTEGER DEFAULT 0,
                total_call_duration_sec INTEGER DEFAULT 0,
                video_data_up_bytes BIGINT DEFAULT 0,
                video_data_down_bytes BIGINT DEFAULT 0,
                audio_data_up_bytes BIGINT DEFAULT 0,
                audio_data_down_bytes BIGINT DEFAULT 0,
                image_data_up_bytes BIGINT DEFAULT 0,
                image_data_down_bytes BIGINT DEFAULT 0,
                text_data_up_bytes BIGINT DEFAULT 0,
                text_data_down_bytes BIGINT DEFAULT 0,
                application_data_up_bytes BIGINT DEFAULT 0,
                application_data_down_bytes BIGINT DEFAULT 0,
                total_up_bytes BIGINT DEFAULT 0,
                total_down_bytes BIGINT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(datetime, msisdn)
            );
            CREATE INDEX IF NOT EXISTS idx_cdr_summary_1hr_datetime ON prepared_layers.cdr_usage_summary_1hr(datetime);
            CREATE INDEX IF NOT EXISTS idx_cdr_summary_1hr_msisdn ON prepared_layers.cdr_usage_summary_1hr(msisdn);
        """)
        
        # CDR Data - 2: Tower sessions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.cdr_tower_sessions (
                id SERIAL PRIMARY KEY,
                msisdn VARCHAR(20) NOT NULL,
                tower_id INTEGER NOT NULL,
                session_start TIMESTAMP NOT NULL,
                session_end TIMESTAMP NOT NULL,
                interaction_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_tower_sessions_msisdn ON prepared_layers.cdr_tower_sessions(msisdn);
            CREATE INDEX IF NOT EXISTS idx_tower_sessions_tower_id ON prepared_layers.cdr_tower_sessions(tower_id);
            CREATE INDEX IF NOT EXISTS idx_tower_sessions_start ON prepared_layers.cdr_tower_sessions(session_start);
        """)
        
        # CRM Data - 1: Flattened CRM with running balance
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.crm_user_balance_hourly (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                -- User info
                account_id INTEGER NOT NULL,
                owner_name VARCHAR(100),
                email VARCHAR(100),
                phone_number VARCHAR(100),
                -- Address info
                street_address VARCHAR(100),
                city VARCHAR(100),
                state VARCHAR(100),
                postal_code VARCHAR(100),
                country VARCHAR(100),
                -- Device info
                device_id INTEGER,
                device_name VARCHAR(100),
                device_type VARCHAR(100),
                device_os VARCHAR(100),
                -- Balance info (in ZAR converted to WAK)
                call_cost_zar DECIMAL(15, 4) DEFAULT 0,
                data_cost_zar DECIMAL(15, 4) DEFAULT 0,
                total_cost_zar DECIMAL(15, 4) DEFAULT 0,
                avg_wakmrv_rate DECIMAL(10, 4),
                avg_mrvzar_rate DECIMAL(10, 4),
                call_cost_wak DECIMAL(15, 4) DEFAULT 0,
                data_cost_wak DECIMAL(15, 4) DEFAULT 0,
                total_cost_wak DECIMAL(15, 4) DEFAULT 0,
                accumulated_cost_wak DECIMAL(15, 4) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(datetime, account_id)
            );
            CREATE INDEX IF NOT EXISTS idx_crm_balance_datetime ON prepared_layers.crm_user_balance_hourly(datetime);
            CREATE INDEX IF NOT EXISTS idx_crm_balance_account ON prepared_layers.crm_user_balance_hourly(account_id);
        """)
        
        # Forex Data - 1: OHLC summaries with technical indicators
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.forex_ohlc_m1 (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                pair_name VARCHAR(20) NOT NULL,
                open_price DECIMAL(10, 4) NOT NULL,
                high_price DECIMAL(10, 4) NOT NULL,
                low_price DECIMAL(10, 4) NOT NULL,
                close_price DECIMAL(10, 4) NOT NULL,
                ema_8 DECIMAL(10, 4),
                ema_21 DECIMAL(10, 4),
                atr_8 DECIMAL(10, 4),
                atr_21 DECIMAL(10, 4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(datetime, pair_name)
            );
            CREATE INDEX IF NOT EXISTS idx_forex_m1_datetime ON prepared_layers.forex_ohlc_m1(datetime);
            CREATE INDEX IF NOT EXISTS idx_forex_m1_pair ON prepared_layers.forex_ohlc_m1(pair_name);
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.forex_ohlc_m30 (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                pair_name VARCHAR(20) NOT NULL,
                open_price DECIMAL(10, 4) NOT NULL,
                high_price DECIMAL(10, 4) NOT NULL,
                low_price DECIMAL(10, 4) NOT NULL,
                close_price DECIMAL(10, 4) NOT NULL,
                ema_8 DECIMAL(10, 4),
                ema_21 DECIMAL(10, 4),
                atr_8 DECIMAL(10, 4),
                atr_21 DECIMAL(10, 4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(datetime, pair_name)
            );
            CREATE INDEX IF NOT EXISTS idx_forex_m30_datetime ON prepared_layers.forex_ohlc_m30(datetime);
            CREATE INDEX IF NOT EXISTS idx_forex_m30_pair ON prepared_layers.forex_ohlc_m30(pair_name);
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.forex_ohlc_h1 (
                id SERIAL PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                pair_name VARCHAR(20) NOT NULL,
                open_price DECIMAL(10, 4) NOT NULL,
                high_price DECIMAL(10, 4) NOT NULL,
                low_price DECIMAL(10, 4) NOT NULL,
                close_price DECIMAL(10, 4) NOT NULL,
                ema_8 DECIMAL(10, 4),
                ema_21 DECIMAL(10, 4),
                atr_8 DECIMAL(10, 4),
                atr_21 DECIMAL(10, 4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(datetime, pair_name)
            );
            CREATE INDEX IF NOT EXISTS idx_forex_h1_datetime ON prepared_layers.forex_ohlc_h1(datetime);
            CREATE INDEX IF NOT EXISTS idx_forex_h1_pair ON prepared_layers.forex_ohlc_h1(pair_name);
        """)
        
        # Processing state tracking table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_layers.processing_state (
                id SERIAL PRIMARY KEY,
                layer_name VARCHAR(100) UNIQUE NOT NULL,
                last_processed_datetime TIMESTAMP,
                last_run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        conn.commit()
        logger.info("Successfully created all prepared layer tables")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating tables: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
