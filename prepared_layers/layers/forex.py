import pandas as pd
from psycopg2.extras import execute_values
from prepared_layers.utils import get_logger, POSTGRES_DB_ANALYTICS
from prepared_layers.database import get_db_connection

logger = get_logger(__name__)

def calculate_ema(data: pd.Series, period: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return data.ewm(span=period, adjust=False).mean()


def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    """Calculate Average True Range."""
    # For OHLC data, calculate true range
    high_low = high - low
    high_close = abs(high - close.shift(1))
    low_close = abs(low - close.shift(1))
    
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()
    
    return atr


def process_forex_data_1(interval_minutes: int, table_suffix: str):
    """
    Process Forex Data - 1
    
    Creates summaries for WAKMRV and MRVZAR with:
    - Open, Close, High, Low values
    - EMA based on open price (8 and 21 periods)
    - ATR based on open price (8 and 21 periods)
    """
    logger.info(f"Processing Forex Data - 1 ({table_suffix} summaries)")
    
    conn = get_db_connection(POSTGRES_DB_ANALYTICS)
    cursor = conn.cursor()
    
    try:
        table_name = f'prepared_layers.forex_ohlc_{table_suffix}'
        
        # Clear and recalculate for accurate indicators
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        # Get OHLC data for each pair
        for pair_name in ['WAKMRV', 'MRVZAR']:
            query = f"""
                SELECT 
                    date_trunc('hour', timestamp) + 
                        (FLOOR(EXTRACT(MINUTE FROM timestamp) / {interval_minutes}) * INTERVAL '{interval_minutes} minutes') as datetime,
                    pair_name,
                    (ARRAY_AGG(bid_price ORDER BY timestamp ASC))[1] as open_price,
                    MAX(bid_price) as high_price,
                    MIN(bid_price) as low_price,
                    (ARRAY_AGG(bid_price ORDER BY timestamp DESC))[1] as close_price
                FROM forex_data.data_raw
                WHERE pair_name = %s
                GROUP BY datetime, pair_name
                ORDER BY datetime
            """
            
            cursor.execute(query, (pair_name,))
            data = cursor.fetchall()
            
            if not data:
                logger.info(f"No forex data found for {pair_name}")
                continue
                
            df = pd.DataFrame(data, columns=['datetime', 'pair_name', 'open_price', 'high_price', 'low_price', 'close_price'])
            
            # Convert to float
            df['open_price'] = df['open_price'].astype(float)
            df['high_price'] = df['high_price'].astype(float)
            df['low_price'] = df['low_price'].astype(float)
            df['close_price'] = df['close_price'].astype(float)
            
            # Calculate EMAs on open price
            df['ema_8'] = calculate_ema(df['open_price'], 8)
            df['ema_21'] = calculate_ema(df['open_price'], 21)
            
            # Calculate ATRs
            df['atr_8'] = calculate_atr(df['high_price'], df['low_price'], df['close_price'], 8)
            df['atr_21'] = calculate_atr(df['high_price'], df['low_price'], df['close_price'], 21)
            
            # Insert records
            records = []
            for _, row in df.iterrows():
                records.append((
                    row['datetime'],
                    row['pair_name'],
                    float(row['open_price']),
                    float(row['high_price']),
                    float(row['low_price']),
                    float(row['close_price']),
                    float(row['ema_8']) if pd.notna(row['ema_8']) else None,
                    float(row['ema_21']) if pd.notna(row['ema_21']) else None,
                    float(row['atr_8']) if pd.notna(row['atr_8']) else None,
                    float(row['atr_21']) if pd.notna(row['atr_21']) else None
                ))
            
            if records:
                insert_query = f"""
                    INSERT INTO {table_name} (
                        datetime, pair_name, open_price, high_price, low_price, close_price,
                        ema_8, ema_21, atr_8, atr_21
                    ) VALUES %s
                    ON CONFLICT (datetime, pair_name) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        ema_8 = EXCLUDED.ema_8,
                        ema_21 = EXCLUDED.ema_21,
                        atr_8 = EXCLUDED.atr_8,
                        atr_21 = EXCLUDED.atr_21
                """
                execute_values(cursor, insert_query, records)
                logger.info(f"Inserted {len(records)} {table_suffix} records for {pair_name}")
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing Forex Data - 1 ({table_suffix}): {e}")
    finally:
        cursor.close()
        conn.close()
