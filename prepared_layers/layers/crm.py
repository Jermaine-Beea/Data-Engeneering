"""
CRM Data Prepared Layer
Creates flattened CRM data with balance calculations.
"""
import pandas as pd
from psycopg2.extras import execute_values
from prepared_layers.utils import get_logger, POSTGRES_DB_ANALYTICS, POSTGRES_DB_PROD
from prepared_layers.database import get_db_connection

logger = get_logger(__name__)

# Pricing constants
DATA_COST_PER_GB = 49  # ZAR per GB
VOICE_COST_PER_MIN = 1  # ZAR per minute


def process_crm_data_1():
    """
    Process CRM Data - 1: Create flattened CRM table with balance calculations.
    
    Combines:
    - Account information (account_id, owner_name, email, msisdn)
    - Device information (device_id, device_name, device_type, device_os)
    - Address information (street_address, city, state, postal_code, country)
    - Cost calculation placeholders (ready for CDR data)
    - Running balance per hour
    """
    logger.info("Processing CRM Data - 1 (Flattened + Balance Summary)")
    
    conn_prod = get_db_connection(POSTGRES_DB_PROD)
    conn_analytics = get_db_connection(POSTGRES_DB_ANALYTICS)
    
    try:
        # Read CRM data from production database
        query = """
        SELECT 
            a.account_id,
            a.owner_name,
            a.email,
            a.phone_number as msisdn,
            d.device_id,
            d.device_name,
            d.device_type,
            d.device_os,
            addr.street_address,
            addr.city,
            addr.state,
            addr.postal_code,
            addr.country,
            GREATEST(a.modified_ts, d.modified_ts, addr.modified_ts) as last_modified
        FROM crm_system.accounts a
        LEFT JOIN crm_system.devices d ON a.account_id = d.account_id
        LEFT JOIN crm_system.addresses addr ON a.account_id = addr.account_id
        WHERE a.phone_number IS NOT NULL
        ORDER BY a.account_id, d.device_id
        """
        
        logger.info("Reading CRM data from production database...")
        df = pd.read_sql(query, conn_prod)
        logger.info(f"Read {len(df)} CRM records")
        
        if len(df) == 0:
            logger.warning("No CRM data found")
            return
        
        # Add cost calculation fields (placeholders for now, will be updated by CDR processing)
        df['total_data_bytes'] = 0
        df['data_cost_zar'] = 0.0
        df['total_call_seconds'] = 0
        df['voice_cost_zar'] = 0.0
        df['total_cost_zar'] = 0.0
        df['running_balance_zar'] = 0.0
        df['created_at'] = pd.Timestamp.now()
        
        # Create or replace the flattened table
        cursor = conn_analytics.cursor()
        
        # Drop and recreate table
        logger.info("Creating prepared_layers.crm_flattened_balance table...")
        cursor.execute("""
        DROP TABLE IF EXISTS prepared_layers.crm_flattened_balance CASCADE;
        
        CREATE TABLE prepared_layers.crm_flattened_balance (
            account_id INTEGER,
            owner_name VARCHAR(255),
            email VARCHAR(255),
            msisdn VARCHAR(20),
            device_id INTEGER,
            device_name VARCHAR(255),
            device_type VARCHAR(50),
            device_os VARCHAR(50),
            street_address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(50),
            postal_code VARCHAR(20),
            country VARCHAR(50),
            last_modified TIMESTAMP,
            total_data_bytes BIGINT DEFAULT 0,
            data_cost_zar DECIMAL(10,2) DEFAULT 0,
            total_call_seconds INTEGER DEFAULT 0,
            voice_cost_zar DECIMAL(10,2) DEFAULT 0,
            total_cost_zar DECIMAL(10,2) DEFAULT 0,
            running_balance_zar DECIMAL(10,2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (account_id, device_id)
        );
        
        CREATE INDEX idx_crm_flattened_msisdn ON prepared_layers.crm_flattened_balance(msisdn);
        CREATE INDEX idx_crm_flattened_account ON prepared_layers.crm_flattened_balance(account_id);
        """)
        conn_analytics.commit()
        
        # Insert data
        logger.info(f"Inserting {len(df)} records into crm_flattened_balance...")
        
        # Prepare data for insertion
        records = [tuple(row) for row in df.values]
        columns = list(df.columns)
        
        insert_query = f"""
        INSERT INTO prepared_layers.crm_flattened_balance 
        ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (account_id, device_id) 
        DO UPDATE SET
            owner_name = EXCLUDED.owner_name,
            email = EXCLUDED.email,
            msisdn = EXCLUDED.msisdn,
            device_name = EXCLUDED.device_name,
            device_type = EXCLUDED.device_type,
            device_os = EXCLUDED.device_os,
            street_address = EXCLUDED.street_address,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            postal_code = EXCLUDED.postal_code,
            country = EXCLUDED.country,
            last_modified = EXCLUDED.last_modified
        """
        
        execute_values(cursor, insert_query, records)
        conn_analytics.commit()
        
        logger.info(f"✅ Successfully processed {len(df)} CRM records")
        
        # Show sample data
        cursor.execute("""
        SELECT account_id, owner_name, msisdn, device_name, city, state
        FROM prepared_layers.crm_flattened_balance
        LIMIT 5
        """)
        samples = cursor.fetchall()
        logger.info("Sample records:")
        for sample in samples:
            logger.info(f"  Account {sample[0]}: {sample[1]} | {sample[2]} | {sample[3]} | {sample[4]}, {sample[5]}")
        
    except Exception as e:
        logger.error(f"Error processing CRM data: {e}")
        conn_analytics.rollback()
        raise
    finally:
        conn_prod.close()
        conn_analytics.close()


def update_crm_balances_from_cdr():
    """
    Update CRM flattened table with CDR usage and cost calculations.
    This will be called after CDR data is loaded.
    """
    logger.info("Updating CRM balances with CDR data...")
    
    conn = get_db_connection(POSTGRES_DB_ANALYTICS)
    cursor = conn.cursor()
    
    try:
        # Update data usage and costs
        logger.info("Calculating data usage costs...")
        cursor.execute(f"""
        UPDATE prepared_layers.crm_flattened_balance crm
        SET 
            total_data_bytes = COALESCE(cdr_summary.total_bytes, 0),
            data_cost_zar = COALESCE(cdr_summary.total_bytes, 0) * {DATA_COST_PER_GB} / 1073741824.0,
            total_call_seconds = COALESCE(voice_summary.total_seconds, 0),
            voice_cost_zar = COALESCE(voice_summary.total_seconds, 0) * {VOICE_COST_PER_MIN} / 60.0
        FROM (
            SELECT 
                msisdn,
                SUM(CAST(up_bytes AS BIGINT) + CAST(down_bytes AS BIGINT)) as total_bytes
            FROM cdr_data.cdr_data
            GROUP BY msisdn
        ) cdr_summary
        LEFT JOIN (
            SELECT 
                msisdn,
                SUM(CAST(call_duration_sec AS INTEGER)) as total_seconds
            FROM cdr_data.cdr_voice
            GROUP BY msisdn
        ) voice_summary ON cdr_summary.msisdn = voice_summary.msisdn
        WHERE crm.msisdn = cdr_summary.msisdn;
        """)
        
        # Update total cost and running balance
        cursor.execute("""
        UPDATE prepared_layers.crm_flattened_balance
        SET 
            total_cost_zar = data_cost_zar + voice_cost_zar,
            running_balance_zar = -(data_cost_zar + voice_cost_zar)
        WHERE total_data_bytes > 0 OR total_call_seconds > 0;
        """)
        
        conn.commit()
        
        rows_updated = cursor.rowcount
        logger.info(f"✅ Updated {rows_updated} CRM records with CDR costs")
        
    except Exception as e:
        logger.error(f"Error updating CRM balances: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
