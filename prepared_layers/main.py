"""
Prepared Layers Service

Generates prepared layer outputs from raw PostgreSQL data:

CDR Data - 1: Summarized call detail records aggregated per 15 minutes, 30 minutes, and 1 hour.
CDR Data - 2: Tracking of user tower sessions.
CRM Data - 1: Flattened CRM records including running balances.
Forex Data - 1: Forex OHLC summaries enriched with technical indicators.
"""

import time
import schedule
from prepared_layers.utils import get_logger, setup_logging, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB_ANALYTICS, POSTGRES_DB_PROD
from prepared_layers.database import create_prepared_layer_tables
from prepared_layers.layers.cdr import process_cdr_data_1, process_cdr_data_2
from prepared_layers.layers.crm import process_crm_data_1
from prepared_layers.layers.forex import process_forex_data_1


logger = get_logger(__name__)

def run_layer(layer_name, func, *args):
    """Run a layer function and handle errors."""
    logger.info(f"Starting {layer_name}...")
    try:
        result = func(*args)
        
        # If result is a number (count), record it. If not, maybe just 1 for success.
        # Most of our process functions return None or print logs.
        # Let's check the return values.
        # process_cdr_data_1 returns None
        # process_forex_data_1 returns None
        
        logger.info(f"Completed {layer_name}")
    except Exception as e:
        logger.error(f"Error in {layer_name}: {e}")

def run_all_processing():
    """Run all prepared layer processing jobs."""
    logger.info("=" * 60)
    logger.info("Starting Prepared Layers")
    logger.info("=" * 60)
    
    start_time = time.time()

    # CRM Data - 1: Flattened CRM with balance
    run_layer('CRM_Flattened', process_crm_data_1)

    # CDR Data - 1: Time-based summaries
    run_layer('CDR_15min', process_cdr_data_1, 15, '15min')
    run_layer('CDR_30min', process_cdr_data_1, 30, '30min')
    run_layer('CDR_1hr', process_cdr_data_1, 60, '1hr')
    
    
    # CDR Data - 2: Tower sessions
    run_layer('CDR_Sessions', process_cdr_data_2)
    
    
    # Forex Data - 1: OHLC summaries
    run_layer('Forex_m1', process_forex_data_1, 1, 'm1')
    run_layer('Forex_m30', process_forex_data_1, 30, 'm30')
    run_layer('Forex_h1', process_forex_data_1, 60, 'h1')
    
    elapsed = time.time() - start_time
    logger.info(f"Completed all processing in {elapsed:.2f} seconds")
    logger.info("=" * 60)



def main():
    """Main entry point."""
    setup_logging()
    
    # Start Prometheus Metrics Server
    # Start Prometheus Metrics Server
    # Metrics server removed
    # logger.info("Prometheus metrics server started on port 8001")

    logger.info("=" * 60)
    logger.info("Prepared Layers Service Starting")
    logger.info("=" * 60)
    logger.info(f"PostgreSQL Analytics: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB_ANALYTICS}")
    logger.info(f"PostgreSQL Prod: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB_PROD}")
    
    # Wait for services to be ready
    logger.info("Waiting 30 seconds for services to be ready...")
    time.sleep(30)
    
    # Create tables
    create_prepared_layer_tables()
    
    # Run initial processing
    run_all_processing()
    
    # Schedules processing every 5 minutes
    schedule.every(5).minutes.do(run_all_processing)
    
    logger.info("Scheduled processing every 5 minutes. Running...")
    
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
