# Create main.py
import os
import time
import csv
import json
import logging
from datetime import datetime
from pathlib import Path
import paramiko
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SFTP_HOST = os.getenv('SFTP_HOST', 'sftp')
SFTP_PORT = int(os.getenv('SFTP_PORT', '22'))
SFTP_USER = os.getenv('SFTP_USER', 'cdr_data')
SFTP_PASSWORD = os.getenv('SFTP_PASSWORD', 'password')
SFTP_REMOTE_PATH = os.getenv('SFTP_REMOTE_PATH', '/upload')

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'redpanda-0:9092,redpanda-1:9092,redpanda-2:9092').split(',')
CDR_DATA_TOPIC = os.getenv('CDR_DATA_TOPIC', 'cdr-data')
CDR_VOICE_TOPIC = os.getenv('CDR_VOICE_TOPIC', 'cdr-voice')

PROCESSED_FILES_LOG = 'processed_files.txt'


def get_sftp_connection():
    """Establish SFTP connection."""
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logger.info(f"Connected to SFTP server: {SFTP_HOST}")
        return sftp, transport
    except Exception as e:
        logger.error(f"Failed to connect to SFTP: {e}")
        raise


def get_kafka_producer():
    """Create Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info(f"Connected to Kafka brokers: {KAFKA_BROKERS}")
        return producer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        raise


def load_processed_files():
    """Load list of already processed files."""
    if os.path.exists(PROCESSED_FILES_LOG):
        with open(PROCESSED_FILES_LOG, 'r') as f:
            return set(line.strip() for line in f)
    return set()


def mark_file_as_processed(filename):
    """Mark a file as processed."""
    with open(PROCESSED_FILES_LOG, 'a') as f:
        f.write(f"{filename}\n")


def list_cdr_files(sftp):
    """List all CDR files from SFTP server."""
    try:
        files = sftp.listdir(SFTP_REMOTE_PATH)
        cdr_files = [f for f in files if f.startswith('cdr_') and f.endswith('.csv')]
        logger.info(f"Found {len(cdr_files)} CDR files on SFTP server")
        return cdr_files
    except Exception as e:
        logger.error(f"Failed to list files from SFTP: {e}")
        return []


def download_and_process_file(sftp, filename, producer, processed_files):
    """Download a CDR file from SFTP and stream to Redpanda."""
    if filename in processed_files:
        logger.info(f"File already processed: {filename}")
        return

    try:
        # Download file
        remote_path = f"{SFTP_REMOTE_PATH}/{filename}"
        local_path = f"/tmp/{filename}"
        
        logger.info(f"Downloading: {filename}")
        sftp.get(remote_path, local_path)
        
        # Determine topic based on filename
        if 'data' in filename:
            topic = CDR_DATA_TOPIC
            record_type = 'data'
        elif 'voice' in filename:
            topic = CDR_VOICE_TOPIC
            record_type = 'voice'
        else:
            logger.warning(f"Unknown file type: {filename}")
            return
        
        # Parse and stream CSV
        records_sent = 0
        with open(local_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Add metadata
                row['_source_file'] = filename
                row['_ingestion_timestamp'] = datetime.utcnow().isoformat()
                row['_record_type'] = record_type
                
                # Send to Kafka
                producer.send(topic, value=row)
                records_sent += 1
                
                if records_sent % 1000 == 0:
                    logger.info(f"Streamed {records_sent} records from {filename} to {topic}")
        
        # Flush to ensure all messages are sent
        producer.flush()
        
        logger.info(f"Completed {filename}: {records_sent} records sent to {topic}")
        
        # Mark as processed
        mark_file_as_processed(filename)
        
        # Clean up local file
        os.remove(local_path)
        
    except Exception as e:
        logger.error(f"Error processing {filename}: {e}")


def main():
    """Main function to orchestrate CDR to Redpanda streaming."""
    logger.info("Starting CDR to Redpanda Streamer...")
    
    # Wait for services to be ready
    logger.info("Waiting 30 seconds for SFTP and Redpanda to be ready...")
    time.sleep(30)
    
    # Load processed files
    processed_files = load_processed_files()
    logger.info(f"Already processed {len(processed_files)} files")
    
    # Connect to SFTP and Kafka
    sftp, transport = get_sftp_connection()
    producer = get_kafka_producer()
    
    try:
        # List and process all CDR files
        cdr_files = list_cdr_files(sftp)
        
        # Sort files by name to process in chronological order
        cdr_files.sort()
        
        total_files = len(cdr_files)
        new_files = [f for f in cdr_files if f not in processed_files]
        
        logger.info(f"Total files: {total_files}, New files to process: {len(new_files)}")
        
        for i, filename in enumerate(new_files, 1):
            logger.info(f"Processing file {i}/{len(new_files)}: {filename}")
            download_and_process_file(sftp, filename, producer, processed_files)
            processed_files.add(filename)
        
        logger.info("✅ All CDR files have been streamed to Redpanda!")
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise
    finally:
        # Cleanup
        producer.close()
        sftp.close()
        transport.close()
        logger.info("Connections closed")


if __name__ == "__main__":
    main()
