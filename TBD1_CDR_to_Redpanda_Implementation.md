TBD 1: CDR to Redpanda - Implementation Guide
Overview
This document describes the implementation of the CDR (Call Data Record) streaming system that downloads CDR files from an SFTP server and streams them to Redpanda topics for downstream processing.
What is CDR?
Call Data Records (CDR) are detailed records of telecommunications events including:

Voice calls: Duration, destination number, call type (voice/video)
Data sessions: Bytes uploaded/downloaded, data type, websites accessed

These records are essential for billing, network analysis, and usage tracking.

Architecture
SFTP Server (CDR Files)
    ↓ (Download via Paramiko)
CDR to Redpanda Streamer
    ↓ (Parse CSV)
    ↓ (Stream via Kafka Protocol)
Redpanda Topics
    ├── cdr-data (Data usage records)
    └── cdr-voice (Voice call records)
    ↓
Downstream Consumers
Components:

SFTP Server: Stores CDR files in CSV format (cdr_data_*.csv, cdr_voice_*.csv)
CDR Streamer: Python application that downloads, parses, and streams CDR files
Redpanda: Kafka-compatible streaming platform storing CDR events
Topics:

cdr-data: Data usage records (msisdn, tower_id, bytes, data_type, etc.)
cdr-voice: Voice call records (msisdn, tower_id, call_type, duration, etc.)




CDR Data Formats
CDR Data Fields
csvmsisdn,tower_id,up_bytes,down_bytes,data_type,ip_address,website_url,event_datetime
9724515726732,1800,787119,662662,application,221.128.139.195,http://example.com,2024-01-01 01:01:15.291148
Fields:

msisdn: Mobile phone number (subscriber identifier)
tower_id: Cell tower ID that handled the session
up_bytes: Bytes uploaded during session
down_bytes: Bytes downloaded during session
data_type: Type of data (application, text, image, video)
ip_address: IP address of the remote server
website_url: URL accessed during the session
event_datetime: Timestamp of the data session (microsecond precision)

CDR Voice Fields
csvmsisdn,tower_id,call_type,dest_nr,call_duration_sec,start_time
5837025539272,724,voice,1693098090050,262,2024-01-01 01:01:29.707413
Fields:

msisdn: Mobile phone number making the call
tower_id: Cell tower ID that handled the call
call_type: Type of call (voice or video)
dest_nr: Destination phone number
call_duration_sec: Call duration in seconds
start_time: Call start timestamp (microsecond precision)

Implementation Steps
Step 1: Create Project Structure
Create the cdr-to-redpanda folder:
bashmkdir cdr-to-redpanda
cd cdr-to-redpanda

Step 2: Create main.py
Create main.py with the CDR streaming logic:
pythonimport os
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
        
        logger.info(f"✅ Completed {filename}: {records_sent} records sent to {topic}")
        
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
Key Features:

SFTP Connection: Uses Paramiko to securely connect to SFTP server
File Tracking: Maintains processed_files.txt to avoid reprocessing
Smart Routing: Routes data files to cdr-data topic, voice files to cdr-voice topic
Metadata Enrichment: Adds source filename, ingestion timestamp, and record type
Progress Logging: Logs every 1000 records for monitoring
Error Handling: Comprehensive error handling and logging
Resource Cleanup: Properly closes connections and deletes temporary files

Step 3: Create requirements.txt
bashcat > requirements.txt << 'EOF'
paramiko==3.4.0
kafka-python==2.0.2
EOF
Dependencies:

paramiko: SSH/SFTP client library for Python
kafka-python: Python client for Apache Kafka (works with Redpanda)


Step 4: Create Dockerfile
bashcat > Dockerfile << 'EOF'
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "main.py"]
EOF
Docker Configuration:

Base image: Python 3.10 slim (lightweight)
Installs dependencies from requirements.txt
Copies main.py into container
Runs the Python script on startup


Step 5: Add Service to docker-compose.yml
Add this service to your docker-compose.yml file:
yaml  # CDR to Redpanda Streamer
  cdr-to-redpanda:
    build: ./cdr-to-redpanda
    container_name: cdr-to-redpanda
    networks:
      - wtc_cap_one
    depends_on:
      sftp:
        condition: service_started
      redpanda-0:
        condition: service_healthy
      redpanda-1:
        condition: service_healthy
      redpanda-2:
        condition: service_healthy
    environment:
      SFTP_HOST: sftp
      SFTP_PORT: 22
      SFTP_USER: cdr_data
      SFTP_PASSWORD: password
      SFTP_REMOTE_PATH: /upload
      KAFKA_BROKERS: redpanda-0:9092,redpanda-1:9092,redpanda-2:9092
      CDR_DATA_TOPIC: cdr-data
      CDR_VOICE_TOPIC: cdr-voice
    deploy:
      resources:
        limits:
          cpus: "1"
          memory: "512m"
        reservations:
          cpus: "0.5"
          memory: "256m"
Service Configuration:

depends_on: Waits for SFTP and Redpanda to be healthy
environment: Configures connection details and topic names
resources: Limits CPU (1 core) and memory (512MB)

Note: The cdr dependency was intentionally removed to allow the streamer to start processing files as they're generated rather than waiting for all files to complete.

Step 6: Build and Run
bash# Navigate back to project root
cd ..

# Build the service
docker compose build cdr-to-redpanda

# Start the service
docker compose up -d cdr-to-redpanda

# Watch the logs
docker logs cdr-to-redpanda -f

Verification
Check Container Status
bashdocker ps | grep cdr-to-redpanda
Expected: Container should show "Exited (0)" after completing successfully.
View Logs
bashdocker logs cdr-to-redpanda --tail 50
Expected Log Messages:
Starting CDR to Redpanda Streamer...
Connected to SFTP server: sftp
Connected to Kafka brokers: ['redpanda-0:9092', 'redpanda-1:9092', 'redpanda-2:9092']
Found 1790 CDR files on SFTP server
Processing file 1/1790: cdr_data_20240102_224000.csv
Downloading: cdr_data_20240102_224000.csv
Streamed 1000 records from cdr_data_20240102_224000.csv to cdr-data
✅ Completed cdr_data_20240102_224000.csv: 1234 records sent to cdr-data
...
✅ All CDR files have been streamed to Redpanda!
Connections closed
Verify Topics in Redpanda
Option A: Using Redpanda Console (Web UI)

Open browser: http://localhost:18084
Click "Topics" in left menu
Verify topics exist:

cdr-data
cdr-voice


Click on a topic → "Messages" tab
View CDR events with metadata

Option B: Using Command Line
bash# List all topics
docker exec -it redpanda-0 rpk topic list | grep cdr

# Describe cdr-data topic
docker exec -it redpanda-0 rpk topic describe cdr-data

# Describe cdr-voice topic
docker exec -it redpanda-0 rpk topic describe cdr-voice

# Consume sample messages from cdr-data
docker exec -it redpanda-0 rpk topic consume cdr-data --num 5

# Consume sample messages from cdr-voice
docker exec -it redpanda-0 rpk topic consume cdr-voice --num 5
Expected Topic Details:
NAME          cdr-data
PARTITIONS    12
REPLICAS      3

Message Format in Redpanda
CDR Data Message Example
json{
  "msisdn": "9724515726732",
  "tower_id": "1800",
  "up_bytes": "787119",
  "down_bytes": "662662",
  "data_type": "application",
  "ip_address": "221.128.139.195",
  "website_url": "http://www.kelly-williams.biz/",
  "event_datetime": "2024-01-01 01:01:15.291148",
  "_source_file": "cdr_data_20240102_224000.csv",
  "_ingestion_timestamp": "2025-12-06T20:55:06.123456",
  "_record_type": "data"
}
CDR Voice Message Example
json{
  "msisdn": "5837025539272",
  "tower_id": "724",
  "call_type": "voice",
  "dest_nr": "1693098090050",
  "call_duration_sec": "262",
  "start_time": "2024-01-01 01:01:29.707413",
  "_source_file": "cdr_voice_20240102_235320.csv",
  "_ingestion_timestamp": "2025-12-06T20:55:07.234567",
  "_record_type": "voice"
}
Metadata Fields Added:

_source_file: Original CSV filename
_ingestion_timestamp: When the record was ingested (ISO 8601 format)
_record_type: Type of CDR record (data or voice)


How It Works
Processing Flow

Startup & Wait: Application waits 30 seconds for dependencies to be ready
Load State: Reads processed_files.txt to skip already-processed files
Connect: Establishes connections to SFTP and Redpanda
List Files: Lists all CDR files (cdr_*.csv) from SFTP /upload directory
Sort Files: Sorts files alphabetically (chronological order by filename)
Process Each File:

Download file to /tmp/
Determine topic based on filename (data vs voice)
Parse CSV line by line
Add metadata fields
Stream to appropriate Redpanda topic
Log progress every 1000 records
Mark file as processed
Delete temporary file


Cleanup: Close all connections and exit

Idempotency
The system maintains a processed_files.txt log to track which files have been processed. If the container restarts, it will:

Skip files already in the log
Only process new files
Prevent duplicate data in Redpanda topics

Troubleshooting
Issue: Container exits immediately
Check logs:
bashdocker logs cdr-to-redpanda
Common causes:

SFTP connection failed (check credentials)
Redpanda not healthy yet (increase wait time)
Python dependencies missing (rebuild image)

Issue: No files found on SFTP
Verify SFTP server has files:
bash# List files in SFTP upload directory
ls -la ./volumes/data/sftp/home/cdr_data/upload/ | grep cdr
Check SFTP connection manually:
bashdocker exec -it sftp ls /upload
Issue: Connection to Redpanda fails
Check Redpanda is healthy:
bashdocker ps | grep redpanda
All should show "Healthy" status.
Verify network connectivity:
bashdocker exec -it cdr-to-redpanda ping redpanda-0
Issue: Processing is very slow
Check resource limits:
bashdocker stats cdr-to-redpanda
Consider increasing CPU/memory limits in docker-compose.yml.
Monitor progress:
bash# Watch logs in real-time
docker logs cdr-to-redpanda -f

# Count processed files
docker exec -it cdr-to-redpanda wc -l processed_files.txt


Performance Considerations
Batch Processing

Files are processed sequentially to maintain order
Each file is streamed record-by-record (not batched in memory)
Logs every 1000 records for progress tracking

Resource Usage

Memory: ~256MB baseline, scales with file size
CPU: Single-threaded processing, ~0.5 cores average
Network: Depends on file size and count
Disk: Temporary storage in /tmp/ (deleted after processing)

Scalability
To improve throughput:

Increase CPU limits in docker-compose.yml
Process multiple files in parallel (modify code)
Increase Kafka producer batch size
Use faster serialization (Avro instead of JSON)


Monitoring
Key Metrics to Track

Files Processed: Count entries in processed_files.txt
Records Streamed: Total messages in Redpanda topics
Processing Time: Time from start to completion
Error Rate: Check logs for ERROR messages
Topic Lag: Monitor Redpanda consumer lag

Logging
All logs use standard Python logging format:
2025-12-06 20:55:06,328 - __main__ - INFO - Downloading: cdr_data_20240102_224000.csv
Log Levels:

INFO: Normal operations (file downloads, progress)
WARNING: Unusual but handled situations (unknown file types)
ERROR: Failures that stop processing (connection errors)


Next Steps
Now that CDR data is streaming to Redpanda:

Build TBD 6 (Persistence System) to save CDR events to PostgreSQL
Build TBD 3 (Stream Processing) to summarize CDR data in real-time
Create prepared layers (CDR Data 1 & 2) for analytics
Implement TBD 5 (Usage API) to query usage data


Project Files Summary
cdr-to-redpanda/
├── main.py              # CDR streaming application (6130 bytes)
├── requirements.txt     # Python dependencies (36 bytes)
└── Dockerfile           # Container definition (155 bytes)


Task Completion Checklist
markdown## Task 1: TBD 1 - CDR to Redpanda
- [x] Create cdr-to-redpanda folder
- [x] Write main.py with SFTP download logic
- [x] Write main.py with Kafka producer logic
- [x] Create requirements.txt
- [x] Create Dockerfile
- [x] Add service to docker-compose.yml
- [x] Build Docker image
- [x] Start service and verify logs
- [x] Verify topics created in Redpanda (cdr-data, cdr-voice)
- [x] Verify messages flowing to topics
- [x] Write documentation
Status: ✅ COMPLETE
Date Completed: December 7, 2025
Implementation Notes:

Processed 1790 CDR files (895 data files + 895 voice files)
Successfully streamed all records to Redpanda topics
Topics created with 12 partitions and 3 replicas for high throughput
Metadata enrichment enabled for downstream traceability
File tracking implemented for idempotent re-runs


References

Paramiko Documentation
kafka-python Documentation
Redpanda Documentation
Python CSV Module