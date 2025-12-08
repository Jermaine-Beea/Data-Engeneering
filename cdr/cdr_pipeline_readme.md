# CDR Data Pipeline

## Overview

This pipeline generates synthetic CDR (Call Detail Record) data and streams it to both an SFTP server and Redpanda (Kafka). It simulates a real-world telecom data generation system that produces voice and data usage records.

## What It Does

The `main.py` script:

1. **Generates synthetic CDR data** using the Faker library
2. **Creates two types of records**:
   - **CDR Data**: Mobile data usage (up/down bytes, IP addresses, websites visited)
   - **CDR Voice**: Voice/video call records (call duration, destination numbers)
3. **Uploads files to SFTP server** for archival/batch processing
4. **Streams records to Redpanda topics** for real-time processing
5. **Tracks progress** using an index file to support resumable execution

## Architecture

```
main.py → Generate CDR Records
    ↓
    ├─→ Write to CSV files
    ├─→ Upload to SFTP (sftp:22)
    └─→ Stream to Redpanda (localhost:19092)
        ├─→ Topic: cdr_data
        └─→ Topic: cdr_voice
```

## Configuration

### Environment Detection

The script automatically detects the environment:
- **Dev Mode** (`USER` env var is set): Generates less data, uses localhost SFTP on port 10022
- **Prod Mode** (running in Docker): Generates full data load, uses SFTP service on port 22

### Key Parameters

| Parameter | Dev | Prod | Description |
|-----------|-----|------|-------------|
| `TOTAL_SECONDS` | 86,400 (1 day) | 172,800 (2 days) | Total time period to simulate |
| `LINE_COUNT` | 1,000 | 1,000 | Records per file |
| `LINES_PER_SECOND` | 1 | 5 | Records generated per second |
| `FILE_COUNT` | Calculated | Calculated | Total number of files to generate |

### Generated Data

**CDR Data Records:**
- `msisdn`: Mobile phone number
- `tower_id`: Cell tower ID (1-2000)
- `up_bytes`: Upload bytes (100KB - 1MB)
- `down_bytes`: Download bytes (100KB - 1MB)
- `data_type`: video, audio, image, text, application
- `ip_address`: Public IPv4 address
- `website_url`: Visited URL
- `event_datetime`: Timestamp

**CDR Voice Records:**
- `msisdn`: Mobile phone number
- `tower_id`: Cell tower ID (1-2000)
- `call_type`: voice or video
- `dest_nr`: Destination phone number
- `call_duration_sec`: Call duration (1-1800 seconds)
- `start_time`: Call start timestamp

## Prerequisites

### Local Development
```bash
pip install faker paramiko confluent-kafka
```

### Docker (Recommended)
All dependencies are included in the Docker image.

## Running the Pipeline

### Option 1: Docker Compose (Recommended)

```bash
# Start the entire stack
docker-compose up -d

# Run just the CDR generator
docker-compose up cdr

# View logs
docker-compose logs -f cdr
```

### Option 2: Using the Bash Script

```bash
# Make the script executable
chmod +x cdr_to_redpanda_script.sh

# Run the pipeline
./cdr_to_redpanda_script.sh
```

### Option 3: Direct Execution (Local Dev)

```bash
python main.py
```

## How It Works

### 1. Initialization
- Loads last processed index from `idx_data.dat` (for resumable execution)
- Generates lookup data (MSISDNs, IP addresses, URLs, destination numbers)
- Connects to Redpanda producer

### 2. Generation Loop
For each time interval:
- Generates `LINE_COUNT` CDR data records
- Generates `LINE_COUNT` CDR voice records
- Writes records to CSV files
- Uploads CSVs to SFTP server with timestamped filenames
- Streams each record to corresponding Redpanda topic
- Saves progress index

### 3. File Naming Convention
- CDR Data: `cdr_data_YYYYMMDD_HHMMSS.csv`
- CDR Voice: `cdr_voice_YYYYMMDD_HHMMSS.csv`

Example: `cdr_data_20240101_120000.csv`

## Monitoring

### Check Progress
The script logs detailed information:
```
2024-12-07 10:00:00 - INFO - Generated 2 cdr files...
2024-12-07 10:00:01 - INFO - Completed data generation. cdr_data [864000], cdr_voice [864000], files [864]
```

### Verify SFTP Uploads
```bash
# Connect to SFTP
sftp -P 10022 cdr_data@localhost

# List files
ls
```

### Verify Redpanda Topics
Access Redpanda Console at: http://localhost:18084

Or use CLI:
```bash
# List topics
docker exec -it redpanda-0 rpk topic list

# Consume messages
docker exec -it redpanda-0 rpk topic consume cdr_data --num 10
```

## Resumable Execution

The pipeline saves its progress in `idx_data.dat`. If the script crashes or is stopped:

1. Simply restart it
2. It will skip already-processed files
3. Continue from the last saved index

## Performance Notes

### Dev Mode
- Generates ~86,400 records per day
- Minimal resource usage
- Great for testing

### Prod Mode
- Generates ~432,000 records per day
- Higher throughput (5 records/sec)
- Requires adequate resources for SFTP and Redpanda

## Troubleshooting

### SFTP Connection Failures
```
An error occurred: [Errno 111] Connection refused
```
**Solution**: Ensure SFTP container is running
```bash
docker-compose up -d sftp
```

### Redpanda Connection Issues
**Solution**: Wait for Redpanda cluster to be healthy
```bash
docker-compose ps
# All redpanda services should show "healthy"
```

### Out of Memory
**Solution**: Reduce `LINE_COUNT` or adjust Docker memory limits in `docker-compose.yml`

## Dependencies

- **faker**: Synthetic data generation
- **paramiko**: SFTP client
- **confluent-kafka**: Kafka/Redpanda producer
- **Python 3.10+**

## Docker Configuration

### Dockerfile
```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "main.py"]
```

### Requirements
```
faker
paramiko
confluent-kafka
```

## Related Services

- **SFTP Server**: Port 10022 (dev) / 22 (prod)
- **Redpanda Brokers**: 19092, 29092, 39092
- **Redpanda Console**: http://localhost:18084
- **Stream Processor**: Consumes from topics and processes data

## License

Internal use only.
