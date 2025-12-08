# CDR SFTP to Redpanda Ingester

## Overview

This component downloads CDR (Call Detail Record) files from an SFTP server and streams them to Redpanda topics for real-time processing. It acts as the ingestion bridge between batch file storage (SFTP) and streaming infrastructure (Redpanda/Kafka).

## What It Does

The ingester (`main.py`):

1. **Connects to SFTP server** and lists available CDR files
2. **Downloads CSV files** (`cdr_data_*.csv` and `cdr_voice_*.csv`)
3. **Parses records** and adds metadata (source file, ingestion timestamp)
4. **Streams to Redpanda** topics (`cdr-data` and `cdr-voice`)
5. **Tracks processed files** to avoid duplicate ingestion
6. **Cleans up** temporary downloaded files

## Architecture

```
SFTP Server (Port 22/10022)
   └── /upload/
       ├── cdr_data_20240101_000000.csv
       ├── cdr_data_20240101_010000.csv
       ├── cdr_voice_20240101_000000.csv
       └── cdr_voice_20240101_010000.csv
                ↓
        CDR Ingester (main.py)
          ├─→ Downloads files
          ├─→ Parses CSV records
          ├─→ Adds metadata
          └─→ Streams to topics
                ↓
        Redpanda Topics
          ├─→ cdr-data (3 partitions, 3 replicas)
          └─→ cdr-voice (3 partitions, 3 replicas)
                ↓
        Stream Processor
                ↓
        ScyllaDB (HVS)
```

## File Format

### Input: CDR Data Files
**Filename pattern**: `cdr_data_YYYYMMDD_HHMMSS.csv`

**Columns**:
```csv
msisdn,tower_id,up_bytes,down_bytes,data_type,ip_address,website_url,event_datetime
+27821234567,1523,523456,1234567,video,41.202.45.12,https://example.com,2024-01-01 10:15:30
```

### Input: CDR Voice Files
**Filename pattern**: `cdr_voice_YYYYMMDD_HHMMSS.csv`

**Columns**:
```csv
msisdn,tower_id,call_type,dest_nr,call_duration_sec,start_time
+27821234567,1523,voice,+27829876543,125,2024-01-01 10:15:30
```

### Output: Enhanced Records
The ingester adds metadata to each record:
```json
{
  "msisdn": "+27821234567",
  "tower_id": "1523",
  "up_bytes": "523456",
  "down_bytes": "1234567",
  "data_type": "video",
  "ip_address": "41.202.45.12",
  "website_url": "https://example.com",
  "event_datetime": "2024-01-01 10:15:30",
  "_source_file": "cdr_data_20240101_101530.csv",
  "_ingestion_timestamp": "2024-12-07T15:23:45.123456",
  "_record_type": "data"
}
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SFTP_HOST` | `sftp` | SFTP server hostname |
| `SFTP_PORT` | `22` | SFTP server port |
| `SFTP_USER` | `cdr_data` | SFTP username |
| `SFTP_PASSWORD` | `password` | SFTP password |
| `SFTP_REMOTE_PATH` | `/upload` | Remote directory containing CDR files |
| `KAFKA_BROKERS` | `redpanda-0:9092,...` | Comma-separated Kafka broker list |
| `CDR_DATA_TOPIC` | `cdr-data` | Topic for data usage records |
| `CDR_VOICE_TOPIC` | `cdr-voice` | Topic for voice call records |

## Prerequisites

### 1. SFTP Server with CDR Files

```bash
# Verify SFTP server is running
docker-compose ps sftp

# Check files are available
docker exec sftp ls /home/cdr_data/
```

### 2. Redpanda Cluster

```bash
# Verify Redpanda is healthy
docker exec redpanda-0 rpk cluster health

# Should show: Healthy: true
```

### 3. Python Dependencies

```txt
kafka-python==2.0.2
paramiko==3.4.0
```

## Running the Ingester

### Option 1: Using the Setup Script (Recommended ⭐)

The easiest way to run the ingester:

```bash
# Make the script executable
chmod +x cdr_sftp_ingester__to_redpanda_script.sh

# Run the ingester
./cdr_sftp_ingester__to_redpanda_script.sh
```

**What the script does:**
1. ✅ Checks SFTP and Redpanda are running
2. ✅ Creates Redpanda topics if they don't exist
3. ✅ Starts the ingester container
4. ✅ Shows live logs and status
5. ✅ Displays record counts in topics

**After running the script:**
```bash
# View live logs
docker-compose logs -f cdr-ingester

# Check which files were processed
docker exec cdr-ingester cat /app/processed_files.txt

# View messages in topics
docker exec redpanda-0 rpk topic consume cdr-data --num 10
```

### Option 2: Docker Compose

```bash
# Start the ingester
docker-compose up -d cdr-ingester

# View logs
docker-compose logs -f cdr-ingester

# Stop the ingester
docker-compose stop cdr-ingester
```

### Option 3: Direct Execution (Local Dev)

```bash
# Set environment variables
export SFTP_HOST=localhost
export SFTP_PORT=10022
export KAFKA_BROKERS=localhost:19092,localhost:29092,localhost:39092

# Run the ingester
python main.py
```

## How It Works

### 1. Initialization & Waiting
```python
# Wait 30 seconds for services to be ready
time.sleep(30)

# Load already processed files
processed_files = load_processed_files()
```

### 2. SFTP Connection
```python
transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
sftp = paramiko.SFTPClient.from_transport(transport)
```

### 3. File Discovery & Sorting
```python
files = sftp.listdir(SFTP_REMOTE_PATH)
cdr_files = [f for f in files if f.startswith('cdr_') and f.endswith('.csv')]
cdr_files.sort()  # Process in chronological order
```

### 4. Download & Parse
```python
sftp.get(remote_path, local_path)

with open(local_path, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        row['_source_file'] = filename
        row['_ingestion_timestamp'] = datetime.utcnow().isoformat()
        producer.send(topic, value=row)
```

### 5. Duplicate Prevention
```python
mark_file_as_processed(filename)  # Writes to processed_files.txt
os.remove(local_path)             # Clean up
```

## Monitoring

### Check Ingestion Progress

```bash
# View logs
docker-compose logs -f cdr-ingester

# Expected output:
# INFO - Connected to SFTP server: sftp
# INFO - Found 24 CDR files on SFTP server
# INFO - Processing file 1/20: cdr_data_20240101_000000.csv
# INFO - Streamed 1000 records from cdr_data_20240101_000000.csv to cdr-data
# INFO - Completed cdr_data_20240101_000000.csv: 1000 records sent to cdr-data
```

### Check Processed Files

```bash
# View list of processed files
docker exec cdr-ingester cat /app/processed_files.txt

# Output:
# cdr_data_20240101_000000.csv
# cdr_voice_20240101_000000.csv
# cdr_data_20240101_010000.csv
# ...
```

### Verify Data in Redpanda

```bash
# List topics
docker exec redpanda-0 rpk topic list

# Check topic details
docker exec redpanda-0 rpk topic describe cdr-data

# Consume messages
docker exec redpanda-0 rpk topic consume cdr-data --num 10

# Count messages (approximate)
docker exec redpanda-0 rpk topic describe cdr-data | grep "High watermark"
```

### Redpanda Console

Access the web UI at: **http://localhost:18084**

Navigate to:
- **Topics** → `cdr-data` or `cdr-voice`
- View messages, partitions, and consumer groups

## Processing Guarantees

### At-Least-Once Delivery
- Messages are sent with `acks='all'` (wait for all replicas)
- Retries are configured (`retries=3`)
- Producer flushes after each file

### Duplicate Prevention
- Processed files are tracked in `processed_files.txt`
- Re-running the ingester skips already processed files
- Files are processed in chronological order (sorted by filename)

### Failure Recovery
If the ingester crashes:
1. Restart it: `./run_ingester.sh`
2. It will skip already processed files
3. Continue from where it left off

## Performance Characteristics

### Throughput
- **Single file**: ~1,000-5,000 records/sec
- **Batch processing**: Processes files sequentially
- **Network bound**: Limited by SFTP download speed

### Scalability
To increase throughput:
1. Run multiple ingester instances (partition files by date)
2. Increase Redpanda partitions
3. Use parallel downloads (modify code)

### Resource Usage
- **CPU**: Low (I/O bound)
- **Memory**: ~128-256 MB
- **Network**: Depends on file sizes
- **Disk**: Minimal (temporary files deleted)

## Troubleshooting

### Ingester Not Starting

**Issue**: Container exits immediately
```bash
docker-compose logs cdr-ingester
```

**Common causes**:
- SFTP server not running → `docker-compose up -d sftp`
- Redpanda not ready → `docker-compose up -d redpanda-0`
- Wrong credentials → Check `SFTP_USER` and `SFTP_PASSWORD`

### No Files Found on SFTP

**Issue**: `Found 0 CDR files on SFTP server`

**Solution**: Run the CDR generator first to create files
```bash
docker-compose up cdr  # Generates and uploads files
```

Or manually upload test files:
```bash
docker cp test_data.csv sftp:/home/cdr_data/cdr_data_20240101_000000.csv
```

### Kafka Connection Error

**Issue**: `NoBrokersAvailable` or timeout

**Check Redpanda health**:
```bash
docker exec redpanda-0 rpk cluster health
```

**Verify broker addresses**:
```bash
docker exec redpanda-0 rpk cluster info
```

### Files Processed But No Data in Topics

**Check topic exists**:
```bash
docker exec redpanda-0 rpk topic list | grep cdr
```

**Verify messages were sent**:
```bash
docker exec redpanda-0 rpk topic consume cdr-data --num 1
```

**Check for serialization errors** in logs:
```bash
docker-compose logs cdr-ingester | grep -i error
```

### Duplicate Processing

If you need to reprocess files:
```bash
# Clear processed files log
docker exec cdr-ingester rm /app/processed_files.txt

# Restart ingester
docker-compose restart cdr-ingester
```

## Docker Configuration

### Dockerfile
```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "main.py"]
```

### requirements.txt
```txt
kafka-python==2.0.2
paramiko==3.4.0
```

### Docker Compose Service
```yaml
cdr-ingester:
  build: ./cdr-ingester
  container_name: cdr-ingester
  networks:
    - wtc_cap_one
  depends_on:
    - sftp
    - redpanda-0
  environment:
    SFTP_HOST: sftp
    SFTP_PORT: 22
    KAFKA_BROKERS: redpanda-0:9092,redpanda-1:9092,redpanda-2:9092
```

## Integration with Downstream Systems

### Stream Processor
After ingestion, records flow to the stream processor:
```
cdr-data topic → consumer.py → ScyllaDB daily_data_summary
cdr-voice topic → consumer.py → ScyllaDB daily_voice_summary
```

### Usage API
The Usage API queries aggregated data in ScyllaDB:
```
Customer Request → Usage API → ScyllaDB → Response
```

## Comparison: Generator vs Ingester

| Feature | CDR Generator | CDR Ingester |
|---------|--------------|--------------|
| **Creates data** | ✅ Yes (Faker) | ❌ No |
| **Reads from SFTP** | ❌ No | ✅ Yes |
| **Writes to SFTP** | ✅ Yes | ❌ No |
| **Streams to Redpanda** | ✅ Yes | ✅ Yes |
| **Use case** | Testing/Dev | Production ingestion |
| **Script** | `run_cdr_generator.sh` | `run_ingester.sh` |

## Best Practices

1. **Monitor processed files**: Regularly check `processed_files.txt` to ensure progress
2. **Run after CDR generation**: If testing, run generator first to populate SFTP
3. **Check Redpanda health**: Always verify cluster is healthy before ingesting
4. **Review logs**: Monitor for errors or warnings during processing
5. **Backup processed files log**: Preserve `processed_files.txt` to avoid reprocessing

## Next Steps

After successful ingestion:
1. ✅ Verify data in Redpanda topics
2. 🔄 Start the stream processor to aggregate data
3. 🔄 Query ScyllaDB for daily summaries
4. 🔄 Use the Usage API to expose data to customers

---

**Related Components:**
- [CDR Generator](../cdr/README.md) - Generates synthetic CDR data
- [Stream Processor](../stream-processor/README.md) - Processes and aggregates CDRs
- [Usage API](../usage-api/README.md) - REST API for usage queries
