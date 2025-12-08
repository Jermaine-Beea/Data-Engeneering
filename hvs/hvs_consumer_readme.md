# CDR Stream Processor & High Velocity Store (ScyllaDB)

## Overview

This component consumes CDR records from Redpanda topics in real-time, processes them, and stores aggregated daily summaries in ScyllaDB (High Velocity Store). It provides the backend data layer for the Usage API.

## What It Does

The `consumer.py` script:

1. **Consumes from Redpanda topics**: `cdr_data` and `cdr_voice`
2. **Processes records in real-time**: Calculates costs and aggregates usage
3. **Updates ScyllaDB**: Stores daily summaries per MSISDN
4. **Exposes Prometheus metrics**: For monitoring throughput and health

## Architecture

```
Redpanda Topics
    ├─→ cdr_data ──┐
    └─→ cdr_voice ─┤
                   ↓
            consumer.py (Stream Processor)
                   ↓
            ScyllaDB (HVS)
         [daily_data_summary]
         [daily_voice_summary]
                   ↓
            Usage API (REST)
                   ↓
            External Customers
```

## Data Model

### ScyllaDB Keyspace & Tables

**Keyspace**: `cdr_keyspace`

**Table 1: daily_data_summary**
```sql
CREATE TABLE IF NOT EXISTS daily_data_summary (
    msisdn TEXT,
    event_date DATE,
    data_type TEXT,
    total_up_bytes COUNTER,
    total_down_bytes COUNTER,
    total_cost_wak COUNTER,
    PRIMARY KEY ((msisdn), event_date, data_type)
);
```

**Table 2: daily_voice_summary**
```sql
CREATE TABLE IF NOT EXISTS daily_voice_summary (
    msisdn TEXT,
    event_date DATE,
    call_type TEXT,
    total_duration_sec COUNTER,
    total_cost_wak COUNTER,
    PRIMARY KEY ((msisdn), event_date, call_type)
);
```

> **Note**: These tables use ScyllaDB's **COUNTER** type, which provides atomic increment operations perfect for real-time aggregation.

## Processing Logic

### Data CDR Processing
1. Parse `event_datetime` to extract usage date
2. Extract `msisdn`, `data_type`, `up_bytes`, `down_bytes`
3. **Calculate cost**: `(total_bytes × 49.0 ZAR) / 1GB = cost_wak`
4. Update ScyllaDB with incremental values (counter-like pattern)

### Voice CDR Processing
1. Parse `start_time` to extract usage date
2. Extract `msisdn`, `call_type`, `call_duration_sec`
3. **Calculate cost**: `(seconds / 60) × 1.0 ZAR = cost_wak`
4. Update ScyllaDB with incremental values

### Pricing Model

| Type | Unit | Rate (ZAR) | Rate (WAK) |
|------|------|------------|------------|
| Data | 1 GB | 49.00 | 49.00* |
| Voice | 1 minute | 1.00 | 1.00* |

*Configurable via `WAK_PER_ZAR` environment variable

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BOOTSTRAP` | `localhost:19092` | Redpanda broker address |
| `TOPIC_DATA` | `cdr_data` | Data usage topic |
| `TOPIC_VOICE` | `cdr_voice` | Voice calls topic |
| `SCYLLA_HOST` | `localhost` | ScyllaDB host |
| `SCYLLA_PORT` | `9042` | ScyllaDB CQL port |
| `WAK_PER_ZAR` | `1.0` | Currency conversion rate |
| `BYTES_PER_GB` | `1000000000` | Bytes per gigabyte (decimal) |

## Prerequisites

### 1. ScyllaDB Setup

**Initialize schema using the provided `schema.cql` file:**

```bash
# Start ScyllaDB
docker-compose up -d scylla

# Wait for ScyllaDB to be ready (~30-60 seconds)
docker exec -it scylla nodetool status

# Apply the schema
docker cp schema.cql scylla:/tmp/schema.cql
docker exec -it scylla cqlsh -f /tmp/schema.cql
```

**Verify tables were created:**
```bash
docker exec -it scylla cqlsh -e "DESCRIBE cdr_keyspace;"
```

### 2. Python Dependencies

```txt
kafka-python
cassandra-driver
prometheus-client
```

## Running the Stream Processor

### Option 1: Docker Compose (Recommended)

```bash
# Start ScyllaDB first
docker-compose up -d scylla

# Wait for ScyllaDB to be ready (takes ~30 seconds)
docker exec -it scylla nodetool status

# Create tables (run the SQL above)
docker exec -it scylla cqlsh -f /path/to/schema.sql

# Start the stream processor
docker-compose up -d stream-processor

# View logs
docker-compose logs -f stream-processor
```

### Option 2: Direct Execution (Local Dev)

```bash
# Set environment variables
export BOOTSTRAP=localhost:19092
export SCYLLA_HOST=localhost
export SCYLLA_PORT=9042

# Run the consumer
python consumer.py
```

## Monitoring

### Prometheus Metrics

The consumer exposes metrics on **port 8000**:

**Metrics exposed:**
- `cdr_data_updates_total` - Counter of data CDR updates
- `cdr_voice_updates_total` - Counter of voice CDR updates  
- `cdr_last_update_timestamp` - Timestamp of last processed record

**Access metrics:**
```bash
curl http://localhost:8000/metrics
```

### Log Output

```
2024-12-07 10:05:32 [INFO] Unified consumer started. Listening to topics: ['cdr_data', 'cdr_voice']
2024-12-07 10:05:33 [INFO] Updated data summary for +27821234567 on 2024-01-01 [video]
2024-12-07 10:05:34 [INFO] Updated voice summary for +27821234567 on 2024-01-01 [voice]
```

### Verify Data in ScyllaDB

```sql
-- Connect to ScyllaDB
docker exec -it scylla cqlsh

USE cdr_keyspace;

-- Check data summaries
SELECT * FROM daily_data_summary WHERE msisdn = '+27821234567' LIMIT 10;

-- Check voice summaries
SELECT * FROM daily_voice_summary WHERE msisdn = '+27821234567' LIMIT 10;

-- Count total records
SELECT COUNT(*) FROM daily_data_summary;
SELECT COUNT(*) FROM daily_voice_summary;
```

## How It Works

### 1. Consumer Initialization
```python
consumer = KafkaConsumer(
    'cdr_data', 'cdr_voice',
    bootstrap_servers=['redpanda-0:9092'],
    group_id='cdr-summary-unified',
    auto_offset_reset='earliest'
)
```

### 2. Message Processing Loop
```python
for msg in consumer:
    if topic == "cdr_data":
        # Process data usage
        calculate_data_cost()
        update_scylladb()
    elif topic == "cdr_voice":
        # Process voice call
        calculate_voice_cost()
        update_scylladb()
```

### 3. ScyllaDB Updates (Atomic Counters)
Uses **COUNTER** updates for atomic, lock-free increments:
```python
# Counter updates are atomic - no read-modify-write needed!
upd_data = session.prepare("""
UPDATE daily_data_summary
SET total_up_bytes = total_up_bytes + ?, 
    total_down_bytes = total_down_bytes + ?, 
    total_cost_wak = total_cost_wak + ?
WHERE msisdn = ? AND event_date = ? AND data_type = ?;
""")
```

**Why COUNTER type?**
- ✅ Atomic increments (thread-safe, no race conditions)
- ✅ No read-before-write overhead
- ✅ Perfect for real-time aggregation
- ✅ Handles high concurrency elegantly

## Performance Characteristics

### ScyllaDB (HVS) Benefits
- **Low Latency**: Sub-millisecond reads/writes
- **High Throughput**: Handles 1M+ ops/sec per node
- **Horizontal Scalability**: Add nodes for more capacity
- **Cassandra-compatible**: Uses CQL (Cassandra Query Language)

### Consumer Performance
- **Processing Rate**: ~1000-5000 records/sec (single instance)
- **Offset Management**: Auto-commit enabled
- **Error Handling**: Skips malformed messages, logs warnings

## Usage API Integration

The Usage API reads from these ScyllaDB tables to serve real-time queries:

**Example Query Flow:**
```
Customer → GET /usage/{msisdn}?date=2024-01-01
          ↓
    Usage API (REST)
          ↓
    ScyllaDB Query:
    SELECT * FROM daily_data_summary 
    WHERE msisdn = '+27821234567' 
    AND event_date = '2024-01-01'
          ↓
    JSON Response
```

## Troubleshooting

### Consumer Not Starting

**Issue**: `NoBrokersAvailable`
```bash
# Check Redpanda status
docker-compose ps redpanda-0

# Verify connectivity
docker exec -it redpanda-0 rpk cluster health
```

### ScyllaDB Connection Refused

**Issue**: `cassandra.cluster.NoHostAvailable`
```bash
# Check ScyllaDB status
docker exec -it scylla nodetool status

# Verify port 9042 is exposed
docker-compose ps scylla
```

### No Data in ScyllaDB

**Check if:**
1. CDR generator is running (`docker-compose logs cdr`)
2. Topics have messages (`rpk topic consume cdr_data --num 10`)
3. Consumer is processing (`docker-compose logs stream-processor`)

### High Memory Usage

**Solution**: Tune ScyllaDB memory settings in docker-compose.yml:
```yaml
command: --smp 1 --memory 512M --overprovisioned 1
```

## Schema Initialization Script

Create `init_schema.cql`:

```sql
CREATE KEYSPACE IF NOT EXISTS cdr_keyspace 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE cdr_keyspace;

CREATE TABLE IF NOT EXISTS daily_data_summary (
    msisdn TEXT,
    event_date DATE,
    data_type TEXT,
    total_up_bytes BIGINT,
    total_down_bytes BIGINT,
    total_cost_wak BIGINT,
    PRIMARY KEY ((msisdn), event_date, data_type)
) WITH CLUSTERING ORDER BY (event_date DESC, data_type ASC);

CREATE TABLE IF NOT EXISTS daily_voice_summary (
    msisdn TEXT,
    event_date DATE,
    call_type TEXT,
    total_duration_sec BIGINT,
    total_cost_wak BIGINT,
    PRIMARY KEY ((msisdn), event_date, call_type)
) WITH CLUSTERING ORDER BY (event_date DESC, call_type ASC);
```

**Apply schema:**
```bash
docker exec -i scylla cqlsh < init_schema.cql
```

## Docker Configuration

### Dockerfile
```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "consumer.py"]
```

### requirements.txt
```txt
kafka-python==2.0.2
cassandra-driver==3.28.0
prometheus-client==0.19.0
```

## Next Steps

1. ✅ Stream processor running and updating ScyllaDB
2. 🔄 Build Usage API to query ScyllaDB
3. 🔄 Add authentication (Basic Auth)
4. 🔄 Create REST endpoints for customers

---

**Related Components:**
- [CDR Generator](./cdr/README.md) - Generates and uploads CDR data
- [Usage API](./usage-api/README.md) - REST API for querying usage summaries
