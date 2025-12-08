# Usage API

REST API for retrieving daily usage summaries from CDR (Call Detail Records) data with basic authentication.

## Quick Start

```bash
# Start via docker-compose
docker-compose up cdr-usage-api

#Inspect rows directly in Postgres:
# connect from host (compose maps 15432 -> 5432)
psql -h localhost -p 15432 -U postgres -d wtc_analytics -c "SELECT * FROM cdr_data.voice_cdr ORDER BY start_time DESC LIMIT 10;"

# Test health endpoint (no auth)
curl http://localhost:5000/health

# Get usage data (with auth)
curl -u admin:admin123 http://localhost:5000/api/usage/7643008066273

To get usage data from the API, use the Daily Usage Summary or Overall Summary endpoints:

Basic example:


curl -u admin:admin123 http://localhost:5000/api/usage/7643008066273
With date filtering:


curl -u admin:admin123 "http://localhost:5000/api/usage/7643008066273?start_date=2024-01-01&end_date=2024-01-31"
For aggregate summary:


curl -u admin:admin123 http://localhost:5000/api/usage/7643008066273/summary
Replace 7643008066273 with the MSISDN (phone number) you want to query. Authentication uses admin:admin123 by default.
```

## API Endpoints

### Health Check
```
GET /health
```
No authentication required. Returns API status.

### Daily Usage Summary
```
GET /api/usage/<msisdn>?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```
Returns daily aggregated usage for the given MSISDN.

**Parameters:**
- `msisdn` (required): Phone number
- `start_date` (optional): Filter from date
- `end_date` (optional): Filter to date

**Response:**
```json
{
  "msisdn": "7643008066273",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "data": [
    {
      "date": "2024-01-05",
      "total_calls": 15,
      "voice_calls": 8,
      "video_calls": 7,
      "total_duration_seconds": 12345,
      "avg_duration_seconds": 823
    }
  ]
}
```

### Overall Summary
```
GET /api/usage/<msisdn>/summary?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```
Returns aggregated usage summary across the date range.

**Response:**
```json
{
  "msisdn": "7643008066273",
  "summary": {
    "total_calls": 150,
    "voice_calls": 85,
    "video_calls": 65,
    "total_duration_seconds": 123450,
    "active_days": 25,
    "first_call": "2024-01-01T10:05:46",
    "last_call": "2024-01-31T22:15:35"
  }
}
```

## Authentication

Uses HTTP Basic Authentication. Default credentials:
- **Username:** `admin`
- **Password:** `admin123`

**Example with curl:**
```bash
curl -u admin:admin123 http://localhost:5000/api/usage/7643008066273
```

**Example with Python:**
```python
import requests
from requests.auth import HTTPBasicAuth

response = requests.get(
    'http://localhost:5000/api/usage/7643008066273',
    auth=HTTPBasicAuth('admin', 'admin123')
)
print(response.json())
```

## Configuration

Set via environment variables in `docker-compose.yml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `postgres` | Database host |
| `DB_PORT` | `5432` | Database port |
| `DB_USER` | `postgres` | Database user |
| `DB_PASSWORD` | `postgres` | Database password |
| `DB_NAME` | `wtc_analytics` | Database name |
| `API_USERNAME` | `admin` | API username |
| `API_PASSWORD` | `admin123` | API password |
| `PORT` | `5000` | API port |

## Database Schema

Queries the `cdr_data.voice_cdr` table:

```sql
CREATE TABLE cdr_data.voice_cdr (
    msisdn VARCHAR(20) NOT NULL,
    tower_id INTEGER,
    call_type VARCHAR(20),          -- 'voice' or 'video'
    dest_nr VARCHAR(20),
    call_duration_sec INTEGER,
    start_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Issue 2 Compliance

✅ **TBD 5 - Usage API Requirements:**
- REST API for daily usage summaries
- Query by MSISDN
- Basic HTTP authentication
- Real-time data from HVS (PostgreSQL)
- Date range filtering support