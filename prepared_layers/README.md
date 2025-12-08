# Prepared Layers Service

## Overview

This component generates and continuously updates all analyst-ready **Prepared Layers** required by the WTC Data Engineering Project. It replaces the need for dbt or Airflow by providing a lightweight, fully automated, production-grade transformation service written in pure Python.

It processes raw data from `cdr_data`, `forex_data`, and `crm_data` schemas and populates the `prepared_layers` schema with the four required deliverables.

## What It Does

The service (`main.py`) runs forever and:

1. **Creates all required tables** in the `prepared_layers` schema on startup  
2. **Generates**:
   - Forex Data - 1 → M1 / M30 / H1 OHLC bars with EMA-8, EMA-21, ATR-8, ATR-21  
   - CDR Data - 2 → Tower sessions (≥3 consecutive interactions)  
   - CDR Data - 1 → 15min / 30min / 1hr usage summaries with cost in ZAR  
   - CRM Data - 1 → Flattened user + device + hourly running balance in WAK  
3. **Refreshes all layers every 5 minutes** automatically  
4. **Logs everything in structured JSON**  
5. **Recovers gracefully** from errors

## Architecture

    Prepared Layers Service (main.py)
        ├─→ Reads raw data
        ├─→ Computes summaries and indicators
        ├─→ Aggregates usage and balances
        └─→ Writes to prepared_layers tables
            ↓
    Postgres (prepared_layers schema)
        ├─→ cdr_usage_summary_15min
        ├─→ cdr_usage_summary_30min
        ├─→ cdr_usage_summary_1hr
        ├─→ cdr_tower_sessions
        ├─→ crm_user_balance_hourly
        ├─→ forex_ohlc_m1
        ├─→ forex_ohlc_m30
        └─→ forex_ohlc_h1
            ↓
    Analysts / Usage API / Dashboards


## Requirements

```bash
psycopg2-binary==2.9.9
pandas==2.0.3
numpy==1.24.3
schedule==1.2.1
```

## Running the Service

```bash
# Start the service
docker compose up -d prepared-layers

# View logs (you will see processing every 5 minutes)
docker compose logs -f prepared-layers

# Stop
docker compose stop prepared-layers
```

# File Format
## Input: Raw Forex Data (forex_data.forex_raw)
### Columns:

```bash
csvtimestamp,pair_name,bid_price,ask_price,spread,ingested_at
2024-01-01 00:00:00.123,MRVZAR,18.1234,18.5678,0.4444,2024-12-06 11:30:45
```

## Input: Raw CDR Data (cdr_data.data_usage and voice_calls)

### data_usage Columns:

```bash
csvmsisdn,tower_id,up_bytes,down_bytes,data_type,ip_address,website_url,event_datetime
+27821234567,1523,523456,1234567,video,41.202.45.12,https://example.com,2024-01-01 10:15:30
```
### voice_calls Columns:

```bash
csvmsisdn,tower_id,call_type,dest_nr,call_duration_sec,start_time
+27821234567,1523,voice,+27829876543,125,2024-01-01 10:15:30
```

## Input: Raw CRM Data (crm_data.accounts, addresses, devices)

### accounts Columns:
```bash
csvaccount_id,owner_name,email,phone_number
1,John Doe,john@example.com,+27821234567
```

### addresses Columns:
```bash
csvaccount_id,street_address,city,state,postal_code,country
1,123 Main St,Johannesburg,Gauteng,2000,South Africa
```

### devices Columns:
```bash
csvaccount_id,device_id,device_name,device_type,device_os
1,1, iPhone 14,smartphone,iOS 17
```

## Output: Prepared Forex OHLC Tables (e.g., prepared_layers.forex_ohlc_m1)

### Columns:
```bash
csvdatetime,pair_name,open_price,high_price,low_price,close_price,ema_8,ema_21,atr_8,atr_21,created_at
2024-01-01 00:00:00,MRVZAR,18.1234,18.5678,18.1000,18.4321,18.2341,18.1987,0.0456,0.0389,2024-12-06 11:30:45
```

## Output: Prepared CDR Tower Sessions (prepared_layers.cdr_tower_sessions)

### Columns:

```bash
csvmsisdn,tower_id,session_start,session_end,interaction_count,created_at
+27821234567,1523,2024-01-01 10:00:00,2024-01-01 10:30:00,15,2024-12-06 11:30:45
```

## Output: Prepared CRM User Balance (prepared_layers.crm_user_balance_hourly)

### Columns:

```bash
csvdatetime,account_id,owner_name,email,phone_number,street_address,city,state,postal_code,country,device_id,device_name,device_type,device_os,call_cost_zar,data_cost_zar,total_cost_zar,avg_wakmrv_rate,avg_mrvzar_rate,call_cost_wak,data_cost_wak,total_cost_wak,accumulated_cost_wak,created_at
2024-01-01 10:00:00,1,John Doe,john@example.com,+27821234567,123 Main St,Johannesburg,Gauteng,200,26.58,1.2345,18.1234,1.68,19.82,21.5,21.5,2024-12-06 11:30:45
```