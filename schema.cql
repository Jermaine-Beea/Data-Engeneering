-- 1) Create keyspace (adjust replication for your cluster)
CREATE KEYSPACE IF NOT EXISTS cdr_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- 2) Use the keyspace
USE cdr_keyspace;

-- 3) Raw tables (already defined)
-- cdr_data and cdr_voice remain as your source-of-truth event logs

-- 4) Daily data usage summary (aggregated per MSISDN, per day, per data_type)
CREATE TABLE IF NOT EXISTS daily_data_summary (
    msisdn TEXT,
    event_date DATE,
    data_type TEXT,
    total_up_bytes COUNTER,
    total_down_bytes COUNTER,
    total_cost_wak COUNTER,
    PRIMARY KEY ((msisdn), event_date, data_type)
);

-- 5) Daily voice usage summary (aggregated per MSISDN, per day, per call_type)
CREATE TABLE IF NOT EXISTS daily_voice_summary (
    msisdn TEXT,
    event_date DATE,
    call_type TEXT,
    total_duration_sec COUNTER,
    total_cost_wak COUNTER,
    PRIMARY KEY ((msisdn), event_date, call_type)
);