import os
import json
import datetime
import logging
import ast
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from prometheus_client import start_http_server, Counter, Gauge

# ---- Config
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:19092")   # Redpanda/Kafka
TOPICS = [os.getenv("TOPIC_DATA", "cdr_data"),
          os.getenv("TOPIC_VOICE", "cdr_voice")]
SCYLLA_HOST = os.getenv("SCYLLA_HOST", "localhost")
SCYLLA_PORT = int(os.getenv("SCYLLA_PORT", "9042"))

WAK_PER_ZAR = float(os.getenv("WAK_PER_ZAR", "1.0"))    # adjust if WAK != ZAR
BYTES_PER_GB = int(os.getenv("BYTES_PER_GB", "1000000000"))  # decimal GB

# ---- Logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# ---- Cassandra / Scylla client
cluster = Cluster(contact_points=[SCYLLA_HOST], port=SCYLLA_PORT)
session = cluster.connect("cdr_keyspace")

# ---- Prepared statements
upd_data = session.prepare("""
UPDATE daily_data_summary
SET total_up_bytes = total_up_bytes + ?, 
    total_down_bytes = total_down_bytes + ?, 
    total_cost_wak = total_cost_wak + ?
WHERE msisdn = ? AND event_date = ? AND data_type = ?;
""")

upd_voice = session.prepare("""
UPDATE daily_voice_summary
SET total_duration_sec = total_duration_sec + ?, 
    total_cost_wak = total_cost_wak + ?
WHERE msisdn = ? AND event_date = ? AND call_type = ?;
""")

# ---- Helpers
def to_usage_date(ts_str):
    """Convert timestamp string to date."""
    try:
        dt = datetime.datetime.fromisoformat(ts_str.replace("Z",""))
    except ValueError:
        dt = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    return dt.date()

def data_cost_wak(total_bytes):
    """Convert bytes to cost in WAK."""
    zar = (total_bytes * 49.0) / BYTES_PER_GB
    return int(round(zar * WAK_PER_ZAR))

def voice_cost_wak(seconds):
    """Convert seconds to cost in WAK."""
    zar = seconds / 60.0
    return int(round(zar * WAK_PER_ZAR))

def safe_load(msg_bytes):
    """Safely load a message as JSON or Python dict string."""
    try:
        return json.loads(msg_bytes.decode("utf-8"))
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(msg_bytes.decode("utf-8"))
        except Exception:
            return None

# ---- Prometheus metrics
cdr_data_updates = Counter('cdr_data_updates_total', 'Number of data CDR updates')
cdr_voice_updates = Counter('cdr_voice_updates_total', 'Number of voice CDR updates')
cdr_last_update = Gauge('cdr_last_update_timestamp', 'Unix timestamp of last CDR processed')

# Start Prometheus metrics server on port 8000
start_http_server(8000)

# ---- Kafka consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[BOOTSTRAP],
    value_deserializer=safe_load,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="cdr-summary-unified"
)

logging.info("Unified consumer started. Listening to topics: %s", TOPICS)

try:
    for msg in consumer:
        v = msg.value
        if v is None:
            logging.warning("Skipping invalid message: %s", msg.value)
            continue

        topic = msg.topic

        if topic == "cdr_data":
            try:
                usage_date = to_usage_date(v["event_datetime"])
                msisdn = v["msisdn"]
                data_type = v["data_type"]
                up_b = int(v["up_bytes"])
                down_b = int(v["down_bytes"])
                cost_wak = data_cost_wak(up_b + down_b)
                session.execute(upd_data, (up_b, down_b, cost_wak, msisdn, usage_date, data_type))
                logging.info("Updated data summary for %s on %s [%s]", msisdn, usage_date, data_type)
                cdr_data_updates.inc()
                cdr_last_update.set_to_current_time()
            except KeyError as e:
                logging.warning("Missing key in data CDR: %s", e)

        elif topic == "cdr_voice":
            try:
                usage_date = to_usage_date(v["start_time"])
                msisdn = v["msisdn"]
                call_type = v["call_type"]
                dur = int(v["call_duration_sec"])
                cost_wak = voice_cost_wak(dur)
                session.execute(upd_voice, (dur, cost_wak, msisdn, usage_date, call_type))
                logging.info("Updated voice summary for %s on %s [%s]", msisdn, usage_date, call_type)
                cdr_voice_updates.inc()
                cdr_last_update.set_to_current_time()
            except KeyError as e:
                logging.warning("Missing key in voice CDR: %s", e)

except KeyboardInterrupt:
    logging.info("Stopping unified consumer...")

finally:
    consumer.close()
    cluster.shutdown()
