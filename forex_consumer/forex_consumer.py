import json
import os
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Environment setup
IS_DEV = os.getenv('USER', '') != ''

# Configuration
KAFKA_SERVERS = ['localhost:19092', 'localhost:29092', 'localhost:39092'] if IS_DEV else ['redpanda-0:9092', 'redpanda-1:9092', 'redpanda-2:9092']
TOPIC = 'tick-data-dev' if IS_DEV else 'tick-data'
DB_URL = "postgresql://postgres:postgres@localhost:15432/wtc_analytics" if IS_DEV else "postgresql://postgres:postgres@postgres:5432/wtc_analytics"

# Database setup
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

# Create table
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS forex_data.forex_raw (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            pair_name VARCHAR(10) NOT NULL,
            bid_price DECIMAL(10, 4) NOT NULL,
            ask_price DECIMAL(10, 4) NOT NULL,
            spread DECIMAL(10, 4) NOT NULL,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.commit()

# Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    auto_offset_reset='earliest',
    group_id='forex-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Helper function to insert batch
def insert_batch(session, batch):
    if not batch:
        return
    for tick in batch:
        session.execute(text("""
            INSERT INTO forex_data.forex_raw (timestamp, pair_name, bid_price, ask_price, spread)
            VALUES (:timestamp, :pair_name, :bid_price, :ask_price, :spread)
        """), tick)
    session.commit()
    print(f"Inserted {len(batch)} records")

# Consume and insert
session = Session()
batch = []
BATCH_SIZE = 100

try:
    for msg in consumer:
        batch.append(msg.value)
        
        if len(batch) >= BATCH_SIZE:
            insert_batch(session, batch)
            batch = []

except KeyboardInterrupt:
    pass
finally:
    insert_batch(session, batch)
    session.close()
    consumer.close()