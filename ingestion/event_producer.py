import uuid
import random
import time
import signal
import os
import json
from datetime import datetime, UTC
from typing import List

import pandas as pd
from kafka import KafkaProducer

from variables import KAFKA_TOPIC, KAFKA_BROKER


BATCH_SIZE = 2000
FLUSH_INTERVAL_SECONDS = 10

BASE_PATH = "../data/raw/product_events"
QUARANTINE_PATH = "../data/quarantine/product_events"

EVENT_TYPES = [
    "search_performed",
    "result_clicked",
    "product_viewed",
    "recommendation_clicked",
    "rfq_sent"
]

running = True

def shutdown_handler(signum, frame):
    global running
    print("\nReceived shutdown signal. Flushing remaining events...")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# KAFKA PRODUCER
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    linger_ms=10,          # wait to batch messages
    batch_size=32768,      # 32KB internal batch
    acks="all",
    retries=3
)


# EVENT GENERATION
def generate_event() -> dict:
    """
    Generates a random product behavioral event. Mock Data
    """
    return {
        "event_id": str(uuid.uuid4()),
        # ISO8601 timestamp format -> "%Y-%m-%dT%H:%M"
        "event_timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M"),
        "event_type": random.choice(EVENT_TYPES),
        "user_id": random.randint(1,1000),
        "account_id": random.randint(1,10),
        "product_id": random.randint(1,100),
        "category": random.choice(["electronics", "furniture", "tools"]),
        "experiment_id": f"exp_{random.randint(1,10)}"
    }


# BASIC SCHEMA VALIDATION
def validate_events(df: pd.DataFrame):
    """
    Basic validation:
    - columns check
    - No null event_id
    - event_type valid
    """
    required_cols = [
        "event_id", "event_timestamp", "event_type",
        "user_id", "account_id", "product_id",
        "category", "experiment_id"
    ]

    valid_mask = (
        df[required_cols].notnull().all(axis=1)
        & df["event_type"].isin(EVENT_TYPES)
    )

    valid_df = df[valid_mask].copy()
    invalid_df = df[~valid_mask].copy()

    return valid_df, invalid_df



# PARQUET WRITER (DYNAMIC PARTITIONING)
def write_batch_to_parquet(df: pd.DataFrame):
    """
    Writes events partitioned by event_date and hour.
    dynamic partition logic.
    Local file is used instead of S3 bucker, for the sake of simplicity.
    """
    if df.empty:
        return

    # (1.3) Task 1 - checking event_id uniqueness
    if df["event_id"].is_unique is False:
        raise ValueError("Event ID must be unique")

    # Ensure timestamp is datetime
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])

    # partition columns
    df["event_date"] = df["event_timestamp"].dt.strftime("%Y-%m-%d")
    df["hour"] = df["event_timestamp"].dt.strftime("%H")

    # if file doesn't exist, it creates it
    os.makedirs(BASE_PATH, exist_ok=True)

    # (1.1) Partitioning by event_date and hour
    # (1.2) Task 1 - using parquet file format
    df.to_parquet(BASE_PATH, engine="pyarrow", index=False, partition_cols=["event_date", "hour"])

    print(f"[S3] Written {len(df)} events → {BASE_PATH}")


# QUARANTINE WRITER FOR INVALID RECORDS
def write_to_quarantine(df: pd.DataFrame):
    # (1.4) Task 1 - invalid records written in quarantine folder
    if df.empty:
        return

    os.makedirs(QUARANTINE_PATH, exist_ok=True)
    file_name = f"{QUARANTINE_PATH}/quarantine_{int(time.time() * 1000)}.parquet"

    df.to_parquet(file_name, engine="pyarrow", index=False)
    print(f"[QUARANTINE] Written {len(df)} invalid events → {file_name}")


# KAFKA PUBLISHER - SENDS DATA TO KAFKA INSTANCE
def publish_batch(events: list[dict]):
    # (2.2) - Task 2 - same data with raw S3 is sent to Apache Kafka
    if len(events) == 0:
        return

    # apache kafka doesn't have a method to send list of records
    for event in events:
        producer.send(KAFKA_TOPIC, event)

    producer.flush()

    print(f"[Kafka] Published {len(events)} events")


# MICRO-BATCH PROCESSING LOGIC
def process_batch(events: List[dict]):
    if not events:
        return

    df = pd.DataFrame(events)
    # (2.4) Task 2 - deduplicate on event_id within batch
    df.drop_duplicates(subset=["event_id"], inplace=True)

    # (2.2) Task 2 - Schema validation
    valid_df, invalid_df = validate_events(df)

    # (2.1) Task 2
    valid_events = valid_df.to_dict(orient="records")
    publish_batch(valid_events)

    # Task 1
    write_batch_to_parquet(valid_df)
    # Task 1
    write_to_quarantine(invalid_df)



# PIPELINE - generates data, saves it in S3/local and sends it to Kafka
def run_pipeline():
    buffer: List[dict] = []
    last_flush_time = time.time()

    print("Starting event pipeline... Press Ctrl+C to stop.")

    while running:
        event = generate_event()
        buffer.append(event)

        size_trigger = len(buffer) >= BATCH_SIZE
        time_trigger = time.time() - last_flush_time >= FLUSH_INTERVAL_SECONDS

        if size_trigger or time_trigger:
            process_batch(buffer)
            buffer.clear()
            last_flush_time = time.time()

        # appr. 2000 events per 10 seconds, 200 events per second
        # 15 million events per day
        time.sleep(0.005)  # simulate near-real-time stream

    # Final flush
    if buffer:
        process_batch(buffer)

    print("Pipeline stopped cleanly.")


if __name__ == "__main__":
    run_pipeline()
