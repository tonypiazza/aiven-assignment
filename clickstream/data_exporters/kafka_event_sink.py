# ==============================================================================
# Kafka Data Exporter - Produce Clickstream Events
# ==============================================================================
"""
Produces clickstream events to Kafka topic.

This data exporter sends events to Kafka with configurable replay speed to
simulate real-time event streaming.

Supports both PLAINTEXT (local Docker) and SSL (Aiven mTLS) connections.
"""

import time

import polars as pl
from kafka import KafkaProducer

if "data_exporter" not in dir():
    from mage_ai.data_preparation.decorators import data_exporter, test

from clickstream.utils.config import get_settings
from clickstream.utils.kafka import build_kafka_config
from clickstream.utils.session_state import increment_producer_messages


@data_exporter
def export_events_to_kafka(df: pl.DataFrame, *args, **kwargs) -> dict:
    """
    Export clickstream events to Kafka.

    Events can be replayed with timing based on original timestamps (realtime mode),
    or sent as fast as possible (batch mode, the default).

    Configuration via environment variables:
        KAFKA_BOOTSTRAP_SERVERS: Kafka broker address
        KAFKA_SECURITY_PROTOCOL: PLAINTEXT or SSL
        KAFKA_SSL_CA_FILE: Path to CA certificate (for SSL)
        KAFKA_SSL_CERT_FILE: Path to client certificate (for SSL)
        KAFKA_SSL_KEY_FILE: Path to client key (for SSL)
        KAFKA_EVENTS_TOPIC: Topic to produce to

    Keyword Args:
        speed: Replay speed multiplier (default: 1)
        realtime_mode: If True, replay with delays based on timestamps (default: False)

    Returns:
        Dictionary with export statistics
    """
    settings = get_settings()

    speed = kwargs.get("speed", 1)
    realtime_mode = kwargs.get("realtime_mode", False)
    topic = settings.kafka.events_topic

    # Build Kafka config with SSL support
    producer_config = build_kafka_config(settings.kafka, include_serializers=True)

    # Create producer
    producer = KafkaProducer(**producer_config)

    # Ensure sorted by timestamp
    df = df.sort("timestamp")

    total_events = len(df)
    sent_count = 0
    prev_timestamp: int | None = None
    start_time = time.time()

    print(f"Producing {total_events:,} events to topic '{topic}'...")
    if realtime_mode:
        print(f"  Mode: Real-time ({speed}x)")
    else:
        print("  Mode: Batch (no delays)")

    try:
        for row in df.iter_rows(named=True):
            timestamp = row["timestamp"]
            visitor_id = row["visitor_id"]

            # Calculate delay based on timestamp difference (only in realtime mode)
            if realtime_mode and prev_timestamp is not None:
                delay_ms = (timestamp - prev_timestamp) / speed
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)

            prev_timestamp = timestamp

            # Build message
            message = {
                "timestamp": timestamp,
                "visitor_id": visitor_id,
                "event": row["event"],
                "item_id": row["item_id"],
                "transaction_id": row.get("transaction_id"),
            }

            # Send to Kafka (use visitor_id as key for partitioning)
            producer.send(topic, key=visitor_id, value=message)
            sent_count += 1

            # Progress update every 10,000 events
            if sent_count % 10000 == 0:
                elapsed = time.time() - start_time
                rate = sent_count / elapsed if elapsed > 0 else 0
                print(f"  Sent {sent_count:,} / {total_events:,} events ({rate:.0f} events/sec)")
                # Update Valkey counter
                try:
                    increment_producer_messages(10000)
                except Exception:
                    pass  # Don't fail producer if Valkey unavailable

        # Flush remaining messages
        producer.flush()

        # Update Valkey counter for remaining events
        remaining = sent_count % 10000
        if remaining > 0:
            try:
                increment_producer_messages(remaining)
            except Exception:
                pass

    finally:
        producer.close()

    elapsed = time.time() - start_time
    rate = sent_count / elapsed if elapsed > 0 else 0

    print(f"Completed: {sent_count:,} events in {elapsed:.1f}s ({rate:.0f} events/sec)")

    return {
        "events_sent": sent_count,
        "elapsed_seconds": elapsed,
        "events_per_second": rate,
        "topic": topic,
    }


@test
def test_output(output: dict, *args) -> None:
    """Validate the export results."""
    assert output is not None, "Output is undefined"
    assert "events_sent" in output, "Missing events_sent in output"
    assert output["events_sent"] >= 0, "events_sent should be non-negative"
