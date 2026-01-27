# ==============================================================================
# Kafka Streaming Source - OpenSearch Consumer Pipeline
# ==============================================================================
"""
Kafka streaming source for the OpenSearch consumer pipeline.

Uses @streaming_source decorator for Mage AI streaming pipeline integration.
Supports both PLAINTEXT (local Docker) and SSL (Aiven mTLS) connections.

Configuration via environment variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker address(es)
    KAFKA_SECURITY_PROTOCOL: PLAINTEXT or SSL
    KAFKA_SSL_CA_FILE: Path to CA certificate (for SSL)
    KAFKA_SSL_CERT_FILE: Path to client certificate (for SSL)
    KAFKA_SSL_KEY_FILE: Path to client key (for SSL)
    KAFKA_EVENTS_TOPIC: Topic to consume from
    OPENSEARCH_CONSUMER_GROUP_ID: Consumer group ID
"""

import json
import os
from typing import Callable

from kafka import KafkaConsumer
from mage_ai.streaming.sources.base_python import BasePythonSource

if "streaming_source" not in dir():
    from mage_ai.data_preparation.decorators import streaming_source

from clickstream.utils.config import get_settings
from clickstream.utils.kafka import build_kafka_config


@streaming_source
class KafkaOpenSearchSource(BasePythonSource):
    """Kafka source for clickstream events (OpenSearch pipeline)."""

    def init_client(self):
        """Initialize Kafka consumer with SSL support."""
        settings = get_settings()

        # Build base config with SSL support
        config = build_kafka_config(settings.kafka)

        # Get consumer group from environment
        consumer_group = os.environ.get("OPENSEARCH_CONSUMER_GROUP_ID", "clickstream-opensearch")

        # Add consumer-specific settings
        config.update(
            {
                "group_id": consumer_group,
                "auto_offset_reset": settings.consumer.auto_offset_reset,
                "enable_auto_commit": False,
                "max_partition_fetch_bytes": 1048576,
            }
        )

        topic = settings.kafka.events_topic
        self.consumer = KafkaConsumer(topic, **config)
        self._print(f"Initialized Kafka consumer for topic: {topic}")
        self._print(f"Consumer group: {consumer_group}")
        self._print(f"Security protocol: {settings.kafka.security_protocol}")

    def batch_read(self, handler: Callable):
        """Read batches of messages and pass to handler."""
        settings = get_settings()
        batch_size = settings.consumer.batch_size
        timeout_ms = settings.consumer.poll_timeout_ms

        self._print("Starting batch read loop...")
        self._print(f"Batch size: {batch_size}, poll timeout: {timeout_ms}ms")

        while True:
            # Poll for messages
            messages = self.consumer.poll(timeout_ms=timeout_ms, max_records=batch_size)

            records = []
            for topic_partition, msgs in messages.items():
                for msg in msgs:
                    # Parse message value as JSON
                    try:
                        value = json.loads(msg.value.decode("utf-8")) if msg.value else None
                    except json.JSONDecodeError:
                        value = msg.value.decode("utf-8") if msg.value else None

                    records.append(
                        {
                            "key": msg.key.decode("utf-8") if msg.key else None,
                            "value": value,
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                        }
                    )

            if records:
                handler(records)
                self.consumer.commit()

    def destroy(self):
        """Clean up consumer connection."""
        if hasattr(self, "consumer") and self.consumer:
            self._print("Closing Kafka consumer...")
            self.consumer.close()
