# ==============================================================================
# Streaming Transformer - Events Only
# ==============================================================================
"""
Transforms raw Kafka messages into flat event records for PostgreSQL.

This transformer:
1. Extracts event data from Kafka messages
2. Converts timestamps to ISO format
3. Outputs flat event records (no type wrapper)

Event deduplication is handled by PostgreSQL via a unique index
on (visitor_id, event_time, event, item_id) with ON CONFLICT DO NOTHING.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Dict, List

if "transformer" not in dir():
    from mage_ai.data_preparation.decorators import transformer

from clickstream.utils.kafka import parse_kafka_messages

logger = logging.getLogger(__name__)


@transformer
def transform(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """
    Transform raw Kafka messages into flat event records.

    Args:
        messages: List of event dictionaries from Kafka

    Returns:
        List of flat event dictionaries ready for PostgreSQL
    """
    events = []

    for event in parse_kafka_messages(messages):
        # Convert timestamp to datetime
        timestamp_ms = event["timestamp"]
        event_time = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)

        # Output flat event record (matches events table schema)
        events.append(
            {
                "event_time": event_time.isoformat(),
                "visitor_id": event["visitor_id"],
                "event": event["event"],
                "item_id": event["item_id"],
                "transaction_id": event.get("transaction_id"),
            }
        )

    if events:
        logger.info("Transformed %d events", len(events))
        # Record last message timestamp for this consumer group
        group_id = os.environ.get("CLICKSTREAM_CONSUMER_GROUP")
        if group_id:
            try:
                from clickstream.utils.session_state import set_last_message_timestamp

                set_last_message_timestamp(group_id)
            except Exception:
                pass  # Don't fail transform if Valkey is unavailable

    return events
