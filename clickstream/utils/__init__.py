# ==============================================================================
# Clickstream Pipeline Utilities
# ==============================================================================
"""
Shared utilities for the clickstream pipeline.

This module exports configuration and models for use throughout the pipeline.
"""

from clickstream.utils.config import (
    ConsumerSettings,
    KafkaSettings,
    OpenSearchSettings,
    PostgresSettings,
    ProducerSettings,
    Settings,
    get_settings,
)
from clickstream.utils.db import (
    ensure_schema,
    reset_schema,
)
from clickstream.utils.kafka import (
    REQUIRED_EVENT_FIELDS,
    parse_kafka_messages,
)
from clickstream.utils.models import (
    ClickstreamEvent,
    EventType,
    Session,
)

__all__ = [
    # Config
    "ConsumerSettings",
    "KafkaSettings",
    "OpenSearchSettings",
    "PostgresSettings",
    "ProducerSettings",
    "Settings",
    "get_settings",
    # Database
    "ensure_schema",
    "reset_schema",
    # Kafka utilities
    "REQUIRED_EVENT_FIELDS",
    "parse_kafka_messages",
    # Models
    "ClickstreamEvent",
    "EventType",
    "Session",
]
