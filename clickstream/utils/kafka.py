# ==============================================================================
# Kafka Utilities
# ==============================================================================
"""
Shared utilities for Kafka client configuration and message processing.

Supports both PLAINTEXT (local Docker) and SSL (Aiven mTLS) security protocols.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Dict, Generator, List

if TYPE_CHECKING:
    from clickstream.utils.config import KafkaSettings


# ==============================================================================
# Configuration
# ==============================================================================


def get_project_root() -> Path:
    """
    Get the project root directory.

    Searches for pyproject.toml to identify the project root.

    Returns:
        Path to the project root directory
    """
    candidates = [
        Path.cwd(),
        Path(__file__).parent.parent.parent,
    ]
    for path in candidates:
        if (path / "pyproject.toml").exists():
            return path
    return Path.cwd()


def build_kafka_config(
    settings: "KafkaSettings",
    include_serializers: bool = False,
    request_timeout_ms: int | None = None,
    include_retry_config: bool = True,
    include_producer_retries: bool = False,
) -> dict:
    """
    Build Kafka client configuration from settings.

    Supports PLAINTEXT (local) and SSL (Aiven mTLS) security protocols.
    Includes retry and reconnection parameters for network resilience.

    Args:
        settings: KafkaSettings instance
        include_serializers: If True, add JSON serializers (for producer)
        request_timeout_ms: Optional request timeout in milliseconds
        include_retry_config: If True, add connection retry/reconnect parameters (default: True)
        include_producer_retries: If True, add producer-specific retry settings (default: False)
            Note: 'retries' and 'retry_backoff_ms' are only valid for KafkaProducer,
            not for KafkaConsumer or KafkaAdminClient.

    Returns:
        Dict with Kafka client configuration
    """
    config: dict = {
        "bootstrap_servers": settings.bootstrap_servers,
        "security_protocol": settings.security_protocol,
    }

    # Add connection retry/reconnect configuration for network resilience
    # These parameters are valid for all Kafka clients (producer, consumer, admin)
    if include_retry_config:
        config.update(
            {
                # Connection retry settings (applies to all clients)
                "reconnect_backoff_ms": 1000,
                "reconnect_backoff_max_ms": 32000,
                # Request timeout
                "request_timeout_ms": request_timeout_ms or 30000,
                # Keep connections alive longer to avoid unnecessary reconnects
                "connections_max_idle_ms": 540000,  # 9 minutes
            }
        )
    elif request_timeout_ms is not None:
        config["request_timeout_ms"] = request_timeout_ms

    # Add producer-specific retry settings
    # Note: These are ONLY valid for KafkaProducer, not Consumer or AdminClient
    if include_producer_retries:
        config.update(
            {
                "retries": 10,
                "retry_backoff_ms": 1000,
            }
        )

    # Add SSL config if using SSL protocol
    if settings.security_protocol == "SSL":
        project_root = get_project_root()

        if settings.ssl_ca_file:
            ca_path = project_root / settings.ssl_ca_file
            if ca_path.exists():
                config["ssl_cafile"] = str(ca_path)

        if settings.ssl_cert_file:
            cert_path = project_root / settings.ssl_cert_file
            if cert_path.exists():
                config["ssl_certfile"] = str(cert_path)

        if settings.ssl_key_file:
            key_path = project_root / settings.ssl_key_file
            if key_path.exists():
                config["ssl_keyfile"] = str(key_path)

    # Add serializers for producer
    if include_serializers:
        import json

        config["value_serializer"] = lambda v: json.dumps(v).encode("utf-8")
        config["key_serializer"] = lambda k: str(k).encode("utf-8") if k is not None else None

    return config


# ==============================================================================
# Message Processing
# ==============================================================================

# Required fields for a valid clickstream event
REQUIRED_EVENT_FIELDS = ["timestamp", "visitor_id", "event", "item_id"]


def parse_kafka_messages(
    messages: List[Dict],
) -> Generator[Dict, None, None]:
    """
    Parse Kafka messages and yield valid event dictionaries.

    Handles multiple message formats:
    - Direct event dict (from YAML-based Kafka sources)
    - Nested in 'data' key (some Kafka message wrappers)
    - Nested in 'value' key (Python @streaming_source classes)

    Skips messages missing required fields: timestamp, visitor_id, event, item_id.

    Args:
        messages: List of raw messages from Kafka

    Yields:
        Valid event dictionaries with required fields
    """
    for msg in messages:
        # Handle various message formats:
        # 1. Direct event dict (from YAML source)
        # 2. Nested in 'data' key (some Kafka wrappers)
        # 3. Nested in 'value' key (Python streaming source)
        if "value" in msg and isinstance(msg["value"], dict):
            event = msg["value"]
        elif "data" in msg and isinstance(msg["data"], dict):
            event = msg["data"]
        else:
            event = msg

        # Skip if missing required fields
        if not all(k in event for k in REQUIRED_EVENT_FIELDS):
            continue

        yield event
