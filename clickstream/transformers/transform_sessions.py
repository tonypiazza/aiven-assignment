# ==============================================================================
# Streaming Transformer - Sessions
# ==============================================================================
"""
Processes events and tracks sessions in Valkey, outputs flat session records.

This transformer:
1. Extracts event data from Kafka messages
2. Updates session state in Valkey for each event
3. Outputs flat session records for sessions updated in this batch

Session records are upserted to PostgreSQL via ON CONFLICT DO UPDATE.
"""

import logging
import sys
from typing import Dict, List, Optional

if "transformer" not in dir():
    from mage_ai.data_preparation.decorators import transformer

from clickstream.utils.config import get_settings
from clickstream.utils.kafka import parse_kafka_messages
from clickstream.utils.session_state import (
    SessionState,
    check_valkey_connection,
    get_valkey_client,
)

logger = logging.getLogger(__name__)

# Cache key for session state in sys.modules to survive Mage AI module reloads
_SESSION_STATE_CACHE_KEY = "_clickstream_session_state_cache"


def get_session_state() -> Optional[SessionState]:
    """
    Get or initialize the session state manager.

    Uses sys.modules as a cache to survive Mage AI's module reloading behavior
    in streaming pipelines. This ensures we don't re-initialize the Valkey
    connection on every batch (~0.3s savings per batch).
    """
    # Check if already cached in sys.modules (survives module reloads)
    cached = getattr(sys.modules[__name__], _SESSION_STATE_CACHE_KEY, None)
    if cached is not None:
        return cached

    if not check_valkey_connection():
        logger.warning("Valkey not available - sessions will not be tracked")
        return None

    settings = get_settings()
    client = get_valkey_client()
    session_state = SessionState(
        client=client,
        timeout_minutes=settings.consumer.session_timeout_minutes,
        ttl_hours=settings.valkey.session_ttl_hours,
    )

    # Cache in sys.modules to survive reloads
    setattr(sys.modules[__name__], _SESSION_STATE_CACHE_KEY, session_state)

    logger.info(
        "Initialized session state (timeout=%d min, ttl=%d hours)",
        settings.consumer.session_timeout_minutes,
        settings.valkey.session_ttl_hours,
    )
    return session_state


@transformer
def transform(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """
    Process events, track sessions in Valkey, output flat session records.

    Uses batch Valkey operations for high performance with remote servers.

    Args:
        messages: List of event dictionaries from Kafka

    Returns:
        List of flat session dictionaries ready for PostgreSQL
    """
    session_state = get_session_state()
    if session_state is None:
        return []

    # Parse all events from Kafka messages
    events = list(parse_kafka_messages(messages))
    if not events:
        return []

    # Batch update all sessions (2 Valkey round-trips instead of thousands)
    updated_sessions = session_state.batch_update_sessions(events)

    # Convert to PostgreSQL records
    sessions = []
    for session in updated_sessions:
        db_record = session_state.to_db_record(session)
        sessions.append(
            {
                "session_id": db_record["session_id"],
                "visitor_id": db_record["visitor_id"],
                "session_start": db_record["session_start"].isoformat(),
                "session_end": db_record["session_end"].isoformat(),
                "duration_seconds": db_record["duration_seconds"],
                "event_count": db_record["event_count"],
                "view_count": db_record["view_count"],
                "cart_count": db_record["cart_count"],
                "transaction_count": db_record["transaction_count"],
                "items_viewed": db_record["items_viewed"],
                "items_carted": db_record["items_carted"],
                "items_purchased": db_record["items_purchased"],
                "converted": db_record["converted"],
            }
        )

    if sessions:
        logger.info("Transformed %d sessions", len(sessions))

    return sessions
