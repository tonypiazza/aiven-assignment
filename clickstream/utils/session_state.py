# ==============================================================================
# Valkey (Redis) Utilities for Session State
# ==============================================================================
"""
Valkey/Redis utilities for managing session state in streaming pipelines.

This module provides:
- Session state storage with automatic TTL
- Atomic session updates using Redis transactions

Note: Event deduplication is handled by PostgreSQL via a unique index
on (visitor_id, event_time, event, item_id) with ON CONFLICT DO NOTHING.

Session State Schema (per visitor):
- session:{visitor_id}:meta -> Hash with session metadata
"""

import json
from datetime import datetime, timezone
from typing import Dict, List, Optional

import redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

from clickstream.utils.config import get_settings


def get_valkey_client() -> redis.Redis:
    """
    Get a Valkey/Redis client connection.

    Configured with:
    - 10 second socket timeouts for fast failure detection
    - 3 automatic retries with exponential backoff for transient failures
    - Health check interval to keep connections alive

    Returns:
        redis.Redis client instance
    """
    settings = get_settings()

    # Configure retry with exponential backoff for transient failures
    retry = Retry(ExponentialBackoff(), retries=3)

    return redis.from_url(
        settings.valkey.url,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=10,
        retry=retry,
        retry_on_error=[RedisTimeoutError, RedisConnectionError],
        health_check_interval=30,
    )


def check_valkey_connection() -> bool:
    """Check if Valkey is reachable.

    Uses a shorter timeout (5 seconds) than the main client since
    this is just a quick health check ping.
    """
    try:
        settings = get_settings()
        # Use shorter timeout for health check - just need a quick ping
        client = redis.from_url(
            settings.valkey.url,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
        client.ping()
        return True
    except Exception:
        return False


class SessionState:
    """
    Manages session state for a visitor in Valkey.

    Session state includes:
    - session_id: Unique session identifier (visitor_id + session_num)
    - session_start: First event timestamp in session
    - session_end: Latest event timestamp in session
    - event_count: Total events in session
    - view_count, cart_count, transaction_count: Event type counts
    - items_viewed, items_carted, items_purchased: Item lists
    - last_activity: Timestamp of last event (for session timeout)
    """

    # Key prefixes
    META_PREFIX = "session:meta:"

    def __init__(self, client: redis.Redis, timeout_minutes: int = 30, ttl_hours: int = 24):
        """
        Initialize session state manager.

        Args:
            client: Redis client
            timeout_minutes: Session inactivity timeout
            ttl_hours: TTL for session keys in Redis
        """
        self.client = client
        self.timeout_ms = timeout_minutes * 60 * 1000
        self.ttl_seconds = ttl_hours * 3600

    def _meta_key(self, visitor_id: int) -> str:
        return f"{self.META_PREFIX}{visitor_id}"

    def get_session(self, visitor_id: int) -> Optional[dict]:
        """
        Get current session state for a visitor.

        Returns:
            Session dict or None if no active session
        """
        meta_key = self._meta_key(visitor_id)
        data = self.client.hgetall(meta_key)

        if not data:
            return None

        # Parse the stored data
        return {
            "session_id": data.get("session_id"),
            "visitor_id": int(data.get("visitor_id", visitor_id)),
            "session_num": int(data.get("session_num", 1)),
            "session_start": int(data.get("session_start", 0)),
            "session_end": int(data.get("session_end", 0)),
            "event_count": int(data.get("event_count", 0)),
            "view_count": int(data.get("view_count", 0)),
            "cart_count": int(data.get("cart_count", 0)),
            "transaction_count": int(data.get("transaction_count", 0)),
            "items_viewed": json.loads(data.get("items_viewed", "[]")),
            "items_carted": json.loads(data.get("items_carted", "[]")),
            "items_purchased": json.loads(data.get("items_purchased", "[]")),
            "last_activity": int(data.get("last_activity", 0)),
        }

    def _is_session_expired(self, session: dict, event_timestamp: int) -> bool:
        """Check if session has expired based on inactivity timeout."""
        if not session or session.get("last_activity", 0) == 0:
            return True
        gap = event_timestamp - session["last_activity"]
        return gap > self.timeout_ms

    def update_session(self, event: dict) -> tuple[dict, bool]:
        """
        Update session state with a new event.

        Args:
            event: Event dictionary with timestamp, visitor_id, event, item_id, transaction_id

        Returns:
            Tuple of (session_dict, is_new_session)
        """
        visitor_id = event["visitor_id"]
        timestamp = event["timestamp"]
        event_type = event["event"]
        item_id = event["item_id"]

        meta_key = self._meta_key(visitor_id)

        # Get current session
        current = self.get_session(visitor_id)
        is_new_session = False

        # Check if we need a new session
        if current is None or self._is_session_expired(current, timestamp):
            is_new_session = True
            session_num = (current["session_num"] + 1) if current else 1
            session_id = f"{visitor_id}_{session_num}"

            current = {
                "session_id": session_id,
                "visitor_id": visitor_id,
                "session_num": session_num,
                "session_start": timestamp,
                "session_end": timestamp,
                "event_count": 0,
                "view_count": 0,
                "cart_count": 0,
                "transaction_count": 0,
                "items_viewed": [],
                "items_carted": [],
                "items_purchased": [],
                "last_activity": timestamp,
            }

        # Update session with new event
        current["event_count"] += 1
        current["session_end"] = max(current["session_end"], timestamp)
        current["last_activity"] = timestamp

        # Update event type counts
        if event_type == "view":
            current["view_count"] += 1
            if item_id not in current["items_viewed"]:
                current["items_viewed"].append(item_id)
        elif event_type == "addtocart":
            current["cart_count"] += 1
            if item_id not in current["items_carted"]:
                current["items_carted"].append(item_id)
        elif event_type == "transaction":
            current["transaction_count"] += 1
            if item_id not in current["items_purchased"]:
                current["items_purchased"].append(item_id)

        # Save to Redis
        self.client.hset(
            meta_key,
            mapping={
                "session_id": current["session_id"],
                "visitor_id": str(current["visitor_id"]),
                "session_num": str(current["session_num"]),
                "session_start": str(current["session_start"]),
                "session_end": str(current["session_end"]),
                "event_count": str(current["event_count"]),
                "view_count": str(current["view_count"]),
                "cart_count": str(current["cart_count"]),
                "transaction_count": str(current["transaction_count"]),
                "items_viewed": json.dumps(current["items_viewed"]),
                "items_carted": json.dumps(current["items_carted"]),
                "items_purchased": json.dumps(current["items_purchased"]),
                "last_activity": str(current["last_activity"]),
            },
        )
        self.client.expire(meta_key, self.ttl_seconds)

        return current, is_new_session

    def batch_update_sessions(self, events: List[Dict]) -> List[Dict]:
        """
        Batch update sessions for multiple events using Redis pipelining.

        This is much faster than calling update_session() for each event
        when working with remote Redis/Valkey servers (e.g., Aiven).

        Performance: 2 Valkey round-trips instead of ~3500 for 1000 events.

        Args:
            events: List of event dicts with timestamp, visitor_id, event, item_id, transaction_id

        Returns:
            List of session dicts ready for to_db_record()
        """
        if not events:
            return []

        # 1. Get unique visitor_ids
        visitor_ids = list(set(e["visitor_id"] for e in events))

        # 2. Batch fetch all existing sessions (one pipeline call)
        pipe = self.client.pipeline()
        for vid in visitor_ids:
            pipe.hgetall(self._meta_key(vid))
        results = pipe.execute()

        # 3. Parse fetched sessions into a local dict
        sessions: Dict[int, Dict] = {}
        for vid, data in zip(visitor_ids, results):
            if data:
                sessions[vid] = {
                    "session_id": data.get("session_id"),
                    "visitor_id": int(data.get("visitor_id", vid)),
                    "session_num": int(data.get("session_num", 1)),
                    "session_start": int(data.get("session_start", 0)),
                    "session_end": int(data.get("session_end", 0)),
                    "event_count": int(data.get("event_count", 0)),
                    "view_count": int(data.get("view_count", 0)),
                    "cart_count": int(data.get("cart_count", 0)),
                    "transaction_count": int(data.get("transaction_count", 0)),
                    "items_viewed": json.loads(data.get("items_viewed", "[]")),
                    "items_carted": json.loads(data.get("items_carted", "[]")),
                    "items_purchased": json.loads(data.get("items_purchased", "[]")),
                    "last_activity": int(data.get("last_activity", 0)),
                }

        # 4. Sort events by timestamp to process in chronological order
        sorted_events = sorted(events, key=lambda e: e["timestamp"])

        # 5. Process all events locally (update sessions in memory)
        updated_visitor_ids = set()
        for event in sorted_events:
            visitor_id = event["visitor_id"]
            timestamp = event["timestamp"]
            event_type = event["event"]
            item_id = event["item_id"]

            current = sessions.get(visitor_id)

            # Check if we need a new session
            if current is None or self._is_session_expired(current, timestamp):
                session_num = (current["session_num"] + 1) if current else 1
                session_id = f"{visitor_id}_{session_num}"
                current = {
                    "session_id": session_id,
                    "visitor_id": visitor_id,
                    "session_num": session_num,
                    "session_start": timestamp,
                    "session_end": timestamp,
                    "event_count": 0,
                    "view_count": 0,
                    "cart_count": 0,
                    "transaction_count": 0,
                    "items_viewed": [],
                    "items_carted": [],
                    "items_purchased": [],
                    "last_activity": timestamp,
                }
                sessions[visitor_id] = current

            # Update session with event
            current["event_count"] += 1
            current["session_end"] = max(current["session_end"], timestamp)
            current["last_activity"] = timestamp

            if event_type == "view":
                current["view_count"] += 1
                if item_id not in current["items_viewed"]:
                    current["items_viewed"].append(item_id)
            elif event_type == "addtocart":
                current["cart_count"] += 1
                if item_id not in current["items_carted"]:
                    current["items_carted"].append(item_id)
            elif event_type == "transaction":
                current["transaction_count"] += 1
                if item_id not in current["items_purchased"]:
                    current["items_purchased"].append(item_id)

            updated_visitor_ids.add(visitor_id)

        # 6. Batch write all updated sessions back to Valkey (one pipeline call)
        pipe = self.client.pipeline()
        for vid in updated_visitor_ids:
            session = sessions[vid]
            meta_key = self._meta_key(vid)
            pipe.hset(
                meta_key,
                mapping={
                    "session_id": session["session_id"],
                    "visitor_id": str(session["visitor_id"]),
                    "session_num": str(session["session_num"]),
                    "session_start": str(session["session_start"]),
                    "session_end": str(session["session_end"]),
                    "event_count": str(session["event_count"]),
                    "view_count": str(session["view_count"]),
                    "cart_count": str(session["cart_count"]),
                    "transaction_count": str(session["transaction_count"]),
                    "items_viewed": json.dumps(session["items_viewed"]),
                    "items_carted": json.dumps(session["items_carted"]),
                    "items_purchased": json.dumps(session["items_purchased"]),
                    "last_activity": str(session["last_activity"]),
                },
            )
            pipe.expire(meta_key, self.ttl_seconds)
        pipe.execute()

        # 7. Return updated sessions (already in memory, no fetch needed)
        return [sessions[vid] for vid in updated_visitor_ids]

    def to_db_record(self, session: dict) -> dict:
        """
        Convert session state to PostgreSQL record format.

        Args:
            session: Session dict from get_session() or update_session()

        Returns:
            Dict ready for PostgreSQL insert/upsert
        """
        # Convert timestamps to datetime
        session_start = datetime.fromtimestamp(session["session_start"] / 1000.0, tz=timezone.utc)
        session_end = datetime.fromtimestamp(session["session_end"] / 1000.0, tz=timezone.utc)
        duration_seconds = int((session_end - session_start).total_seconds())

        return {
            "session_id": session["session_id"],
            "visitor_id": session["visitor_id"],
            "session_start": session_start,
            "session_end": session_end,
            "duration_seconds": duration_seconds,
            "event_count": session["event_count"],
            "view_count": session["view_count"],
            "cart_count": session["cart_count"],
            "transaction_count": session["transaction_count"],
            "items_viewed": session["items_viewed"],
            "items_carted": session["items_carted"],
            "items_purchased": session["items_purchased"],
            "converted": session["transaction_count"] > 0,
        }


def clear_session_state() -> int:
    """
    Clear all session state from Valkey.

    Returns:
        Number of keys deleted
    """
    client = get_valkey_client()
    keys = list(client.scan_iter(f"{SessionState.META_PREFIX}*"))

    if keys:
        return client.delete(*keys)
    return 0


# ==============================================================================
# Producer Messages Counter
# ==============================================================================

PRODUCER_MESSAGES_KEY = "producer:messages_produced"


def increment_producer_messages(count: int = 1) -> int:
    """
    Increment the producer messages counter.

    Args:
        count: Number to increment by (default: 1)

    Returns:
        New total count after increment
    """
    client = get_valkey_client()
    return client.incrby(PRODUCER_MESSAGES_KEY, count)


def get_producer_messages() -> int:
    """
    Get the current producer messages count.

    Returns:
        Current count, or 0 if not set
    """
    client = get_valkey_client()
    value = client.get(PRODUCER_MESSAGES_KEY)
    return int(value) if value else 0


# ==============================================================================
# Throughput Stats Tracking
# ==============================================================================

STATS_SAMPLES_KEY_PREFIX = "clickstream:stats:samples:"
STATS_TTL_SECONDS = 3600  # 1 hour


def record_stats_sample(source: str, count: int, count2: Optional[int] = None) -> None:
    """
    Record a stats sample in Valkey.

    Args:
        source: Source identifier (e.g., "postgresql", "opensearch")
        count: Primary count (e.g., events for PostgreSQL, documents for OpenSearch)
        count2: Optional secondary count (e.g., sessions for PostgreSQL)
    """
    import time

    client = get_valkey_client()
    timestamp = int(time.time())
    key = f"{STATS_SAMPLES_KEY_PREFIX}{source}"

    # Store as JSON with timestamp as score
    sample_data = {"count": count, "ts": timestamp}
    if count2 is not None:
        sample_data["count2"] = count2
    sample = json.dumps(sample_data)

    # Add to sorted set
    client.zadd(key, {sample: timestamp})

    # Set TTL on the key
    client.expire(key, STATS_TTL_SECONDS)

    # Clean up old samples (older than 1 hour)
    cutoff = timestamp - STATS_TTL_SECONDS
    client.zremrangebyscore(key, "-inf", cutoff)


def get_throughput_stats(source: str) -> dict:
    """
    Calculate throughput stats from stored samples.

    Args:
        source: Source identifier (e.g., "postgresql", "opensearch")

    Returns:
        Dict with:
        - current_rate: count/sec over last 60 seconds
        - samples_count: number of samples in window
    """
    import time

    client = get_valkey_client()
    key = f"{STATS_SAMPLES_KEY_PREFIX}{source}"

    # Get all samples
    samples_raw = client.zrange(key, 0, -1, withscores=True)

    if not samples_raw or len(samples_raw) < 2:
        return {
            "current_rate": None,
            "samples_count": len(samples_raw) if samples_raw else 0,
        }

    # Parse samples and sort by timestamp
    samples = []
    for sample_json, score in samples_raw:
        try:
            sample = json.loads(sample_json)
            samples.append(sample)
        except json.JSONDecodeError:
            continue

    samples.sort(key=lambda x: x["ts"])

    if len(samples) < 2:
        return {
            "current_rate": None,
            "samples_count": len(samples),
        }

    # Calculate current rate (last 60 seconds)
    now = int(time.time())
    recent_samples = [s for s in samples if now - s["ts"] <= 60]

    current_rate = None
    if len(recent_samples) >= 2:
        oldest = recent_samples[0]
        newest = recent_samples[-1]
        time_diff = newest["ts"] - oldest["ts"]
        if time_diff > 0:
            count_diff = newest["count"] - oldest["count"]
            current_rate = count_diff / time_diff

    return {
        "current_rate": current_rate,
        "samples_count": len(samples),
    }


# ==============================================================================
# Last Message Timestamp Tracking
# ==============================================================================

LAST_MESSAGE_TS_PREFIX = "consumer:last_message_ts:"
LAST_MESSAGE_TS_TTL_SECONDS = 86400  # 24 hours


def set_last_message_timestamp(group_id: str) -> None:
    """
    Store the current time as the last message processed timestamp.

    Args:
        group_id: Consumer group ID to store timestamp for
    """
    client = get_valkey_client()
    key = f"{LAST_MESSAGE_TS_PREFIX}{group_id}"
    client.set(key, datetime.now(timezone.utc).isoformat())
    client.expire(key, LAST_MESSAGE_TS_TTL_SECONDS)


def get_last_message_timestamp(group_id: Optional[str] = None) -> Optional[datetime]:
    """
    Get the timestamp of the last processed message for a consumer group.

    Args:
        group_id: Consumer group ID to check. If None, uses the default consumer group.

    Returns:
        datetime of last message processed (in local time), or None if unavailable
    """
    try:
        settings = get_settings()
        effective_group_id = group_id if group_id else settings.consumer.group_id
        client = get_valkey_client()
        key = f"{LAST_MESSAGE_TS_PREFIX}{effective_group_id}"
        value = client.get(key)
        if value:
            # Parse UTC timestamp and convert to local time
            utc_time = datetime.fromisoformat(value)
            return utc_time.replace(tzinfo=timezone.utc).astimezone()
        return None
    except Exception:
        return None


def get_kafka_consumer_lag(group_id: Optional[str] = None) -> Optional[int]:
    """
    Get the Kafka consumer lag (number of messages behind) for a consumer group.

    Args:
        group_id: Consumer group ID to check. If None, uses the default session consumer group.

    Returns:
        Number of messages the consumer is behind, or None if unavailable
    """
    try:
        from kafka import KafkaAdminClient, KafkaConsumer

        from clickstream.utils.config import get_settings
        from clickstream.utils.kafka import build_kafka_config

        settings = get_settings()
        kafka_config = build_kafka_config(settings.kafka)

        # Get end offsets (latest messages in topic)
        consumer = KafkaConsumer(**kafka_config)
        topic = settings.kafka.events_topic
        partitions = consumer.partitions_for_topic(topic)

        if not partitions:
            consumer.close()
            return None

        from kafka import TopicPartition

        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        end_offsets = consumer.end_offsets(topic_partitions)
        consumer.close()

        # Get committed offsets for consumer group
        admin = KafkaAdminClient(**kafka_config)
        effective_group_id = group_id if group_id else settings.consumer.group_id

        try:
            offsets = admin.list_consumer_group_offsets(effective_group_id)
        except Exception:
            admin.close()
            return None

        admin.close()

        # Calculate total lag
        total_lag = 0
        for tp, end_offset in end_offsets.items():
            committed = offsets.get(tp)
            if committed and committed.offset >= 0:
                lag = end_offset - committed.offset
                total_lag += max(0, lag)
            else:
                # No committed offset, assume lag is full partition
                total_lag += end_offset

        return total_lag

    except Exception:
        return None
