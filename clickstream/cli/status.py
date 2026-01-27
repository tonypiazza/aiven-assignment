# ==============================================================================
# Status Command
# ==============================================================================
"""
Status command for the clickstream pipeline CLI.

Displays pipeline status and service health in either formatted box output
or JSON format for programmatic consumption.
"""

import json as json_module
import os
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import typer

from clickstream.cli.shared import (
    BOX_WIDTH,
    PRODUCER_LOG_FILE,
    PRODUCER_PID_FILE,
    B,
    C,
    I,
    _box_bottom,
    _box_header,
    _box_line,
    _empty_line,
    _get_all_consumer_pids,
    _get_consumer_log_file,
    _get_consumer_pid_file,
    _get_kafka_config,
    _get_opensearch_instance,
    _is_opensearch_consumer_running,
    get_process_end_time,
    get_process_pid,
    get_process_start_time,
    is_process_running,
    _section_header_plain,
)
from clickstream.utils.config import get_settings


# ==============================================================================
# Data Collection - Individual Functions
# ==============================================================================


def _collect_producer_data() -> dict[str, Any]:
    """Collect producer status data."""
    producer_running = is_process_running(PRODUCER_PID_FILE)
    producer_pid = get_process_pid(PRODUCER_PID_FILE) if producer_running else None
    producer_start_time = get_process_start_time(producer_pid) if producer_pid else None
    producer_last_run = get_process_end_time(PRODUCER_LOG_FILE) if not producer_running else None

    producer_messages = None
    try:
        from clickstream.utils.session_state import get_producer_messages

        messages = get_producer_messages()
        if messages > 0:
            producer_messages = messages
    except Exception:
        pass

    return {
        "running": producer_running,
        "pid": producer_pid,
        "start_time": producer_start_time,
        "last_run": producer_last_run,
        "messages_produced": producer_messages,
    }


def _collect_consumer_data(settings: Any) -> dict[str, Any]:
    """Collect consumer status data (both PostgreSQL and OpenSearch)."""
    result: dict[str, Any] = {"postgresql": {}, "opensearch": {}}

    # ── PostgreSQL Consumers ──────────────────────────────
    running_consumers = _get_all_consumer_pids()
    num_running = len(running_consumers)
    pg_consumer_start_time = None
    pg_consumer_lag = None
    pg_consumer_last_run = None
    pg_consumer_last_message = None

    if num_running > 0:
        first_pid = running_consumers[0][1]
        pg_consumer_start_time = get_process_start_time(first_pid)

        try:
            from clickstream.utils.session_state import get_kafka_consumer_lag

            pg_group = os.environ.get("POSTGRESQL_CONSUMER_GROUP_ID", "clickstream-postgresql")
            pg_consumer_lag = get_kafka_consumer_lag(pg_group)

            # Get last message timestamp when caught up
            if pg_consumer_lag == 0:
                from clickstream.utils.session_state import get_last_message_timestamp

                last_msg_time = get_last_message_timestamp(pg_group)
                if last_msg_time:
                    pg_consumer_last_message = last_msg_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    else:
        # Check for last run time
        for i in range(4):
            log_file = _get_consumer_log_file(i, "postgresql")
            if log_file.exists():
                pg_consumer_last_run = get_process_end_time(log_file)
                if pg_consumer_last_run:
                    break

    result["postgresql"] = {
        "running": num_running > 0,
        "instances": num_running,
        "pids": [pid for _, pid in running_consumers],
        "start_time": pg_consumer_start_time,
        "last_run": pg_consumer_last_run,
        "lag": pg_consumer_lag,
        "last_message": pg_consumer_last_message,
    }

    # ── OpenSearch Consumer ──────────────────────────────
    os_enabled = settings.opensearch.enabled
    os_consumer_running = _is_opensearch_consumer_running() if os_enabled else False
    os_consumer_pid = None
    os_consumer_start_time = None
    os_consumer_lag = None
    os_consumer_last_run = None
    os_consumer_last_message = None

    if os_enabled:
        instance = _get_opensearch_instance()
        os_pid_file = _get_consumer_pid_file(instance, "opensearch")
        os_log_file = _get_consumer_log_file(instance, "opensearch")

        if os_consumer_running:
            os_consumer_pid = get_process_pid(os_pid_file)
            os_consumer_start_time = get_process_start_time(os_consumer_pid)

            try:
                from clickstream.utils.session_state import get_kafka_consumer_lag

                os_consumer_lag = get_kafka_consumer_lag(settings.opensearch.consumer_group_id)

                # Get last message timestamp when caught up
                if os_consumer_lag == 0:
                    from clickstream.utils.session_state import get_last_message_timestamp

                    last_msg_time = get_last_message_timestamp(
                        settings.opensearch.consumer_group_id
                    )
                    if last_msg_time:
                        os_consumer_last_message = last_msg_time.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        else:
            os_consumer_last_run = get_process_end_time(os_log_file)

    result["opensearch"] = {
        "enabled": os_enabled,
        "running": os_consumer_running,
        "pid": os_consumer_pid,
        "start_time": os_consumer_start_time,
        "last_run": os_consumer_last_run,
        "lag": os_consumer_lag,
        "last_message": os_consumer_last_message,
    }

    return result


def _collect_kafka_data() -> dict[str, Any]:
    """Collect Kafka service status data.

    Strategy:
    1. Try Aiven API first (fast, ~1-2s)
    2. If API confirms service is RUNNING, get topic details via Kafka (shorter timeout)
    3. If API not configured or fails, fall back to direct connection (~30s)
    """
    # Try Aiven API first (fast path)
    try:
        from clickstream.utils.aiven import get_kafka_status

        api_status = get_kafka_status(timeout=5)
        if api_status:
            state = api_status.get("state", "").upper()
            if state == "RUNNING":
                # Service is running per API, get topic details via Kafka
                # Use shorter timeout since we know the service is up
                return _collect_kafka_topics_direct(timeout_ms=10000)
            else:
                # Service exists but not running (POWEROFF, REBUILDING, etc.)
                return {
                    "status": state.lower(),
                    "topics": [],
                }
    except Exception:
        pass

    # Fall back to direct Kafka connection (for local Docker or if API fails)
    return _collect_kafka_topics_direct(timeout_ms=30000)


def _collect_kafka_topics_direct(timeout_ms: int = 30000) -> dict[str, Any]:
    """Collect Kafka topic data via direct connection."""
    kafka_status = "unreachable"
    kafka_topics: list[dict[str, Any]] = []

    try:
        from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

        kafka_config = _get_kafka_config(timeout_ms=timeout_ms)

        admin = KafkaAdminClient(**kafka_config)
        topic_names = [t for t in admin.list_topics() if not t.startswith("__")]
        admin.close()

        if topic_names:
            consumer = KafkaConsumer(**kafka_config)
            for topic in topic_names:
                try:
                    partitions = consumer.partitions_for_topic(topic)
                    if partitions:
                        topic_partitions = [TopicPartition(topic, p) for p in partitions]
                        end_offsets = consumer.end_offsets(topic_partitions)
                        total_messages = sum(end_offsets.values())
                        kafka_topics.append({"name": topic, "messages": total_messages})
                except Exception:
                    kafka_topics.append({"name": topic, "messages": None})
            consumer.close()

        kafka_status = "connected"
    except Exception:
        pass

    return {
        "status": kafka_status,
        "topics": kafka_topics,
    }


def _collect_valkey_data() -> dict[str, Any]:
    """Collect Valkey service status data.

    Strategy:
    1. Try Aiven API first to check if service is running
    2. If running, get session/memory stats via direct connection
    3. If API not configured or fails, fall back to direct connection only
    """
    valkey_status = "unreachable"
    valkey_active_sessions = None
    valkey_memory = None

    # Try Aiven API first for quick status check
    try:
        from clickstream.utils.aiven import get_valkey_status

        api_status = get_valkey_status(timeout=5)
        if api_status:
            state = api_status.get("state", "").upper()
            if state != "RUNNING":
                # Service exists but not running
                return {
                    "status": state.lower(),
                    "active_sessions": None,
                    "memory": None,
                }
            # Service is running, continue to get detailed stats
    except Exception:
        pass

    # Get detailed stats via direct connection
    try:
        from clickstream.utils.session_state import check_valkey_connection, get_valkey_client

        if check_valkey_connection():
            valkey_status = "connected"
            try:
                client = get_valkey_client()
                # Count active sessions using server-side Lua script (single round trip)
                count_keys_script = """
local count = 0
local cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", KEYS[1], "COUNT", 10000)
    cursor = result[1]
    count = count + #result[2]
until cursor == "0"
return count
"""
                valkey_active_sessions = client.eval(count_keys_script, 1, "session:meta:*")
                info = client.info("memory")
                valkey_memory = info.get("used_memory_human", None)
                # Normalize memory format
                if valkey_memory and valkey_memory[-1] in ("K", "M", "G"):
                    valkey_memory = valkey_memory[:-1] + " " + valkey_memory[-1] + "B"
            except Exception:
                pass
    except Exception:
        pass

    return {
        "status": valkey_status,
        "active_sessions": valkey_active_sessions,
        "memory": valkey_memory,
    }


def _collect_postgresql_data(settings: Any, num_running: int) -> dict[str, Any]:
    """Collect PostgreSQL service status data.

    Strategy:
    1. Try Aiven API first to check if service is running
    2. If running, get row counts via direct connection
    3. If API not configured or fails, fall back to direct connection only
    """
    pg_status = "unreachable"
    pg_events = None
    pg_sessions = None
    pg_rate = None

    # Try Aiven API first for quick status check
    try:
        from clickstream.utils.aiven import get_postgres_status

        api_status = get_postgres_status(timeout=5)
        if api_status:
            state = api_status.get("state", "").upper()
            if state != "RUNNING":
                # Service exists but not running
                return {
                    "status": state.lower(),
                    "events": None,
                    "sessions": None,
                    "rate": None,
                }
            # Service is running, continue to get detailed stats
    except Exception:
        pass

    # Get detailed stats via direct connection
    try:
        import psycopg2

        with psycopg2.connect(settings.postgres.connection_string, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                schema_name = settings.postgres.schema_name
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s 
                        AND table_name = 'events'
                    )
                    """,
                    (schema_name,),
                )
                result = cur.fetchone()
                schema_exists = result[0] if result else False

                if schema_exists:
                    cur.execute(f"SELECT COUNT(*) FROM {schema_name}.events")
                    result = cur.fetchone()
                    pg_events = result[0] if result else 0

                    cur.execute(f"SELECT COUNT(*) FROM {schema_name}.sessions")
                    result = cur.fetchone()
                    pg_sessions = result[0] if result else 0
                    pg_status = "connected"
                else:
                    pg_status = "no_schema"
    except Exception:
        pass

    # Get throughput stats
    if pg_status == "connected" and pg_events is not None and pg_sessions is not None:
        try:
            from clickstream.utils.session_state import get_throughput_stats, record_stats_sample

            record_stats_sample("postgresql", pg_events, pg_sessions)
            stats = get_throughput_stats("postgresql")
            if stats and stats.get("current_rate") is not None and num_running > 0:
                pg_rate = stats["current_rate"]
        except Exception:
            pass

    return {
        "status": pg_status,
        "events": pg_events,
        "sessions": pg_sessions,
        "rate": pg_rate,
    }


def _collect_opensearch_data(settings: Any, os_consumer_running: bool) -> dict[str, Any]:
    """Collect OpenSearch service status data.

    Strategy:
    1. Try Aiven API first to check if service is running
    2. If running, get document counts via direct connection
    3. If API not configured or fails, fall back to direct connection only
    """
    opensearch_status = "disabled" if not settings.opensearch.enabled else "unreachable"
    opensearch_cluster = None
    opensearch_documents = None
    opensearch_rate = None

    if settings.opensearch.enabled:
        # Try Aiven API first for quick status check
        try:
            from clickstream.utils.aiven import get_opensearch_status

            api_status = get_opensearch_status(timeout=5)
            if api_status:
                state = api_status.get("state", "").upper()
                if state != "RUNNING":
                    # Service exists but not running
                    return {
                        "status": state.lower(),
                        "cluster": None,
                        "documents": None,
                        "rate": None,
                    }
                # Service is running, continue to get detailed stats
        except Exception:
            pass

        # Get detailed stats via direct connection
        try:
            from opensearchpy import OpenSearch

            client = OpenSearch(
                hosts=settings.opensearch.hosts,
                http_auth=(settings.opensearch.user, settings.opensearch.password),
                use_ssl=settings.opensearch.use_ssl,
                verify_certs=settings.opensearch.verify_certs,
                timeout=5,
            )
            info = client.info()
            opensearch_cluster = info.get("cluster_name", "unknown")
            opensearch_status = "connected"

            index_name = settings.opensearch.events_index
            if client.indices.exists(index=index_name):
                stats = client.count(index=index_name)
                opensearch_documents = stats.get("count", 0)
        except Exception:
            pass

        # Get throughput stats
        if opensearch_status == "connected" and opensearch_documents is not None:
            try:
                from clickstream.utils.session_state import (
                    get_throughput_stats,
                    record_stats_sample,
                )

                record_stats_sample("opensearch", opensearch_documents)
                stats = get_throughput_stats("opensearch")
                if stats and stats.get("current_rate") is not None and os_consumer_running:
                    opensearch_rate = stats["current_rate"]
            except Exception:
                pass

    return {
        "status": opensearch_status,
        "cluster": opensearch_cluster,
        "documents": opensearch_documents,
        "rate": opensearch_rate,
    }


def _collect_status_data_parallel() -> dict[str, Any]:
    """
    Collect all status data using parallel execution.

    This significantly reduces status collection time by:
    1. Running independent service checks concurrently
    2. Using Aiven API for fast service status checks when configured

    With Aiven API configured: ~2-3s total
    Without Aiven API (direct connections): ~30s total

    Returns:
        Dictionary containing all pipeline status information
    """
    settings = get_settings()

    # First, collect consumer data (fast, and needed for PostgreSQL/OpenSearch rate flags)
    consumer_data = _collect_consumer_data(settings)
    num_running = consumer_data["postgresql"]["instances"]
    os_consumer_running = consumer_data["opensearch"]["running"]

    # Run remaining checks in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            "producer": executor.submit(_collect_producer_data),
            "kafka": executor.submit(_collect_kafka_data),
            "valkey": executor.submit(_collect_valkey_data),
            "postgresql": executor.submit(_collect_postgresql_data, settings, num_running),
            "opensearch": executor.submit(_collect_opensearch_data, settings, os_consumer_running),
        }

        # Collect results with timeout (35s to allow for slow Kafka connection)
        results = {}
        for name, future in futures.items():
            try:
                results[name] = future.result(timeout=35)
            except Exception:
                # Provide default data on failure
                if name == "producer":
                    results[name] = {
                        "running": False,
                        "pid": None,
                        "start_time": None,
                        "last_run": None,
                        "messages_produced": None,
                    }
                elif name == "kafka":
                    results[name] = {"status": "unreachable", "topics": []}
                elif name == "valkey":
                    results[name] = {
                        "status": "unreachable",
                        "active_sessions": None,
                        "memory": None,
                    }
                elif name == "postgresql":
                    results[name] = {
                        "status": "unreachable",
                        "events": None,
                        "sessions": None,
                        "rate": None,
                    }
                elif name == "opensearch":
                    results[name] = {
                        "status": "unreachable",
                        "cluster": None,
                        "documents": None,
                        "rate": None,
                    }

    # Assemble the final data structure
    return {
        "producer": results["producer"],
        "consumers": consumer_data,
        "services": {
            "kafka": results["kafka"],
            "valkey": results["valkey"],
            "postgresql": results["postgresql"],
            "opensearch": results["opensearch"],
        },
    }


# ==============================================================================
# Data Collection - Legacy Sequential (kept for reference)
# ==============================================================================


def _collect_status_data() -> dict[str, Any]:
    """
    Collect all status data and return as a dictionary.

    Returns:
        Dictionary containing all pipeline status information
    """
    settings = get_settings()
    data: dict[str, Any] = {
        "producer": {},
        "consumers": {
            "postgresql": {},
            "opensearch": {},
        },
        "services": {
            "kafka": {},
            "valkey": {},
            "postgresql": {},
            "opensearch": {},
        },
    }

    # ── Producer ──────────────────────────────────────────
    producer_running = is_process_running(PRODUCER_PID_FILE)
    producer_pid = get_process_pid(PRODUCER_PID_FILE) if producer_running else None
    producer_start_time = get_process_start_time(producer_pid) if producer_pid else None
    producer_last_run = get_process_end_time(PRODUCER_LOG_FILE) if not producer_running else None

    producer_messages = None
    try:
        from clickstream.utils.session_state import get_producer_messages

        messages = get_producer_messages()
        if messages > 0:
            producer_messages = messages
    except Exception:
        pass

    data["producer"] = {
        "running": producer_running,
        "pid": producer_pid,
        "start_time": producer_start_time,
        "last_run": producer_last_run,
        "messages_produced": producer_messages,
    }

    # ── PostgreSQL Consumers ──────────────────────────────
    running_consumers = _get_all_consumer_pids()
    num_running = len(running_consumers)
    pg_consumer_start_time = None
    pg_consumer_lag = None
    pg_consumer_last_run = None
    pg_consumer_last_message = None

    if num_running > 0:
        first_pid = running_consumers[0][1]
        pg_consumer_start_time = get_process_start_time(first_pid)

        try:
            from clickstream.utils.session_state import get_kafka_consumer_lag

            pg_group = os.environ.get("POSTGRESQL_CONSUMER_GROUP_ID", "clickstream-postgresql")
            pg_consumer_lag = get_kafka_consumer_lag(pg_group)

            # Get last message timestamp when caught up
            if pg_consumer_lag == 0:
                from clickstream.utils.session_state import get_last_message_timestamp

                last_msg_time = get_last_message_timestamp(pg_group)
                if last_msg_time:
                    pg_consumer_last_message = last_msg_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    else:
        # Check for last run time
        for i in range(4):
            log_file = _get_consumer_log_file(i, "postgresql")
            if log_file.exists():
                pg_consumer_last_run = get_process_end_time(log_file)
                if pg_consumer_last_run:
                    break

    data["consumers"]["postgresql"] = {
        "running": num_running > 0,
        "instances": num_running,
        "pids": [pid for _, pid in running_consumers],
        "start_time": pg_consumer_start_time,
        "last_run": pg_consumer_last_run,
        "lag": pg_consumer_lag,
        "last_message": pg_consumer_last_message,
    }

    # ── OpenSearch Consumer ──────────────────────────────
    os_enabled = settings.opensearch.enabled
    os_consumer_running = _is_opensearch_consumer_running() if os_enabled else False
    os_consumer_pid = None
    os_consumer_start_time = None
    os_consumer_lag = None
    os_consumer_last_run = None
    os_consumer_last_message = None

    if os_enabled:
        instance = _get_opensearch_instance()
        os_pid_file = _get_consumer_pid_file(instance, "opensearch")
        os_log_file = _get_consumer_log_file(instance, "opensearch")

        if os_consumer_running:
            os_consumer_pid = get_process_pid(os_pid_file)
            os_consumer_start_time = get_process_start_time(os_consumer_pid)

            try:
                from clickstream.utils.session_state import get_kafka_consumer_lag

                os_consumer_lag = get_kafka_consumer_lag(settings.opensearch.consumer_group_id)

                # Get last message timestamp when caught up
                if os_consumer_lag == 0:
                    from clickstream.utils.session_state import get_last_message_timestamp

                    last_msg_time = get_last_message_timestamp(
                        settings.opensearch.consumer_group_id
                    )
                    if last_msg_time:
                        os_consumer_last_message = last_msg_time.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        else:
            os_consumer_last_run = get_process_end_time(os_log_file)

    data["consumers"]["opensearch"] = {
        "enabled": os_enabled,
        "running": os_consumer_running,
        "pid": os_consumer_pid,
        "start_time": os_consumer_start_time,
        "last_run": os_consumer_last_run,
        "lag": os_consumer_lag,
        "last_message": os_consumer_last_message,
    }

    # ── Kafka Service ──────────────────────────────────────
    kafka_status = "unreachable"
    kafka_topics: list[dict[str, Any]] = []

    try:
        from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

        kafka_config = _get_kafka_config(timeout_ms=30000)

        admin = KafkaAdminClient(**kafka_config)
        topic_names = [t for t in admin.list_topics() if not t.startswith("__")]
        admin.close()

        if topic_names:
            consumer = KafkaConsumer(**kafka_config)
            for topic in topic_names:
                try:
                    partitions = consumer.partitions_for_topic(topic)
                    if partitions:
                        topic_partitions = [TopicPartition(topic, p) for p in partitions]
                        end_offsets = consumer.end_offsets(topic_partitions)
                        total_messages = sum(end_offsets.values())
                        kafka_topics.append({"name": topic, "messages": total_messages})
                except Exception:
                    kafka_topics.append({"name": topic, "messages": None})
            consumer.close()

        kafka_status = "connected"
    except Exception:
        pass

    data["services"]["kafka"] = {
        "status": kafka_status,
        "topics": kafka_topics,
    }

    # ── Valkey Service ──────────────────────────────────────
    valkey_status = "unreachable"
    valkey_active_sessions = None
    valkey_memory = None

    try:
        from clickstream.utils.session_state import check_valkey_connection, get_valkey_client

        if check_valkey_connection():
            valkey_status = "connected"
            try:
                client = get_valkey_client()
                valkey_active_sessions = 0
                for _ in client.scan_iter("session:meta:*", count=1000):
                    valkey_active_sessions += 1
                info = client.info("memory")
                valkey_memory = info.get("used_memory_human", None)
                # Normalize memory format
                if valkey_memory and valkey_memory[-1] in ("K", "M", "G"):
                    valkey_memory = valkey_memory[:-1] + " " + valkey_memory[-1] + "B"
            except Exception:
                pass
    except Exception:
        pass

    data["services"]["valkey"] = {
        "status": valkey_status,
        "active_sessions": valkey_active_sessions,
        "memory": valkey_memory,
    }

    # ── PostgreSQL Service ──────────────────────────────────
    pg_status = "unreachable"
    pg_events = None
    pg_sessions = None
    pg_rate = None

    try:
        import psycopg2

        with psycopg2.connect(settings.postgres.connection_string, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                schema_name = settings.postgres.schema_name
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s 
                        AND table_name = 'events'
                    )
                    """,
                    (schema_name,),
                )
                result = cur.fetchone()
                schema_exists = result[0] if result else False

                if schema_exists:
                    cur.execute(f"SELECT COUNT(*) FROM {schema_name}.events")
                    result = cur.fetchone()
                    pg_events = result[0] if result else 0

                    cur.execute(f"SELECT COUNT(*) FROM {schema_name}.sessions")
                    result = cur.fetchone()
                    pg_sessions = result[0] if result else 0
                    pg_status = "connected"
                else:
                    pg_status = "no_schema"
    except Exception:
        pass

    # Get throughput stats
    if pg_status == "connected" and pg_events is not None and pg_sessions is not None:
        try:
            from clickstream.utils.session_state import get_throughput_stats, record_stats_sample

            record_stats_sample("postgresql", pg_events, pg_sessions)
            stats = get_throughput_stats("postgresql")
            if stats and stats.get("current_rate") is not None and num_running > 0:
                pg_rate = stats["current_rate"]
        except Exception:
            pass

    data["services"]["postgresql"] = {
        "status": pg_status,
        "events": pg_events,
        "sessions": pg_sessions,
        "rate": pg_rate,
    }

    # ── OpenSearch Service ──────────────────────────────────
    opensearch_status = "disabled" if not settings.opensearch.enabled else "unreachable"
    opensearch_cluster = None
    opensearch_documents = None
    opensearch_rate = None

    if settings.opensearch.enabled:
        try:
            from opensearchpy import OpenSearch

            client = OpenSearch(
                hosts=settings.opensearch.hosts,
                http_auth=(settings.opensearch.user, settings.opensearch.password),
                use_ssl=settings.opensearch.use_ssl,
                verify_certs=settings.opensearch.verify_certs,
                timeout=5,
            )
            info = client.info()
            opensearch_cluster = info.get("cluster_name", "unknown")
            opensearch_status = "connected"

            index_name = settings.opensearch.events_index
            if client.indices.exists(index=index_name):
                stats = client.count(index=index_name)
                opensearch_documents = stats.get("count", 0)
        except Exception:
            pass

        # Get throughput stats
        if opensearch_status == "connected" and opensearch_documents is not None:
            try:
                from clickstream.utils.session_state import (
                    get_throughput_stats,
                    record_stats_sample,
                )

                record_stats_sample("opensearch", opensearch_documents)
                stats = get_throughput_stats("opensearch")
                if stats and stats.get("current_rate") is not None and os_consumer_running:
                    opensearch_rate = stats["current_rate"]
            except Exception:
                pass

    data["services"]["opensearch"] = {
        "status": opensearch_status,
        "cluster": opensearch_cluster,
        "documents": opensearch_documents,
        "rate": opensearch_rate,
    }

    return data


# ==============================================================================
# Display Functions
# ==============================================================================


def _display_status(data: dict[str, Any]) -> None:
    """Display status in formatted box output."""
    settings = get_settings()
    W = BOX_WIDTH

    print()
    print(_box_header("CLICKSTREAM PIPELINE STATUS", W))
    print(_empty_line(W))

    # ── Producer ──────────────────────────────────────────
    print(_section_header_plain("Producer", W))
    print(_empty_line(W))

    producer = data["producer"]
    if producer["running"]:
        print(_box_line(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Running", W))
        print(_box_line(f"    PID:      {C.WHITE}{producer['pid']}{C.RESET}", W))
        if producer["start_time"]:
            print(_box_line(f"    Since:    {C.DIM}{producer['start_time']}{C.RESET}", W))
        if producer["messages_produced"]:
            msg_str = f"{producer['messages_produced']:,} produced"
            print(_box_line(f"    Messages: {C.WHITE}{msg_str}{C.RESET}", W))
    else:
        if producer["last_run"]:
            print(_box_line(f"  {C.DIM}{I.STOP}{C.RESET} Stopped", W))
            print(_box_line(f"    Last run: {C.DIM}{producer['last_run']}{C.RESET}", W))
            if producer["messages_produced"]:
                msg_str = f"{producer['messages_produced']:,} produced"
                print(_box_line(f"    Messages: {C.WHITE}{msg_str}{C.RESET}", W))
        else:
            print(_box_line(f"  {C.DIM}{I.STOP}{C.RESET} Not running", W))
    print(_empty_line(W))

    # ── Consumers ──────────────────────────────────────────
    print(_section_header_plain("Consumers", W))
    print(_empty_line(W))

    # PostgreSQL Consumer
    pg_consumer = data["consumers"]["postgresql"]
    if pg_consumer["running"]:
        num = pg_consumer["instances"]
        status_text = f"Running ({num} instance{'s' if num > 1 else ''})"
        print(_box_line(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} PostgreSQL: {status_text}", W))
        pids_str = ", ".join(str(p) for p in pg_consumer["pids"])
        print(_box_line(f"    PIDs:  {C.WHITE}{pids_str}{C.RESET}", W))
        if pg_consumer["start_time"]:
            print(_box_line(f"    Since: {C.DIM}{pg_consumer['start_time']}{C.RESET}", W))
        if pg_consumer["lag"] is not None:
            if pg_consumer["lag"] == 0:
                if pg_consumer.get("last_message"):
                    lag_str = f"0 messages (last at {pg_consumer['last_message']})"
                else:
                    lag_str = "0 messages"
            else:
                lag_str = f"{pg_consumer['lag']:,} messages"
            print(_box_line(f"    Lag:   {C.WHITE}{lag_str}{C.RESET}", W))
    else:
        print(_box_line(f"  {C.DIM}{I.STOP}{C.RESET} PostgreSQL: Not running", W))
        if pg_consumer["last_run"]:
            print(_box_line(f"    Last run: {C.DIM}{pg_consumer['last_run']}{C.RESET}", W))

    # OpenSearch Consumer
    os_consumer = data["consumers"]["opensearch"]
    if os_consumer["enabled"]:
        print(_empty_line(W))
        if os_consumer["running"]:
            print(_box_line(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} OpenSearch: Running", W))
            print(_box_line(f"    PID:   {C.WHITE}{os_consumer['pid']}{C.RESET}", W))
            if os_consumer["start_time"]:
                print(_box_line(f"    Since: {C.DIM}{os_consumer['start_time']}{C.RESET}", W))
            if os_consumer["lag"] is not None:
                if os_consumer["lag"] == 0:
                    if os_consumer.get("last_message"):
                        lag_str = f"0 messages (last at {os_consumer['last_message']})"
                    else:
                        lag_str = "0 messages"
                else:
                    lag_str = f"{os_consumer['lag']:,} messages"
                print(_box_line(f"    Lag:   {C.WHITE}{lag_str}{C.RESET}", W))
        else:
            print(_box_line(f"  {C.DIM}{I.STOP}{C.RESET} OpenSearch: Not running", W))
            if os_consumer["last_run"]:
                print(_box_line(f"    Last run: {C.DIM}{os_consumer['last_run']}{C.RESET}", W))

    print(_empty_line(W))

    # ── Services ──────────────────────────────────────────
    print(_section_header_plain("Services", W))

    # Kafka
    kafka = data["services"]["kafka"]
    print(_empty_line(W))
    if kafka["status"] == "connected":
        print(_box_line(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} {C.BOLD}Kafka{C.RESET}", W))
        if kafka["topics"]:
            for topic in kafka["topics"]:
                if topic["messages"] is not None:
                    topic_str = f"{topic['name']} ({topic['messages']:,} messages)"
                else:
                    topic_str = topic["name"]
                print(_box_line(f"    Topic:    {C.WHITE}{topic_str}{C.RESET}", W))
        else:
            print(_box_line(f"    Topics:   {C.DIM}(none){C.RESET}", W))
    else:
        print(
            _box_line(
                f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} {C.BOLD}Kafka{C.RESET} {C.DIM}(unreachable){C.RESET}",
                W,
            )
        )

    # Valkey
    valkey = data["services"]["valkey"]
    print(_empty_line(W))
    if valkey["status"] == "connected":
        print(_box_line(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} {C.BOLD}Valkey{C.RESET}", W))
        if valkey["active_sessions"] is not None:
            sessions_str = f"{valkey['active_sessions']:,}"
            print(_box_line(f"    Sessions: {C.WHITE}{sessions_str}{C.RESET} active", W))
        if valkey["memory"]:
            print(_box_line(f"    Memory:   {C.WHITE}{valkey['memory']}{C.RESET}", W))
    else:
        print(
            _box_line(
                f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} {C.BOLD}Valkey{C.RESET} {C.DIM}(unreachable){C.RESET}",
                W,
            )
        )

    # PostgreSQL
    pg = data["services"]["postgresql"]
    print(_empty_line(W))
    if pg["status"] == "connected":
        print(_box_line(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} {C.BOLD}PostgreSQL{C.RESET}", W))
        events_str = f"{pg['events']:,}"
        print(_box_line(f"    Events:   {C.WHITE}{events_str}{C.RESET}", W))
        sessions_str = f"{pg['sessions']:,}"
        print(_box_line(f"    Sessions: {C.WHITE}{sessions_str}{C.RESET}", W))
        if pg["rate"] is not None:
            rate_str = f"{pg['rate']:,.0f} inserts/sec"
            print(_box_line(f"    Rate:     {C.WHITE}{rate_str}{C.RESET}", W))
    elif pg["status"] == "no_schema":
        print(
            _box_line(
                f"  {C.BRIGHT_YELLOW}{I.WARN}{C.RESET} {C.BOLD}PostgreSQL{C.RESET} {C.DIM}(no schema){C.RESET}",
                W,
            )
        )
        print(_box_line(f"    {C.DIM}Run: clickstream db init{C.RESET}", W))
    else:
        print(
            _box_line(
                f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} {C.BOLD}PostgreSQL{C.RESET} {C.DIM}(unreachable){C.RESET}",
                W,
            )
        )

    # OpenSearch
    opensearch = data["services"]["opensearch"]
    print(_empty_line(W))
    if opensearch["status"] == "disabled":
        print(
            _box_line(
                f"  {C.BRIGHT_YELLOW}{I.CIRCLE}{C.RESET} {C.BOLD}OpenSearch{C.RESET} {C.DIM}(disabled){C.RESET}",
                W,
            )
        )
    elif opensearch["status"] == "connected":
        print(_box_line(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} {C.BOLD}OpenSearch{C.RESET}", W))
        index_name = settings.opensearch.events_index
        if opensearch["documents"] is not None:
            doc_str = f"{opensearch['documents']:,}"
            index_line = f"    Index:    {C.WHITE}{index_name}{C.RESET} ({doc_str} documents)"
            print(_box_line(index_line, W))
            if opensearch["rate"] is not None:
                rate_str = f"{opensearch['rate']:,.0f} documents/sec"
                print(_box_line(f"    Rate:     {C.WHITE}{rate_str}{C.RESET}", W))
        else:
            print(_box_line(f"    Index:    {C.WHITE}{index_name}{C.RESET} (not created)", W))
    else:
        print(
            _box_line(
                f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} {C.BOLD}OpenSearch{C.RESET} {C.DIM}(unreachable){C.RESET}",
                W,
            )
        )

    print(_empty_line(W))
    print(_box_bottom(W))
    print()


# ==============================================================================
# Command
# ==============================================================================


def show_status(
    json_output: bool = typer.Option(False, "--json", help="Output status as JSON"),
) -> None:
    """Show pipeline status and service health."""
    warnings.filterwarnings("ignore")

    # Use parallel collection for faster status checks (~30s vs ~73s)
    data = _collect_status_data_parallel()

    if json_output:
        print(json_module.dumps(data, indent=2))
    else:
        _display_status(data)
