# ==============================================================================
# CLI Commands Module
# ==============================================================================
"""
CLI commands for the clickstream pipeline.

Commands are organized into separate modules for maintainability:
- shared.py: Common utilities, constants, and helpers
- status.py: Status command showing pipeline health
"""

from clickstream.cli.shared import (
    # Constants
    BOX_WIDTH,
    PRODUCER_LOG_FILE,
    PRODUCER_PID_FILE,
    # Classes
    Box,
    Colors,
    Icons,
    # Aliases
    B,
    C,
    I,
    # Box drawing helpers
    _box_bottom,
    _box_header,
    _box_line,
    _empty_line,
    _section_header,
    _section_header_plain,
    _status_badge,
    _visible_len,
    # Path helpers
    _get_consumer_log_file,
    _get_consumer_pid_file,
    _get_opensearch_instance,
    get_mage_project_path,
    get_project_root,
    # Process management
    get_process_end_time,
    get_process_pid,
    get_process_start_time,
    is_process_running,
    start_background_process,
    stop_process,
    # Multi-consumer management
    _count_running_consumers,
    _get_all_consumer_pids,
    _get_topic_partition_count,
    _start_consumer_instance,
    _stop_all_consumers,
    # OpenSearch consumer management
    _is_opensearch_consumer_running,
    _start_opensearch_consumer,
    _stop_opensearch_consumer,
    # Kafka helpers
    _get_kafka_admin_client,
    _get_kafka_config,
    _purge_kafka_topic,
    _reset_consumer_group,
    # Database helpers
    check_db_connection,
)

__all__ = [
    # Constants
    "BOX_WIDTH",
    "PRODUCER_LOG_FILE",
    "PRODUCER_PID_FILE",
    # Classes
    "Box",
    "Colors",
    "Icons",
    # Aliases
    "B",
    "C",
    "I",
    # Box drawing helpers
    "_box_bottom",
    "_box_header",
    "_box_line",
    "_empty_line",
    "_section_header",
    "_section_header_plain",
    "_status_badge",
    "_visible_len",
    # Path helpers
    "_get_consumer_log_file",
    "_get_consumer_pid_file",
    "_get_opensearch_instance",
    "get_mage_project_path",
    "get_project_root",
    # Process management
    "get_process_end_time",
    "get_process_pid",
    "get_process_start_time",
    "is_process_running",
    "start_background_process",
    "stop_process",
    # Multi-consumer management
    "_count_running_consumers",
    "_get_all_consumer_pids",
    "_get_topic_partition_count",
    "_start_consumer_instance",
    "_stop_all_consumers",
    # OpenSearch consumer management
    "_is_opensearch_consumer_running",
    "_start_opensearch_consumer",
    "_stop_opensearch_consumer",
    # Kafka helpers
    "_get_kafka_admin_client",
    "_get_kafka_config",
    "_purge_kafka_topic",
    "_reset_consumer_group",
    # Database helpers
    "check_db_connection",
]
