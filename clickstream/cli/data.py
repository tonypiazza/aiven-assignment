# ==============================================================================
# Data Commands
# ==============================================================================
"""
Data management commands for the clickstream pipeline CLI.

Commands for resetting data stores (Kafka, PostgreSQL, Valkey, OpenSearch).
"""

import os
import warnings
from pathlib import Path
from typing import Annotated

import typer

from clickstream.cli.shared import (
    C,
    I,
    PRODUCER_LOG_FILE,
    PRODUCER_PID_FILE,
    _count_running_consumers,
    _is_opensearch_consumer_running,
    _purge_kafka_topic,
    _reset_consumer_group,
    check_db_connection,
    is_process_running,
)
from clickstream.utils.config import get_settings


# ==============================================================================
# Commands
# ==============================================================================


def data_reset(
    confirm: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
) -> None:
    """Reset all data stores (Kafka, PostgreSQL, Valkey, OpenSearch) and log files.

    This command performs a complete data reset:
    1. Purges Kafka topic (clickstream-events)
    2. Resets PostgreSQL schema (drops and recreates tables)
    3. Flushes all Valkey data
    4. Resets Kafka consumer group offsets
    5. Deletes OpenSearch index (if enabled)
    6. Deletes producer and consumer log files

    IMPORTANT: Consumer and producer must be stopped before running this command.

    Examples:
        clickstream data reset       # With confirmation prompt
        clickstream data reset -y    # Skip confirmation
    """
    from clickstream.utils.db import reset_schema
    from clickstream.utils.session_state import check_valkey_connection, get_valkey_client

    settings = get_settings()

    print()
    print(f"  Checking processes...")

    # Check PostgreSQL consumer is stopped (check for any running instances)
    running_consumers = _count_running_consumers()
    if running_consumers > 0:
        print(
            f"{C.BRIGHT_RED}{I.CROSS} PostgreSQL consumer is running ({running_consumers} instances) - "
            f"stop it first with '{C.WHITE}clickstream consumer stop{C.BRIGHT_RED}'{C.RESET}"
        )
        raise typer.Exit(1)
    print(f"{C.BRIGHT_GREEN}{I.CHECK} PostgreSQL consumer is stopped{C.RESET}")

    # Check OpenSearch consumer is stopped (if enabled)
    if settings.opensearch.enabled and _is_opensearch_consumer_running():
        print(
            f"{C.BRIGHT_RED}{I.CROSS} OpenSearch consumer is running - "
            f"stop it first with '{C.WHITE}clickstream consumer stop{C.BRIGHT_RED}'{C.RESET}"
        )
        raise typer.Exit(1)
    if settings.opensearch.enabled:
        print(f"{C.BRIGHT_GREEN}{I.CHECK} OpenSearch consumer is stopped{C.RESET}")

    # Check producer is stopped
    if is_process_running(PRODUCER_PID_FILE):
        print(
            f"{C.BRIGHT_RED}{I.CROSS} Producer is running - "
            f"stop it first with '{C.WHITE}clickstream producer stop{C.BRIGHT_RED}'{C.RESET}"
        )
        raise typer.Exit(1)
    print(f"{C.BRIGHT_GREEN}{I.CHECK} Producer is stopped{C.RESET}")

    print()

    # Confirm with user
    if not confirm:
        typer.confirm(
            "This will DELETE all data from Kafka, PostgreSQL, Valkey, OpenSearch, and log files. Are you sure?",
            abort=True,
        )
        print()

    # 1. Purge Kafka topic
    topic = settings.kafka.events_topic
    print(f"  Purging Kafka topic '{C.WHITE}{topic}{C.RESET}'...")
    success, partitions, error = _purge_kafka_topic(topic)
    if success:
        if partitions > 0:
            print(
                f"{C.BRIGHT_GREEN}{I.CHECK} Kafka topic purged "
                f"({C.WHITE}{partitions}{C.RESET} partitions){C.RESET}"
            )
        else:
            print(f"{C.BRIGHT_GREEN}{I.CHECK} Kafka topic purged (created new){C.RESET}")
    else:
        print(f"{C.BRIGHT_RED}{I.CROSS} Failed to purge Kafka topic: {error}{C.RESET}")
        raise typer.Exit(1)

    # 2. Reset PostgreSQL
    print(f"  Resetting PostgreSQL schema '{C.WHITE}{settings.postgres.schema_name}{C.RESET}'...")
    if not check_db_connection():
        print(f"{C.BRIGHT_RED}{I.CROSS} Cannot connect to PostgreSQL{C.RESET}")
        raise typer.Exit(1)

    try:
        reset_schema()
        print(f"{C.BRIGHT_GREEN}{I.CHECK} PostgreSQL reset{C.RESET}")
    except Exception as e:
        print(f"{C.BRIGHT_RED}{I.CROSS} Failed to reset PostgreSQL: {e}{C.RESET}")
        raise typer.Exit(1)

    # 3. Flush Valkey
    print(f"  Flushing Valkey...")
    if check_valkey_connection():
        try:
            client = get_valkey_client()
            client.flushall()
            print(f"{C.BRIGHT_GREEN}{I.CHECK} Valkey flushed{C.RESET}")
        except Exception as e:
            print(f"{C.BRIGHT_RED}{I.CROSS} Failed to flush Valkey: {e}{C.RESET}")
            raise typer.Exit(1)
    else:
        print(f"{C.BRIGHT_YELLOW}{I.STOP} Valkey not available, skipping{C.RESET}")

    # 4. Reset consumer groups
    # Reset PostgreSQL consumer group
    consumer_group = os.environ.get("POSTGRESQL_CONSUMER_GROUP_ID", "clickstream-postgresql")
    print(f"  Resetting PostgreSQL consumer group '{C.WHITE}{consumer_group}{C.RESET}'...")
    success, error = _reset_consumer_group(consumer_group)
    if success:
        print(f"{C.BRIGHT_GREEN}{I.CHECK} PostgreSQL consumer group reset{C.RESET}")
    else:
        print(
            f"{C.BRIGHT_YELLOW}{I.STOP} Could not reset PostgreSQL consumer group: {error}{C.RESET}"
        )

    # Always reset OpenSearch consumer group (it's a Kafka operation, works even if OpenSearch is disabled)
    os_consumer_group = settings.opensearch.consumer_group_id
    print(f"  Resetting OpenSearch consumer group '{C.WHITE}{os_consumer_group}{C.RESET}'...")
    success, error = _reset_consumer_group(os_consumer_group)
    if success:
        print(f"{C.BRIGHT_GREEN}{I.CHECK} OpenSearch consumer group reset{C.RESET}")
    else:
        print(
            f"{C.BRIGHT_YELLOW}{I.STOP} Could not reset OpenSearch consumer group: {error}{C.RESET}"
        )

    # 5. Delete OpenSearch index (try if reachable, regardless of enabled setting)
    index_name = settings.opensearch.events_index
    print(f"  Checking OpenSearch for cleanup...")
    try:
        import urllib3

        from opensearchpy import OpenSearch

        # Suppress SSL warnings for local self-signed certs
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        warnings.filterwarnings(
            "ignore", message="Connecting to .* using SSL with verify_certs=False"
        )

        client = OpenSearch(
            hosts=settings.opensearch.hosts,
            http_auth=(settings.opensearch.user, settings.opensearch.password),
            use_ssl=settings.opensearch.use_ssl,
            verify_certs=settings.opensearch.verify_certs,
            ssl_show_warn=False,
            timeout=5,
        )
        # Quick ping to check if reachable
        client.info()

        if client.indices.exists(index=index_name):
            client.indices.delete(index=index_name)
            print(f"{C.BRIGHT_GREEN}{I.CHECK} OpenSearch index '{index_name}' deleted{C.RESET}")
    except Exception:
        print(
            f"{C.BRIGHT_YELLOW}{I.CIRCLE} OpenSearch not reachable, skipping index cleanup{C.RESET}"
        )

    # 6. Delete log files
    print(f"  Deleting log files...")
    import glob as glob_module

    deleted = []
    # Delete producer log
    if PRODUCER_LOG_FILE.exists():
        PRODUCER_LOG_FILE.unlink()
        deleted.append(PRODUCER_LOG_FILE.name)
    # Delete all consumer log files (PostgreSQL and OpenSearch)
    for log_path in glob_module.glob("/tmp/consumer_*_*.log"):
        Path(log_path).unlink()
        deleted.append(Path(log_path).name)
    if deleted:
        print(f"{C.BRIGHT_GREEN}{I.CHECK} Log files deleted ({len(deleted)} files){C.RESET}")
    else:
        print(f"{C.BRIGHT_GREEN}{I.CHECK} No log files to delete{C.RESET}")

    print()
    print(f"{C.BRIGHT_GREEN}{I.CHECK} All data reset successfully{C.RESET}")
    print()
