# ==============================================================================
# Benchmark Command
# ==============================================================================
"""
Benchmark command for the clickstream pipeline CLI.

Measures consumer throughput from Kafka to PostgreSQL.
"""

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Annotated

import typer

from clickstream.cli.shared import (
    C,
    I,
    PRODUCER_PID_FILE,
    _count_running_consumers,
    _get_all_consumer_pids,
    _purge_kafka_topic,
    _reset_consumer_group,
    _start_consumer_instance,
    _stop_all_consumers,
    check_db_connection,
    get_project_root,
    is_process_running,
)
from clickstream.utils.config import get_settings


# ==============================================================================
# Helper Functions
# ==============================================================================


def _get_pg_event_count() -> int:
    """Get current event count from PostgreSQL."""
    settings = get_settings()
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=settings.postgres.host,
            port=settings.postgres.port,
            user=settings.postgres.user,
            password=settings.postgres.password,
            dbname=settings.postgres.database,
            sslmode=settings.postgres.sslmode,
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {settings.postgres.schema_name}.events")
            result = cur.fetchone()
            count = result[0] if result else 0
        conn.close()
        return count
    except Exception:
        return 0


def _run_producer_blocking(limit: int) -> bool:
    """Run producer and wait for it to complete. Returns True if successful."""
    project_root = get_project_root()
    runner_script = project_root / "clickstream" / "producer_runner.py"

    env = os.environ.copy()
    python_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{project_root}:{python_path}" if python_path else str(project_root)
    env["PRODUCER_LIMIT"] = str(limit)

    # Run producer in foreground and wait for completion
    process = subprocess.Popen(
        [sys.executable, str(runner_script)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
        cwd=str(project_root),
    )

    # Wait for process to complete
    return_code = process.wait()
    return return_code == 0


def _wait_for_consumers_stable(limit: int, stable_threshold: int = 5) -> tuple[int, float]:
    """
    Wait until PostgreSQL event count stabilizes.

    Args:
        limit: Target event count (for progress bar)
        stable_threshold: Seconds of no change before considering stable

    Returns:
        Tuple of (final_count, duration_seconds)
    """
    start_time = time.time()
    last_count = 0
    stable_seconds = 0

    while stable_seconds < stable_threshold:
        count = _get_pg_event_count()

        # Calculate progress bar (round to avoid 99% when at 99.9%)
        percent = min(100, round(count / limit * 100))
        filled = int(percent / 5)  # 20 chars total, each = 5%
        bar = "█" * filled + "░" * (20 - filled)

        # Print progress (update in place)
        print(
            f"\r  • Waiting for consumers... [{bar}] {percent:3d}% ({count:,} events)",
            end="",
            flush=True,
        )

        # Only start stability countdown after we've seen at least 1 event
        if count > 0 and count == last_count:
            stable_seconds += 1
        else:
            stable_seconds = 0

        last_count = count
        time.sleep(1)

    # Print final state with newline (round to avoid 99% when at 99.9%)
    # Replace bullet with checkmark to indicate completion
    percent = min(100, round(last_count / limit * 100))
    filled = int(percent / 5)
    bar = "█" * filled + "░" * (20 - filled)
    print(
        f"\r  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Waiting for consumers... [{bar}] {percent:3d}% ({last_count:,} events)"
    )

    duration = time.time() - start_time - stable_threshold  # Subtract stabilization wait
    return last_count, duration


# ==============================================================================
# Commands
# ==============================================================================


def benchmark(
    limit: Annotated[
        int, typer.Option("--limit", "-l", help="Number of events to produce")
    ] = 100000,
    output: Annotated[Path, typer.Option("--output", "-o", help="Output CSV file")] = Path(
        "benchmark_results.csv"
    ),
    confirm: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
) -> None:
    """Benchmark consumer throughput.

    Measures how quickly consumers process events from Kafka to PostgreSQL.
    The number of consumers equals KAFKA_EVENTS_TOPIC_PARTITIONS.

    Before running, ensure KAFKA_NUM_PARTITIONS in docker-compose.yml matches
    KAFKA_EVENTS_TOPIC_PARTITIONS in your .env file.

    Examples:
        clickstream benchmark --limit 100000 -y
        clickstream benchmark --limit 500000 --output results.csv -y
    """
    from datetime import datetime

    from clickstream.utils.db import reset_schema
    from clickstream.utils.session_state import check_valkey_connection, get_valkey_client

    settings = get_settings()
    partitions = settings.kafka.events_topic_partitions

    print()
    print(f"  {C.BOLD}Benchmark Configuration{C.RESET}")
    print(f"  • Partitions/Consumers: {C.WHITE}{partitions}{C.RESET}")
    print(f"  • Events to produce:    {C.WHITE}{limit:,}{C.RESET}")
    print(f"  • Output file:          {C.WHITE}{output}{C.RESET}")
    print()

    # Confirmation prompt
    if not confirm:
        print(
            f"{C.BRIGHT_YELLOW}{I.WARN} This will reset all data before running the benchmark.{C.RESET}"
        )
        typer.confirm("Continue?", abort=True)
        print()

    # Pre-flight checks
    print(f"  {C.BOLD}Pre-flight checks{C.RESET}")

    # Check consumer is stopped
    running_consumers = _count_running_consumers()
    if running_consumers > 0:
        print(
            f"{C.BRIGHT_RED}{I.CROSS} Consumer is running ({running_consumers} instances) - "
            f"stop it first with '{C.WHITE}clickstream consumer stop{C.BRIGHT_RED}'{C.RESET}"
        )
        raise typer.Exit(1)
    print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Consumer is stopped")

    # Check producer is stopped
    if is_process_running(PRODUCER_PID_FILE):
        print(
            f"{C.BRIGHT_RED}{I.CROSS} Producer is running - "
            f"stop it first with '{C.WHITE}clickstream producer stop{C.BRIGHT_RED}'{C.RESET}"
        )
        raise typer.Exit(1)
    print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Producer is stopped")

    # Check services
    if not check_db_connection():
        print(f"{C.BRIGHT_RED}{I.CROSS} PostgreSQL is unreachable{C.RESET}")
        raise typer.Exit(1)
    print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} PostgreSQL is reachable")

    if not check_valkey_connection():
        print(f"{C.BRIGHT_RED}{I.CROSS} Valkey is unreachable{C.RESET}")
        raise typer.Exit(1)
    print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Valkey is reachable")

    print()
    print(f"  {C.BOLD}Running benchmark{C.RESET}")

    # Reset data
    try:
        # Purge Kafka topic
        topic = settings.kafka.events_topic
        _purge_kafka_topic(topic)

        # Reset PostgreSQL
        reset_schema()

        # Flush Valkey
        client = get_valkey_client()
        client.flushall()

        # Reset consumer group
        consumer_group = os.environ.get("POSTGRESQL_CONSUMER_GROUP_ID", "clickstream-postgresql")
        _reset_consumer_group(consumer_group)

        print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Resetting data")
    except Exception as e:
        print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Resetting data")
        print(f"    {C.BRIGHT_RED}Failed: {e}{C.RESET}")
        raise typer.Exit(1)

    # Start consumers
    try:
        project_root = get_project_root()
        runner_script = project_root / "clickstream" / "consumer_runner.py"

        # Start all instances (staggered to avoid Mage log directory race condition)
        for i in range(partitions):
            _start_consumer_instance(runner_script, i, project_root)
            if i < partitions - 1:
                time.sleep(1)

        # Wait for processes to start
        time.sleep(2)

        running = _get_all_consumer_pids()
        if len(running) != partitions:
            print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Starting {partitions} consumer(s)")
            print(f"    {C.BRIGHT_RED}Only {len(running)}/{partitions} consumers started{C.RESET}")
            raise typer.Exit(1)

        print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Starting {partitions} consumer(s)")
    except typer.Exit:
        raise
    except Exception as e:
        print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Starting {partitions} consumer(s)")
        print(f"    {C.BRIGHT_RED}Failed: {e}{C.RESET}")
        raise typer.Exit(1)

    # Run producer and wait for completion
    producer_success = _run_producer_blocking(limit)
    if not producer_success:
        print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Running producer ({limit:,} events)")
        _stop_all_consumers()
        raise typer.Exit(1)
    print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Running producer ({limit:,} events)")

    # Wait for consumers to finish processing
    events_consumed, duration = _wait_for_consumers_stable(limit)

    # Stop consumers
    _stop_all_consumers()
    print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Stopping consumers")

    # Calculate throughput (rounded to whole number)
    throughput = round(events_consumed / duration) if duration > 0 else 0

    print()
    print(f"  {C.BOLD}Results{C.RESET}")
    print(f"  • Events consumed: {C.WHITE}{events_consumed:,}{C.RESET}")
    print(f"  • Duration:        {C.WHITE}{round(duration)} seconds{C.RESET}")
    print(f"  • Throughput:      {C.WHITE}{throughput:,} events/sec{C.RESET}")

    # Write results to CSV
    timestamp = datetime.now().isoformat(timespec="seconds")
    csv_header = (
        "timestamp,partitions,events_produced,events_consumed,duration_sec,throughput_events_sec\n"
    )
    csv_row = f"{timestamp},{partitions},{limit},{events_consumed},{round(duration)},{throughput}\n"

    # Append to file (create with header if doesn't exist)
    write_header = not output.exists()
    with open(output, "a") as f:
        if write_header:
            f.write(csv_header)
        f.write(csv_row)

    print()
    print(f"{C.BRIGHT_GREEN}{I.CHECK} Results appended to {output}{C.RESET}")
    print()
