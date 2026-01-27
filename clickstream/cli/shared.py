# ==============================================================================
# Shared Utilities for CLI Commands
# ==============================================================================
"""
Shared utilities, constants, and helper functions used across CLI command modules.

This module provides:
- ANSI color codes and box-drawing characters
- Process management utilities
- Kafka helpers
- Box drawing helpers for formatted output
"""

import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from clickstream.utils.config import get_settings


# ==============================================================================
# Constants
# ==============================================================================

# Producer PID and log files
PRODUCER_PID_FILE = Path("/tmp/kafka_producer.pid")
PRODUCER_LOG_FILE = Path("/tmp/kafka_producer.log")

# Box drawing width (unified for all commands)
BOX_WIDTH = 68


def _get_consumer_pid_file(instance: int, consumer_type: str) -> Path:
    """Get PID file path for a consumer instance."""
    return Path(f"/tmp/consumer_{instance}_{consumer_type}.pid")


def _get_consumer_log_file(instance: int, consumer_type: str) -> Path:
    """Get log file path for a consumer instance."""
    return Path(f"/tmp/consumer_{instance}_{consumer_type}.log")


def _get_opensearch_instance() -> int:
    """Get the instance number for OpenSearch consumer (equals PostgreSQL partition count)."""
    return get_settings().kafka.events_topic_partitions


# ==============================================================================
# ANSI Colors and Box Drawing
# ==============================================================================


class Colors:
    """ANSI color codes for terminal output."""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    # Colors
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    # Bright colors
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_CYAN = "\033[96m"


class Box:
    """Unicode box-drawing characters."""

    # Single line
    H = "─"  # horizontal
    V = "│"  # vertical
    TL = "┌"  # top-left
    TR = "┐"  # top-right
    BL = "└"  # bottom-left
    BR = "┘"  # bottom-right
    LT = "├"  # left-tee
    RT = "┤"  # right-tee

    # Double line (for headers)
    DH = "═"
    DV = "║"
    DTL = "╔"
    DTR = "╗"
    DBL = "╚"
    DBR = "╝"


class Icons:
    """Status icons using Unicode symbols."""

    CHECK = "✓"
    CROSS = "✗"
    WARN = "!"
    CIRCLE = "●"
    BULLET = "•"
    ARROW = "→"
    KAFKA = "⚡"
    DATABASE = "◆"
    SEARCH = "◎"
    PLAY = "▶"
    STOP = "□"


# Module-level aliases for convenience
C, B, I = Colors, Box, Icons


# ==============================================================================
# Path/Project Helpers
# ==============================================================================


def get_project_root() -> Path:
    """Get the project root directory."""
    candidates = [
        Path.cwd(),
        Path(__file__).parent.parent.parent,  # commands/shared.py -> clickstream -> project
    ]
    for path in candidates:
        if (path / "pyproject.toml").exists():
            return path
    return Path.cwd()


def get_mage_project_path() -> Path:
    """Get the Mage AI project directory."""
    project_root = get_project_root()
    return project_root / "clickstream"


# ==============================================================================
# Database Helpers
# ==============================================================================


def check_db_connection() -> bool:
    """Check if PostgreSQL is reachable."""
    try:
        import psycopg2

        settings = get_settings()
        with psycopg2.connect(settings.postgres.connection_string, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except Exception:
        return False


# ==============================================================================
# Process Management Helpers
# ==============================================================================


def get_process_pid(pid_file: Path) -> Optional[int]:
    """Get the PID from a PID file, if the process is still running."""
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            # Check if process is still running
            os.kill(pid, 0)
            return pid
        except (ValueError, ProcessLookupError, PermissionError):
            pid_file.unlink(missing_ok=True)
    return None


def is_process_running(pid_file: Path) -> bool:
    """Check if a process is running based on its PID file."""
    return get_process_pid(pid_file) is not None


def get_process_start_time(pid: Optional[int]) -> Optional[str]:
    """Get the start time of a process from its PID using psutil."""
    if pid is None:
        return None
    try:
        import psutil

        proc = psutil.Process(pid)
        start_time = datetime.fromtimestamp(proc.create_time())
        return start_time.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def get_process_end_time(log_file: Path) -> Optional[str]:
    """Get the end time from the log file.

    Looks for a "shutdown complete" message first. If not found,
    falls back to the timestamp of the last log entry.
    """
    if not log_file.exists():
        return None
    try:
        last_end_time = None
        last_timestamp = None
        with open(log_file) as f:
            for line in f:
                # Try to extract timestamp from any log line
                # Format: "2026-01-27 14:57:05,142 - module - INFO - message"
                parts = line.split(" - ", 1)
                if parts and len(parts[0]) >= 19:
                    try:
                        timestamp = parts[0].split(",")[0]
                        # Basic validation: should look like a timestamp
                        if timestamp[4] == "-" and timestamp[10] == " ":
                            last_timestamp = timestamp
                    except (IndexError, ValueError):
                        pass
                # Prefer shutdown messages if found
                if "shutdown complete" in line.lower():
                    last_end_time = last_timestamp
        # Fall back to last log line timestamp if no shutdown message
        return last_end_time or last_timestamp
    except Exception:
        pass
    return None


def stop_process(pid_file: Path, name: str) -> bool:
    """Stop a process by its PID file. Returns True if stopped."""
    pid = get_process_pid(pid_file)
    if not pid:
        print(f"{C.BRIGHT_YELLOW}{I.STOP} {name} is not running{C.RESET}")
        return False

    print(f"  Stopping {name} (PID: {C.WHITE}{pid}{C.RESET})...")

    try:
        os.kill(pid, signal.SIGTERM)
        # Wait up to 10 seconds for graceful shutdown (allows batch processing to complete)
        for _ in range(20):
            time.sleep(0.5)
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
        else:
            # Force kill if still running
            print(f"  {name} not responding, force killing...")
            os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass  # Already terminated
    except PermissionError:
        print(f"{C.BRIGHT_RED}{I.CROSS} Permission denied to stop {name}{C.RESET}")
        return False

    pid_file.unlink(missing_ok=True)
    print(f"{C.BRIGHT_GREEN}{I.CHECK} {name} stopped{C.RESET}")
    return True


def start_background_process(
    script_path: Path,
    pid_file: Path,
    log_file: Path,
    name: str,
    extra_env: Optional[dict] = None,
) -> bool:
    """Start a background process. Returns True if started successfully."""
    project_root = get_project_root()

    env = os.environ.copy()
    python_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{project_root}:{python_path}" if python_path else str(project_root)

    if extra_env:
        env.update(extra_env)

    # Redirect stdout/stderr to devnull - the runner handles logging to file
    with open(os.devnull, "w") as devnull:
        process = subprocess.Popen(
            [sys.executable, str(script_path)],
            stdout=devnull,
            stderr=devnull,
            start_new_session=True,
            env=env,
            cwd=str(project_root),
        )

    # Save PID
    pid_file.write_text(str(process.pid))

    # Wait a moment and verify it started
    time.sleep(2)
    if is_process_running(pid_file):
        print(f"{C.BRIGHT_GREEN}{I.CHECK} {name} started{C.RESET}")
        print(f"  PID: {C.WHITE}{process.pid}{C.RESET}")
        print(f"  Log: {C.DIM}{log_file}{C.RESET}")
        return True
    else:
        print(f"{C.BRIGHT_RED}{I.CROSS} {name} failed to start{C.RESET}")
        print(f"  Check logs: {C.DIM}{log_file}{C.RESET}")
        return False


# ==============================================================================
# Multi-Consumer Process Management
# ==============================================================================


def _get_all_consumer_pids() -> list[tuple[int, int]]:
    """Get all running PostgreSQL consumer instances. Returns list of (instance, pid) tuples."""
    import glob

    running = []
    for pid_file in glob.glob("/tmp/consumer_*_postgresql.pid"):
        path = Path(pid_file)
        # Extract instance number from filename (consumer_N_postgresql.pid)
        try:
            parts = path.stem.split("_")
            instance = int(parts[1])  # consumer_N_postgresql -> N
            pid = get_process_pid(path)
            if pid is not None:
                running.append((instance, pid))
        except (ValueError, IndexError):
            continue
    return sorted(running)


def _count_running_consumers() -> int:
    """Count number of running consumer instances."""
    return len(_get_all_consumer_pids())


def _start_consumer_instance(
    runner_script: Path,
    instance: int,
    project_root: Path,
) -> bool:
    """Start a single PostgreSQL consumer instance. Returns True if started successfully."""
    pid_file = _get_consumer_pid_file(instance, "postgresql")
    log_file = _get_consumer_log_file(instance, "postgresql")

    env = os.environ.copy()
    python_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{project_root}:{python_path}" if python_path else str(project_root)

    # Redirect stdout/stderr to devnull - the runner handles logging to file
    with open(os.devnull, "w") as devnull:
        process = subprocess.Popen(
            [sys.executable, str(runner_script), "--instance", str(instance)],
            stdout=devnull,
            stderr=devnull,
            start_new_session=True,
            env=env,
            cwd=str(project_root),
        )

    # Save PID
    pid_file.write_text(str(process.pid))
    return True


def _stop_all_consumers() -> int:
    """Stop all running PostgreSQL consumer instances. Returns count of stopped processes."""
    import glob

    stopped = 0
    for pid_file_path in glob.glob("/tmp/consumer_*_postgresql.pid"):
        pid_file = Path(pid_file_path)
        pid = get_process_pid(pid_file)
        if pid:
            try:
                os.kill(pid, signal.SIGTERM)
                # Wait up to 10 seconds for graceful shutdown (allows batch processing to complete)
                for _ in range(20):
                    time.sleep(0.5)
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        break
                else:
                    # Force kill if still running
                    os.kill(pid, signal.SIGKILL)
                stopped += 1
            except ProcessLookupError:
                pass  # Already terminated
            except PermissionError:
                pass
        pid_file.unlink(missing_ok=True)
    return stopped


def _get_topic_partition_count(topic_name: str) -> Optional[int]:
    """Get the number of partitions for a Kafka topic. Returns None if topic doesn't exist."""
    try:
        admin = _get_kafka_admin_client()
        topics = admin.list_topics()
        if topic_name not in topics:
            admin.close()
            return None

        metadata = admin.describe_topics([topic_name])
        admin.close()

        if metadata:
            return len(metadata[0].get("partitions", []))
        return None
    except Exception:
        return None


# ==============================================================================
# OpenSearch Consumer Process Management
# ==============================================================================


def _is_opensearch_consumer_running() -> bool:
    """Check if the OpenSearch consumer is running."""
    instance = _get_opensearch_instance()
    pid_file = _get_consumer_pid_file(instance, "opensearch")
    return is_process_running(pid_file)


def _start_opensearch_consumer(project_root: Path) -> bool:
    """Start the OpenSearch consumer. Returns True if started successfully."""
    instance = _get_opensearch_instance()
    pid_file = _get_consumer_pid_file(instance, "opensearch")
    runner_script = project_root / "clickstream" / "opensearch_runner.py"

    env = os.environ.copy()
    python_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{project_root}:{python_path}" if python_path else str(project_root)

    # Redirect stdout/stderr to devnull - the runner handles logging to file
    with open(os.devnull, "w") as devnull:
        process = subprocess.Popen(
            [sys.executable, str(runner_script), "--instance", str(instance)],
            stdout=devnull,
            stderr=devnull,
            start_new_session=True,
            env=env,
            cwd=str(project_root),
        )

    # Save PID
    pid_file.write_text(str(process.pid))
    return True


def _stop_opensearch_consumer() -> bool:
    """Stop the OpenSearch consumer. Returns True if stopped."""
    instance = _get_opensearch_instance()
    pid_file = _get_consumer_pid_file(instance, "opensearch")
    pid = get_process_pid(pid_file)
    if not pid:
        return False

    try:
        os.kill(pid, signal.SIGTERM)
        # Wait up to 10 seconds for graceful shutdown (allows batch processing to complete)
        for _ in range(20):
            time.sleep(0.5)
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
        else:
            # Force kill if still running
            os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass  # Already terminated
    except PermissionError:
        return False

    pid_file.unlink(missing_ok=True)
    return True


# ==============================================================================
# Kafka Helpers
# ==============================================================================


def _get_kafka_config(timeout_ms: int = 10000) -> dict:
    """Get Kafka connection configuration dict.

    Args:
        timeout_ms: Request timeout in milliseconds (default: 10000)

    Returns:
        Dict with Kafka connection parameters
    """
    from clickstream.utils.kafka import build_kafka_config

    settings = get_settings()
    return build_kafka_config(settings.kafka, request_timeout_ms=timeout_ms)


def _get_kafka_admin_client():
    """Get a Kafka admin client with current settings."""
    from kafka import KafkaAdminClient

    return KafkaAdminClient(**_get_kafka_config())


def _purge_kafka_topic(topic_name: str) -> tuple[bool, int, str]:
    """
    Purge a Kafka topic by deleting and recreating it with the same config.

    Args:
        topic_name: Name of the topic to purge

    Returns:
        Tuple of (success, partition_count, error_message)
    """
    from kafka.admin import NewTopic
    from kafka.errors import UnknownTopicOrPartitionError

    try:
        admin = _get_kafka_admin_client()

        # Check if topic exists and get its config
        topics = admin.list_topics()
        if topic_name not in topics:
            admin.close()
            return True, 0, "Topic does not exist"

        # Get topic metadata for partition count
        metadata = admin.describe_topics([topic_name])
        if not metadata:
            admin.close()
            return False, 0, "Could not get topic metadata"

        topic_metadata = metadata[0]
        num_partitions = len(topic_metadata.get("partitions", []))

        # Get topic config for replication factor
        from kafka.admin import ConfigResource, ConfigResourceType

        config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
        configs = admin.describe_configs([config_resource])

        # Default replication factor if not found
        replication_factor = 1

        # Delete the topic
        try:
            admin.delete_topics([topic_name])
        except UnknownTopicOrPartitionError:
            pass  # Topic already deleted

        # Wait for deletion to complete
        time.sleep(2)

        # Recreate with same config
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions if num_partitions > 0 else 1,
            replication_factor=replication_factor,
        )
        admin.create_topics([new_topic])

        admin.close()
        return True, num_partitions, ""

    except Exception as e:
        return False, 0, str(e)


def _reset_consumer_group(group_id: str) -> tuple[bool, str]:
    """
    Reset a Kafka consumer group by deleting it.

    Args:
        group_id: Consumer group ID to reset

    Returns:
        Tuple of (success, error_message)
    """
    try:
        admin = _get_kafka_admin_client()

        # Delete the consumer group
        try:
            admin.delete_consumer_groups([group_id])
        except Exception:
            pass  # Group may not exist

        admin.close()
        return True, ""

    except Exception as e:
        return False, str(e)


# ==============================================================================
# Box Drawing Helpers
# ==============================================================================

# Regex pattern for stripping ANSI escape codes
_ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-9;]*m")


def _visible_len(s: str) -> int:
    """Calculate visible length of string, ignoring ANSI escape codes."""
    return len(_ANSI_ESCAPE_PATTERN.sub("", s))


def _box_header(title: str, width: int = BOX_WIDTH) -> str:
    """Create a single-line box header."""
    inner_width = width - 2
    title_padded = f" {title} "
    left_bar = (inner_width - len(title_padded)) // 2
    right_bar = inner_width - left_bar - len(title_padded)
    return (
        f"{C.CYAN}{B.TL}{B.H * left_bar}{C.BOLD}{C.WHITE}{title_padded}"
        f"{C.RESET}{C.CYAN}{B.H * right_bar}{B.TR}{C.RESET}"
    )


def _section_header(title: str, icon: str, width: int = BOX_WIDTH) -> str:
    """Create a section header with icon."""
    inner_width = width - 2
    title_with_icon = f" {icon} {title} "
    bar_len = inner_width - len(title_with_icon) - 1  # -1 for the first H after LT
    return f"{C.CYAN}{B.LT}{B.H}{C.BOLD}{title_with_icon}{C.RESET}{C.CYAN}{B.H * bar_len}{B.RT}{C.RESET}"


def _section_header_plain(title: str, width: int = BOX_WIDTH) -> str:
    """Create a section header without icon."""
    inner_width = width - 2
    title_padded = f" {title} "
    bar_len = inner_width - len(title_padded) - 1  # -1 for the first H after LT
    return (
        f"{C.CYAN}{B.LT}{B.H}{C.BOLD}{title_padded}{C.RESET}{C.CYAN}{B.H * bar_len}{B.RT}{C.RESET}"
    )


def _box_line(content: str, width: int = BOX_WIDTH) -> str:
    """Create a line inside the box with proper padding to right border."""
    inner_width = width - 2
    padding = inner_width - _visible_len(content)
    return f"{C.CYAN}{B.V}{C.RESET}{content}{' ' * padding}{C.CYAN}{B.V}{C.RESET}"


def _empty_line(width: int = BOX_WIDTH) -> str:
    """Create an empty line inside the box."""
    return f"{C.CYAN}{B.V}{' ' * (width - 2)}{B.V}{C.RESET}"


def _box_bottom(width: int = BOX_WIDTH) -> str:
    """Create a box bottom border with centered attribution."""
    text = " Demo by Tony Piazza "
    text_len = len(text)
    # Calculate padding on each side
    remaining = width - 2 - text_len  # -2 for corners
    left_pad = remaining // 2
    right_pad = remaining - left_pad
    return f"{C.CYAN}{B.BL}{B.H * left_pad}{text}{B.H * right_pad}{B.BR}{C.RESET}"


def _status_badge(status: str, is_ok: bool, is_stopped: bool = False) -> tuple[str, int]:
    """Create a colored status badge. Returns (formatted_string, visible_length)."""
    if is_ok:
        return f"{C.BRIGHT_GREEN}{I.CHECK} {status}{C.RESET}", len(status) + 2
    elif is_stopped:
        return f"{C.BRIGHT_YELLOW}{I.STOP} {status}{C.RESET}", len(status) + 2
    else:
        return f"{C.BRIGHT_RED}{I.CROSS} {status}{C.RESET}", len(status) + 2
