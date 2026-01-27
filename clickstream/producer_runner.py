#!/usr/bin/env python3
"""
Producer runner script - runs the producer pipeline.
This script is started as a background process by 'clickstream producer start'.
"""

import contextlib
import io
import logging
import os
import signal
import sys
import warnings
from pathlib import Path

# Suppress noisy warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Configure logging to only write to file (not console)
LOG_FILE = os.environ.get("CLICKSTREAM_LOG_FILE", "/tmp/kafka_producer.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
    ],
)

# Suppress noisy third-party loggers
logging.getLogger("traitlets").setLevel(logging.ERROR)
logging.getLogger("mage_ai").setLevel(logging.ERROR)
logging.getLogger("kafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class GracefulShutdown(Exception):
    """Exception raised to trigger graceful shutdown."""

    pass


def signal_handler(signum, frame):
    """Handle shutdown signals by raising an exception to interrupt blocking calls."""
    logger.info("Received signal %d, shutting down gracefully...", signum)
    raise GracefulShutdown()


def main():
    # Set up signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Find project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    mage_project = script_dir

    # Set up environment
    python_path = os.environ.get("PYTHONPATH", "")
    if str(project_root) not in python_path:
        os.environ["PYTHONPATH"] = (
            f"{project_root}:{python_path}" if python_path else str(project_root)
        )

    # Change to Mage project directory
    os.chdir(mage_project)

    # Get options from environment
    # PRODUCER_SPEED presence implies realtime mode; absence means batch mode
    speed_env = os.environ.get("PRODUCER_SPEED")
    realtime_mode = speed_env is not None
    speed = int(speed_env) if speed_env else 1
    limit = os.environ.get("PRODUCER_LIMIT")
    limit = int(limit) if limit else None

    # Import Mage, suppressing "DBT library not installed" message
    with contextlib.redirect_stdout(io.StringIO()):
        from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.get("kafka_producer")
    if pipeline is None:
        logger.error("kafka_producer pipeline not found!")
        sys.exit(1)

    logger.info("Producer started")
    if realtime_mode:
        logger.info("Mode: Real-time (%dx)", speed)
    else:
        logger.info("Mode: Batch (no delays)")
    if limit:
        logger.info("Limit: %d events", limit)
    else:
        logger.info("Limit: All events")

    global_vars = {
        "speed": speed,
        "realtime_mode": realtime_mode,
    }
    if limit:
        global_vars["limit"] = limit

    try:
        pipeline.execute_sync(global_vars=global_vars)
        logger.info("Producer completed - all events published.")
    except GracefulShutdown:
        logger.info("Producer interrupted by shutdown signal.")
    except Exception as e:
        logger.exception("Producer error: %s", e)
        sys.exit(1)

    logger.info("Producer shutdown complete.")


if __name__ == "__main__":
    main()
