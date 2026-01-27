#!/usr/bin/env python3
"""
Consumer runner script - runs the streaming consumer pipeline.
This script is started as a background process by 'clickstream consumer start'.

Uses Mage AI streaming pipeline which maintains a persistent Kafka connection
and processes messages in real-time as they arrive.

Supports multiple instances via --instance argument for parallel consumption.
"""

import argparse
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

# Parse arguments before configuring logging
parser = argparse.ArgumentParser(description="Clickstream consumer runner")
parser.add_argument(
    "--instance", type=int, default=0, help="Consumer instance number (for parallel consumers)"
)
args = parser.parse_args()

# Configure logging to only write to file (not console)
# The log file path can be set by CLI or defaults to instance-specific file
DEFAULT_LOG_FILE = f"/tmp/consumer_{args.instance}_postgresql.log"
LOG_FILE = os.environ.get("CLICKSTREAM_LOG_FILE", DEFAULT_LOG_FILE)

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
logging.getLogger("opensearch").setLevel(logging.ERROR)
logging.getLogger("kafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class GracefulShutdown(Exception):
    """Exception raised to trigger graceful shutdown."""

    pass


def signal_handler(signum, frame):
    """Handle shutdown signals by raising exception."""
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

    # Set consumer group ID for last message timestamp tracking
    os.environ["CLICKSTREAM_CONSUMER_GROUP"] = os.environ.get(
        "POSTGRESQL_CONSUMER_GROUP_ID", "clickstream-postgresql"
    )

    # Change to Mage project directory
    os.chdir(mage_project)

    # Import Mage, suppressing "DBT library not installed" message
    with contextlib.redirect_stdout(io.StringIO()):
        from mage_ai.data_preparation.models.pipeline import Pipeline

    # Use the streaming pipeline
    pipeline = Pipeline.get("postgresql_consumer")
    if pipeline is None:
        logger.error("postgresql_consumer pipeline not found!")
        sys.exit(1)

    logger.info("Consumer started (streaming mode)")
    logger.info("Instance: %d", args.instance)
    logger.info("Pipeline: postgresql_consumer")

    try:
        # For streaming pipelines, execute_sync runs continuously
        pipeline.execute_sync()
    except GracefulShutdown:
        logger.info("Consumer interrupted by shutdown signal.")
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by keyboard.")
    except Exception as e:
        logger.exception("Consumer error: %s", e)
        sys.exit(1)

    logger.info("Consumer shutdown complete.")


if __name__ == "__main__":
    main()
