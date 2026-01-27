#!/usr/bin/env python3
"""
OpenSearch consumer runner script - runs the OpenSearch streaming pipeline.
This script is started as a background process by 'clickstream consumer start'
when OPENSEARCH_ENABLED=true.

Uses a separate Mage AI streaming pipeline with its own Kafka consumer group,
allowing OpenSearch indexing to be enabled after events have been consumed
(automatic backfill from offset 0).
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
parser = argparse.ArgumentParser(description="OpenSearch consumer runner")
parser.add_argument("--instance", type=int, required=True, help="Consumer instance number")
args = parser.parse_args()

# Configure logging to only write to file (not console)
DEFAULT_LOG_FILE = f"/tmp/consumer_{args.instance}_opensearch.log"
LOG_FILE = os.environ.get("CLICKSTREAM_OPENSEARCH_LOG_FILE", DEFAULT_LOG_FILE)

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
        "OPENSEARCH_CONSUMER_GROUP_ID", "clickstream-opensearch"
    )

    # Change to Mage project directory
    os.chdir(mage_project)

    # Import Mage, suppressing "DBT library not installed" message
    with contextlib.redirect_stdout(io.StringIO()):
        from mage_ai.data_preparation.models.pipeline import Pipeline

    # Use the OpenSearch streaming pipeline
    pipeline = Pipeline.get("opensearch_consumer")
    if pipeline is None:
        logger.error("opensearch_consumer pipeline not found!")
        sys.exit(1)

    logger.info("OpenSearch consumer started (streaming mode)")
    logger.info("Pipeline: opensearch_consumer")

    try:
        # For streaming pipelines, execute_sync runs continuously
        pipeline.execute_sync()
    except GracefulShutdown:
        logger.info("OpenSearch consumer interrupted by shutdown signal.")
    except KeyboardInterrupt:
        logger.info("OpenSearch consumer interrupted by keyboard.")
    except Exception as e:
        logger.exception("OpenSearch consumer error: %s", e)
        sys.exit(1)

    logger.info("OpenSearch consumer shutdown complete.")


if __name__ == "__main__":
    main()
