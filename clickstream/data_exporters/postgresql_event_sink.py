# ==============================================================================
# Streaming Data Exporter - Events to PostgreSQL (Bulk Insert)
# ==============================================================================
"""
Exports streaming events to PostgreSQL using bulk inserts.

Uses psycopg2.extras.execute_batch() for high-performance bulk upserts,
replacing the slow row-by-row inserts of Mage's built-in YAML exporter.

Includes retry logic with exponential backoff for network resilience.

Performance improvement: ~20-50x faster than the default YAML exporter
when writing to remote databases (e.g., Aiven) due to reduced network
round-trips.
"""

import logging
from typing import Dict, List

import psycopg2
from psycopg2.extras import execute_batch
from mage_ai.streaming.sinks.base_python import BasePythonSink
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

if "streaming_sink" not in dir():
    from mage_ai.data_preparation.decorators import streaming_sink

from clickstream.utils.config import get_settings
from clickstream.utils.retry import (
    RETRY_ATTEMPTS,
    RETRY_WAIT_MIN,
    RETRY_WAIT_MAX,
    log_retry_attempt,
)

logger = logging.getLogger(__name__)

# Number of rows per batch sent to the server
# Higher = fewer round-trips, but more memory usage
# 1000 works well with batch_size=1000-2000 from Kafka
PAGE_SIZE = 1000

# Connection timeout in seconds
CONNECT_TIMEOUT = 10


@streaming_sink
class PostgresEventsSink(BasePythonSink):
    """Streaming sink that bulk-inserts events to PostgreSQL with retry logic."""

    def init_client(self):
        """Initialize PostgreSQL connection."""
        self.settings = get_settings()
        self.schema = self.settings.postgres.schema_name
        self.conn = None
        self._connect()

    def _connect(self):
        """
        Establish PostgreSQL connection with connection timeout.

        Raises:
            Exception: If connection fails
        """
        try:
            conn_string = self.settings.postgres.connection_string
            # Add connect_timeout if not already in connection string
            if "connect_timeout" not in conn_string:
                separator = "&" if "?" in conn_string else "?"
                conn_string = f"{conn_string}{separator}connect_timeout={CONNECT_TIMEOUT}"

            self.conn = psycopg2.connect(conn_string)
            logger.info(
                "PostgresEventsSink initialized (schema=%s, page_size=%d)",
                self.schema,
                PAGE_SIZE,
            )
        except Exception as e:
            logger.error("Failed to connect to PostgreSQL: %s", e)
            raise

    def _ensure_connection(self):
        """
        Ensure PostgreSQL connection is alive, reconnecting if necessary.

        Raises:
            Exception: If reconnection fails
        """
        if self.conn is None or self.conn.closed:
            logger.warning("PostgreSQL connection lost, attempting to reconnect...")
            self._connect()
            return

        # Test if connection is still alive
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.warning("PostgreSQL connection stale, reconnecting...")
            try:
                self.conn.close()
            except Exception:
                pass
            self._connect()

    @retry(
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
        retry=retry_if_exception_type((psycopg2.OperationalError, psycopg2.InterfaceError)),
        reraise=True,
    )
    def batch_write(self, messages: List[Dict]):
        """
        Bulk insert events to PostgreSQL with retry logic.

        Retries on connection errors with exponential backoff (10 attempts, ~60 seconds).
        Automatically reconnects if the connection is lost.

        Args:
            messages: List of event dictionaries with keys:
                - event_time: ISO format timestamp string
                - visitor_id: int
                - event: str (view, addtocart, transaction)
                - item_id: int
                - transaction_id: int or None
        """
        if not messages:
            return

        # Ensure connection is alive before attempting write
        self._ensure_connection()

        if self.conn is None:
            logger.error("PostgreSQL connection not available")
            raise psycopg2.InterfaceError("PostgreSQL connection not available")

        try:
            with self.conn.cursor() as cur:
                execute_batch(
                    cur,
                    f"""
                    INSERT INTO {self.schema}.events
                        (event_time, visitor_id, event, item_id, transaction_id)
                    VALUES
                        (%(event_time)s, %(visitor_id)s, %(event)s::{self.schema}.event_type,
                         %(item_id)s, %(transaction_id)s)
                    ON CONFLICT (visitor_id, event_time, event, item_id) DO NOTHING
                    """,
                    messages,
                    page_size=PAGE_SIZE,
                )
            self.conn.commit()
            logger.info("Bulk inserted %d events", len(messages))

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            # Connection error - will be retried
            logger.warning("Connection error during batch write, will retry: %s", e)
            if self.conn:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
            # Clear connection so _ensure_connection will reconnect on retry
            self.conn = None
            raise

        except Exception as e:
            logger.error("Failed to bulk insert events: %s", e)
            if self.conn:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
            raise

    def destroy(self):
        """Clean up PostgreSQL connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.info("PostgresEventsSink connection closed")
            except Exception as e:
                logger.warning("Error closing PostgreSQL connection: %s", e)
