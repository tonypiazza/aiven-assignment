# ==============================================================================
# Streaming Data Exporter - Sessions to PostgreSQL (Bulk Upsert)
# ==============================================================================
"""
Exports streaming sessions to PostgreSQL using bulk upserts.

Uses psycopg2.extras.execute_batch() for high-performance bulk upserts,
replacing the slow row-by-row inserts of Mage's built-in YAML exporter.

Sessions are upserted (INSERT ... ON CONFLICT DO UPDATE) because session
data is continuously updated as new events arrive for a visitor.

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
# 1000 works well with typical session batches (~700 per 1000 events)
PAGE_SIZE = 1000

# Connection timeout in seconds
CONNECT_TIMEOUT = 10


@streaming_sink
class PostgresSessionsSink(BasePythonSink):
    """Streaming sink that bulk-upserts sessions to PostgreSQL with retry logic."""

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
                "PostgresSessionsSink initialized (schema=%s, page_size=%d)",
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
        Bulk upsert sessions to PostgreSQL with retry logic.

        Retries on connection errors with exponential backoff (10 attempts, ~60 seconds).
        Automatically reconnects if the connection is lost.

        Args:
            messages: List of session dictionaries with keys:
                - session_id: str
                - visitor_id: int
                - session_start: ISO format timestamp string
                - session_end: ISO format timestamp string
                - duration_seconds: int
                - event_count: int
                - view_count: int
                - cart_count: int
                - transaction_count: int
                - items_viewed: list of ints
                - items_carted: list of ints
                - items_purchased: list of ints
                - converted: bool
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
                    INSERT INTO {self.schema}.sessions
                        (session_id, visitor_id, session_start, session_end,
                         duration_seconds, event_count, view_count, cart_count,
                         transaction_count, items_viewed, items_carted,
                         items_purchased, converted)
                    VALUES
                        (%(session_id)s, %(visitor_id)s, %(session_start)s, %(session_end)s,
                         %(duration_seconds)s, %(event_count)s, %(view_count)s, %(cart_count)s,
                         %(transaction_count)s, %(items_viewed)s, %(items_carted)s,
                         %(items_purchased)s, %(converted)s)
                    ON CONFLICT (session_id) DO UPDATE SET
                        session_end = EXCLUDED.session_end,
                        duration_seconds = EXCLUDED.duration_seconds,
                        event_count = EXCLUDED.event_count,
                        view_count = EXCLUDED.view_count,
                        cart_count = EXCLUDED.cart_count,
                        transaction_count = EXCLUDED.transaction_count,
                        items_viewed = EXCLUDED.items_viewed,
                        items_carted = EXCLUDED.items_carted,
                        items_purchased = EXCLUDED.items_purchased,
                        converted = EXCLUDED.converted
                    """,
                    messages,
                    page_size=PAGE_SIZE,
                )
            self.conn.commit()
            logger.info("Bulk upserted %d sessions", len(messages))

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
            logger.error("Failed to bulk upsert sessions: %s", e)
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
                logger.info("PostgresSessionsSink connection closed")
            except Exception as e:
                logger.warning("Error closing PostgreSQL connection: %s", e)
