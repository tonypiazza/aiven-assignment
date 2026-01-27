# ==============================================================================
# Streaming Data Exporter - Events to PostgreSQL (Bulk Insert)
# ==============================================================================
"""
Exports streaming events to PostgreSQL using bulk inserts.

Uses psycopg2.extras.execute_batch() for high-performance bulk upserts,
replacing the slow row-by-row inserts of Mage's built-in YAML exporter.

Performance improvement: ~20-50x faster than the default YAML exporter
when writing to remote databases (e.g., Aiven) due to reduced network
round-trips.
"""

import logging
from typing import Dict, List

import psycopg2
from psycopg2.extras import execute_batch
from mage_ai.streaming.sinks.base_python import BasePythonSink

if "streaming_sink" not in dir():
    from mage_ai.data_preparation.decorators import streaming_sink

from clickstream.utils.config import get_settings

logger = logging.getLogger(__name__)

# Number of rows per batch sent to the server
# Higher = fewer round-trips, but more memory usage
# 1000 works well with batch_size=1000-2000 from Kafka
PAGE_SIZE = 1000


@streaming_sink
class PostgresEventsSink(BasePythonSink):
    """Streaming sink that bulk-inserts events to PostgreSQL."""

    def init_client(self):
        """Initialize PostgreSQL connection."""
        self.settings = get_settings()
        self.schema = self.settings.postgres.schema_name
        self.conn = None

        try:
            self.conn = psycopg2.connect(self.settings.postgres.connection_string)
            logger.info(
                "PostgresEventsSink initialized (schema=%s, page_size=%d)",
                self.schema,
                PAGE_SIZE,
            )
        except Exception as e:
            logger.error("Failed to connect to PostgreSQL: %s", e)
            raise

    def batch_write(self, messages: List[Dict]):
        """
        Bulk insert events to PostgreSQL.

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

        if self.conn is None:
            logger.error("PostgreSQL connection not available")
            return

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

        except Exception as e:
            logger.error("Failed to bulk insert events: %s", e)
            if self.conn:
                self.conn.rollback()
            raise

    def destroy(self):
        """Clean up PostgreSQL connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.info("PostgresEventsSink connection closed")
            except Exception as e:
                logger.warning("Error closing PostgreSQL connection: %s", e)
