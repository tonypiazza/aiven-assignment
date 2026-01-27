# ==============================================================================
# Streaming Data Exporter - Events to OpenSearch
# ==============================================================================
"""
Exports streaming events to OpenSearch for visual dashboards.

Uses Mage AI @streaming_sink decorator pattern.

This sink:
- Receives flat event records from transform_events transformer
- Uses deterministic _id for deduplication (based on unique key)
- Gracefully fails if OpenSearch is unavailable
- Can be disabled via OPENSEARCH_ENABLED=false
"""

import hashlib
import logging
from typing import Dict, List

from mage_ai.streaming.sinks.base_python import BasePythonSink

if "streaming_sink" not in dir():
    from mage_ai.data_preparation.decorators import streaming_sink

from clickstream.utils.config import get_settings

logger = logging.getLogger(__name__)

# Suppress noisy OpenSearch warnings
logging.getLogger("opensearch").setLevel(logging.ERROR)


@streaming_sink
class OpenSearchSink(BasePythonSink):
    """Streaming sink that indexes events to OpenSearch."""

    def init_client(self):
        """Initialize the OpenSearch client if enabled."""
        self.settings = get_settings()
        self.client = None
        self.index_name = self.settings.opensearch.events_index

        # Check if OpenSearch is enabled
        if not self.settings.opensearch.enabled:
            logger.info("OpenSearch indexing disabled (OPENSEARCH_ENABLED=false)")
            return

        # Try to connect
        try:
            from opensearchpy import OpenSearch

            self.client = OpenSearch(
                hosts=[
                    {
                        "host": self.settings.opensearch.host,
                        "port": self.settings.opensearch.port,
                    }
                ],
                http_auth=(self.settings.opensearch.user, self.settings.opensearch.password),
                use_ssl=self.settings.opensearch.use_ssl,
                verify_certs=self.settings.opensearch.verify_certs,
                ssl_show_warn=False,
            )
            # Test connection
            self.client.info()
            self._ensure_index_exists()
            logger.info("OpenSearch sink initialized (index=%s)", self.index_name)
        except Exception as e:
            logger.warning("OpenSearch unavailable, indexing disabled: %s", e)
            self.client = None

    def _ensure_index_exists(self):
        """Create the index if it doesn't exist."""
        if self.client.indices.exists(index=self.index_name):
            return

        index_body = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            },
            "mappings": {
                "properties": {
                    "event_time": {"type": "date"},
                    "visitor_id": {"type": "keyword"},
                    "event": {"type": "keyword"},
                    "item_id": {"type": "keyword"},
                    "transaction_id": {"type": "keyword"},
                }
            },
        }
        self.client.indices.create(index=self.index_name, body=index_body)
        logger.info("Created OpenSearch index: %s", self.index_name)

    def batch_write(self, messages: List[Dict]):
        """Index events to OpenSearch."""
        # Skip if client not available
        if self.client is None:
            return

        # Messages are now flat event records (no type wrapper)
        events = [msg for msg in messages if msg]

        if not events:
            return

        # Bulk index
        try:
            from opensearchpy.helpers import bulk

            def generate_actions():
                for event in events:
                    # Generate deterministic _id from unique key for deduplication
                    # Matches PostgreSQL unique constraint: (visitor_id, event_time, event, item_id)
                    unique_key = f"{event.get('visitor_id')}:{event.get('event_time')}:{event.get('event')}:{event.get('item_id')}"
                    doc_id = hashlib.md5(unique_key.encode()).hexdigest()

                    yield {
                        "_index": self.index_name,
                        "_id": doc_id,
                        "_source": {
                            "event_time": event.get("event_time"),
                            "visitor_id": event.get("visitor_id"),
                            "event": event.get("event"),
                            "item_id": event.get("item_id"),
                            "transaction_id": event.get("transaction_id"),
                        },
                    }

            success, failed = bulk(
                self.client,
                generate_actions(),
                stats_only=True,
                raise_on_error=False,
            )

            if failed > 0:
                logger.warning("OpenSearch indexing: %d succeeded, %d failed", success, failed)
            else:
                logger.debug("OpenSearch indexed %d events", success)

        except Exception as e:
            logger.warning("OpenSearch bulk indexing failed: %s", e)
