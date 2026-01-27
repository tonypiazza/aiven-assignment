# ==============================================================================
# Application Configuration
# ==============================================================================
"""
Configuration management using pydantic-settings.

All configuration is loaded from environment variables, with support for
.env files via python-dotenv.
"""

from functools import lru_cache
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load .env file before any settings are instantiated
load_dotenv()


class AivenSettings(BaseSettings):
    """Aiven API settings for fast service status checks.

    When configured, the CLI can use the Aiven REST API to check service
    status instead of making direct connections. This is significantly
    faster for Aiven-hosted services, especially Kafka (~30s -> ~1-2s).
    """

    model_config = SettingsConfigDict(env_prefix="AIVEN_")

    api_token: Optional[str] = Field(default=None, description="Aiven API token")
    project_name: Optional[str] = Field(default=None, description="Aiven project name")

    # Service names (derived from project_name by default)
    # Override these if using custom service names
    kafka_service_name: Optional[str] = Field(
        default=None, description="Kafka service name (defaults to {project}-kafka)"
    )
    pg_service_name: Optional[str] = Field(
        default=None, description="PostgreSQL service name (defaults to {project}-pg)"
    )
    opensearch_service_name: Optional[str] = Field(
        default=None, description="OpenSearch service name (defaults to {project}-opensearch)"
    )
    valkey_service_name: Optional[str] = Field(
        default=None, description="Valkey service name (defaults to {project}-valkey)"
    )

    @property
    def is_configured(self) -> bool:
        """Check if Aiven API is configured."""
        return bool(self.api_token and self.project_name)

    def get_service_name(self, service_type: str) -> str:
        """Get service name for a given type (kafka, pg, opensearch, valkey).

        Args:
            service_type: One of 'kafka', 'pg', 'opensearch', 'valkey'

        Returns:
            Service name, either explicitly configured or derived from project name.
            Returns empty string if neither is available.
        """
        # Check for explicit override
        explicit = getattr(self, f"{service_type}_service_name", None)
        if explicit:
            return explicit
        # Derive from project name
        if self.project_name:
            return f"{self.project_name}-{service_type}"
        return ""


class KafkaSettings(BaseSettings):
    """Kafka connection settings."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = Field(..., description="Kafka bootstrap servers")
    security_protocol: str = Field(
        default="PLAINTEXT", description="Security protocol (PLAINTEXT or SSL)"
    )

    # SSL settings for Aiven mTLS authentication
    ssl_ca_file: Optional[str] = Field(default=None, description="Path to CA certificate file")
    ssl_cert_file: Optional[str] = Field(
        default=None, description="Path to client certificate file"
    )
    ssl_key_file: Optional[str] = Field(default=None, description="Path to client private key file")

    # Topic configuration
    events_topic: str = Field(default="clickstream-events", description="Events topic name")
    events_topic_partitions: int = Field(
        default=3, description="Number of partitions for events topic (and consumer instances)"
    )


class PostgresSettings(BaseSettings):
    """PostgreSQL connection settings."""

    model_config = SettingsConfigDict(env_prefix="PG_")

    host: str = Field(..., description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    user: str = Field(..., description="PostgreSQL username")
    password: str = Field(..., description="PostgreSQL password")
    database: str = Field(default="clickstream", description="Database name")
    schema_name: str = Field(default="clickstream", description="Schema name")
    sslmode: str = Field(default="require", description="SSL mode")

    @property
    def connection_string(self) -> str:
        """Build PostgreSQL connection string."""
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}?sslmode={self.sslmode}"
        )


class ValkeySettings(BaseSettings):
    """Valkey (Redis-compatible) connection settings for session state."""

    model_config = SettingsConfigDict(env_prefix="VALKEY_")

    host: str = Field(default="localhost", description="Valkey host")
    port: int = Field(default=6379, description="Valkey port")
    password: Optional[str] = Field(default=None, description="Valkey password")
    db: int = Field(default=0, description="Valkey database number")
    ssl: bool = Field(default=False, description="Use SSL/TLS connection")

    # Session state configuration
    session_ttl_hours: int = Field(default=24, description="TTL for session state in hours")

    @property
    def url(self) -> str:
        """Build Valkey connection URL."""
        # Use rediss:// scheme for SSL connections (Aiven requires SSL)
        scheme = "rediss" if self.ssl else "redis"
        if self.password:
            return f"{scheme}://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"{scheme}://{self.host}:{self.port}/{self.db}"


class OpenSearchSettings(BaseSettings):
    """OpenSearch connection settings."""

    model_config = SettingsConfigDict(env_prefix="OPENSEARCH_")

    enabled: bool = Field(default=False, description="Enable OpenSearch indexing")
    host: str = Field(default="localhost", description="OpenSearch host")
    port: int = Field(default=9200, description="OpenSearch port")
    user: str = Field(default="admin", description="OpenSearch username")
    password: str = Field(default="admin", description="OpenSearch password")
    use_ssl: bool = Field(default=True, description="Use SSL")
    verify_certs: bool = Field(
        default=True, description="Verify SSL certificates (False for local self-signed)"
    )

    # Index names
    events_index: str = Field(default="clickstream-events", description="Events index name")

    # Consumer settings for separate OpenSearch consumer pipeline
    consumer_group_id: str = Field(
        default="clickstream-opensearch",
        description="Kafka consumer group ID for OpenSearch consumer",
    )

    @property
    def hosts(self) -> list[dict]:
        """Build OpenSearch hosts configuration."""
        return [
            {
                "host": self.host,
                "port": self.port,
            }
        ]


class ProducerSettings(BaseSettings):
    """Event producer settings."""

    model_config = SettingsConfigDict(env_prefix="PRODUCER_")

    data_file: Path = Field(default=Path("data/events.csv"), description="Path to events CSV file")

    @property
    def data_file_path(self) -> Path:
        """Resolve data file to absolute path from project root."""
        if self.data_file.is_absolute():
            return self.data_file
        # Resolve relative to project root (parent of clickstream)
        project_root = Path(__file__).parent.parent.parent
        return project_root / self.data_file


class ConsumerSettings(BaseSettings):
    """Event consumer settings."""

    model_config = SettingsConfigDict(env_prefix="CONSUMER_")

    group_id: str = Field(default="clickstream-consumer", description="Kafka consumer group ID")
    session_timeout_minutes: int = Field(
        default=30, description="Session inactivity timeout in minutes"
    )
    auto_offset_reset: str = Field(default="earliest", description="Auto offset reset policy")
    batch_size: int = Field(
        default=1000,
        description="Number of messages to fetch per Kafka poll (higher = better throughput)",
    )
    poll_timeout_ms: int = Field(default=250, description="Kafka poll timeout in milliseconds")


class Settings(BaseSettings):
    """Main application settings."""

    model_config = SettingsConfigDict(
        extra="ignore",
    )

    # Nested settings
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    opensearch: OpenSearchSettings = Field(default_factory=OpenSearchSettings)
    valkey: ValkeySettings = Field(default_factory=ValkeySettings)
    producer: ProducerSettings = Field(default_factory=ProducerSettings)
    consumer: ConsumerSettings = Field(default_factory=ConsumerSettings)
    aiven: AivenSettings = Field(default_factory=AivenSettings)

    # General settings
    debug: bool = Field(default=False, description="Enable debug mode")
    log_level: str = Field(default="INFO", description="Logging level")


@lru_cache
def get_settings() -> Settings:
    """
    Get cached application settings.

    Settings are loaded once and cached for subsequent calls.
    """
    return Settings()
