# ==============================================================================
# Database Utilities
# ==============================================================================
"""
Database utility functions for the clickstream pipeline.

Provides schema initialization and other database helpers.
"""

import logging
from pathlib import Path

import psycopg2
from jinja2 import Template

from clickstream.utils.config import get_settings

logger = logging.getLogger(__name__)


def get_schema_file() -> Path | None:
    """Get the schema init.sql path, or None if not found."""
    candidates = [
        Path(__file__).parent.parent.parent / "schema" / "init.sql",
        Path.cwd() / "schema" / "init.sql",
        Path("schema/init.sql"),
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def render_schema_sql(schema_name: str) -> str:
    """Render the schema SQL template with the given schema name."""
    schema_file = get_schema_file()
    if not schema_file:
        raise RuntimeError(
            "Schema file (schema/init.sql) not found. "
            "Make sure you're running from the project root."
        )

    template = Template(schema_file.read_text())
    return template.render(schema_name=schema_name)


def ensure_database_exists() -> None:
    """
    Ensure the target database exists, creating it if needed.

    Connects to the 'postgres' database (which always exists) to check
    and create the target database.

    Raises:
        RuntimeError: If database creation fails
    """
    settings = get_settings()
    target_db = settings.postgres.database

    # Build connection string to 'postgres' database instead of target
    postgres_conn_string = (
        f"postgresql://{settings.postgres.user}:{settings.postgres.password}@"
        f"{settings.postgres.host}:{settings.postgres.port}/postgres"
        f"?sslmode={settings.postgres.sslmode}"
    )

    try:
        # Connect to postgres database to check/create target database
        # Use autocommit because CREATE DATABASE cannot run inside a transaction
        conn = psycopg2.connect(postgres_conn_string, connect_timeout=5)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                # Check if database exists
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (target_db,),
                )
                if cur.fetchone() is None:
                    # Database doesn't exist, create it
                    logger.info("Creating database '%s'...", target_db)
                    # Use quote_ident to safely escape the database name
                    cur.execute(f'CREATE DATABASE "{target_db}"')
                    logger.info("Database '%s' created.", target_db)
        finally:
            conn.close()
    except Exception as e:
        raise RuntimeError(f"Failed to ensure database exists: {e}") from e


def check_schema_exists() -> bool:
    """Check if the database schema (events table) exists."""
    try:
        settings = get_settings()
        schema_name = settings.postgres.schema_name
        with psycopg2.connect(settings.postgres.connection_string, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s 
                        AND table_name = 'events'
                    )
                    """,
                    (schema_name,),
                )
                result = cur.fetchone()
                return result[0] if result else False
    except Exception:
        return False


def ensure_schema() -> None:
    """
    Ensure database schema exists, initializing if needed.

    This function is idempotent and safe to call multiple times.
    It will create the database if it doesn't exist.

    Raises:
        RuntimeError: If schema file not found or initialization fails
    """
    # First ensure the database itself exists
    ensure_database_exists()

    if check_schema_exists():
        return

    settings = get_settings()
    schema_name = settings.postgres.schema_name

    logger.info("Initializing database schema '%s'...", schema_name)

    try:
        schema_sql = render_schema_sql(schema_name)
        with psycopg2.connect(settings.postgres.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(schema_sql)
            conn.commit()
        logger.info("Database schema '%s' initialized.", schema_name)
    except Exception as e:
        raise RuntimeError(f"Failed to initialize schema: {e}") from e


def reset_schema() -> None:
    """
    Drop and recreate the database schema.

    WARNING: This deletes all data in the schema!
    """
    settings = get_settings()
    schema_name = settings.postgres.schema_name

    try:
        schema_sql = render_schema_sql(schema_name)
        with psycopg2.connect(settings.postgres.connection_string) as conn:
            with conn.cursor() as cur:
                # Drop schema with all objects
                cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
                # Run the full init (which creates the schema)
                cur.execute(schema_sql)
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Failed to reset schema: {e}") from e


def get_funnel_metrics() -> dict | None:
    """
    Calculate funnel metrics for all time, last day, and last month.

    Time windows are based on data timestamps, not current time.
    Sessions are attributed to a window based on when they started.

    Returns:
        dict with metrics for each time window, or None if no data/connection error.
        Structure:
        {
            "all_time": { sessions, cart_sessions, purchase_sessions, total_purchases, ... },
            "last_day": { ... },
            "last_month": { ... },
        }
    """
    try:
        settings = get_settings()
        schema_name = settings.postgres.schema_name

        with psycopg2.connect(settings.postgres.connection_string, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                # Set search path
                cur.execute(f"SET search_path TO {schema_name}, public")

                # Get max timestamp to determine time window boundaries
                cur.execute("SELECT MAX(session_start) FROM sessions")
                result = cur.fetchone()
                if not result or result[0] is None:
                    return None

                max_ts = result[0]

                # Calculate metrics for all three windows in a single query
                cur.execute(
                    """
                    SELECT
                        -- All time
                        COUNT(*) as sessions_all,
                        COUNT(*) FILTER (WHERE cart_count > 0) as cart_all,
                        COUNT(*) FILTER (WHERE transaction_count > 0) as purchase_all,
                        COALESCE(SUM(transaction_count), 0) as items_all,

                        -- Last day (sessions started within 24h of max timestamp)
                        COUNT(*) FILTER (WHERE session_start > %s - interval '1 day') as sessions_day,
                        COUNT(*) FILTER (WHERE session_start > %s - interval '1 day' AND cart_count > 0) as cart_day,
                        COUNT(*) FILTER (WHERE session_start > %s - interval '1 day' AND transaction_count > 0) as purchase_day,
                        COALESCE(SUM(transaction_count) FILTER (WHERE session_start > %s - interval '1 day'), 0) as items_day,

                        -- Last month (sessions started within 30 days of max timestamp)
                        COUNT(*) FILTER (WHERE session_start > %s - interval '30 days') as sessions_month,
                        COUNT(*) FILTER (WHERE session_start > %s - interval '30 days' AND cart_count > 0) as cart_month,
                        COUNT(*) FILTER (WHERE session_start > %s - interval '30 days' AND transaction_count > 0) as purchase_month,
                        COALESCE(SUM(transaction_count) FILTER (WHERE session_start > %s - interval '30 days'), 0) as items_month
                    FROM sessions
                    """,
                    (max_ts, max_ts, max_ts, max_ts, max_ts, max_ts, max_ts, max_ts),
                )

                row = cur.fetchone()
                if not row:
                    return None

                (
                    sessions_all,
                    cart_all,
                    purchase_all,
                    items_all,
                    sessions_day,
                    cart_day,
                    purchase_day,
                    items_day,
                    sessions_month,
                    cart_month,
                    purchase_month,
                    items_month,
                ) = row

                def build_metrics(sessions: int, cart: int, purchase: int, items: int) -> dict:
                    """Build metrics dict for a time window."""
                    conversion_rate = (purchase / sessions * 100) if sessions > 0 else 0.0
                    cart_abandonment = ((cart - purchase) / cart * 100) if cart > 0 else 0.0
                    avg_items = (items / purchase) if purchase > 0 else 0.0

                    return {
                        "sessions": sessions,
                        "cart_sessions": cart,
                        "purchase_sessions": purchase,
                        "total_purchases": int(items),
                        "conversion_rate": conversion_rate,
                        "cart_abandonment": cart_abandonment,
                        "avg_items_per_purchase": avg_items,
                    }

                return {
                    "all_time": build_metrics(sessions_all, cart_all, purchase_all, items_all),
                    "last_day": build_metrics(sessions_day, cart_day, purchase_day, items_day),
                    "last_month": build_metrics(
                        sessions_month, cart_month, purchase_month, items_month
                    ),
                }

    except Exception as e:
        logger.error("Failed to get funnel metrics: %s", e)
        return None
