-- ==============================================================================
-- CLICKSTREAM DATABASE SCHEMA
-- ==============================================================================
-- This script initializes the clickstream database with tables for storing
-- raw events and aggregated sessions.
--
-- Template variables:
--   {{ schema_name }} - The target schema name (from PG_SCHEMA_NAME)
-- ==============================================================================

-- Create the schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS {{ schema_name }};

-- Set search path for this session
SET search_path TO {{ schema_name }}, public;

-- ==============================================================================
-- EXTENSIONS
-- ==============================================================================

-- Enable UUID generation if needed in the future
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ==============================================================================
-- ENUM TYPES
-- ==============================================================================

-- Event types matching the source data
-- Note: Check if type exists first to make script idempotent
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'event_type') THEN
        CREATE TYPE event_type AS ENUM ('view', 'addtocart', 'transaction');
    END IF;
END$$;

-- ==============================================================================
-- EVENTS TABLE
-- ==============================================================================

-- Stores raw clickstream events from Kafka
CREATE TABLE IF NOT EXISTS events (
    id              BIGSERIAL PRIMARY KEY,
    event_time      TIMESTAMPTZ NOT NULL,
    visitor_id      BIGINT NOT NULL,
    event           event_type NOT NULL,
    item_id         BIGINT NOT NULL,
    transaction_id  BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for querying events by visitor
CREATE INDEX IF NOT EXISTS idx_events_visitor_id ON events (visitor_id);

-- Index for querying events by time range
CREATE INDEX IF NOT EXISTS idx_events_event_time ON events (event_time);

-- Index for querying events by type
CREATE INDEX IF NOT EXISTS idx_events_event ON events (event);

-- Composite index for session tracking queries
CREATE INDEX IF NOT EXISTS idx_events_visitor_time ON events (visitor_id, event_time);

-- Unique index for deduplication (prevents duplicate events)
-- Events are uniquely identified by visitor, time, event type, and item
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_unique 
ON events (visitor_id, event_time, event, item_id);

-- ==============================================================================
-- SESSIONS TABLE
-- ==============================================================================

-- Stores aggregated user sessions (session timeout is configurable)
CREATE TABLE IF NOT EXISTS sessions (
    id                  BIGSERIAL PRIMARY KEY,
    session_id          VARCHAR(100) NOT NULL UNIQUE,
    visitor_id          BIGINT NOT NULL,
    session_start       TIMESTAMPTZ NOT NULL,
    session_end         TIMESTAMPTZ NOT NULL,
    duration_seconds    INTEGER NOT NULL,
    event_count         INTEGER NOT NULL,
    view_count          INTEGER NOT NULL DEFAULT 0,
    cart_count          INTEGER NOT NULL DEFAULT 0,
    transaction_count   INTEGER NOT NULL DEFAULT 0,
    items_viewed        BIGINT[] NOT NULL DEFAULT '{}',
    items_carted        BIGINT[] NOT NULL DEFAULT '{}',
    items_purchased     BIGINT[] NOT NULL DEFAULT '{}',
    converted           BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for querying sessions by visitor
CREATE INDEX IF NOT EXISTS idx_sessions_visitor_id ON sessions (visitor_id);

-- Index for querying sessions by time range
CREATE INDEX IF NOT EXISTS idx_sessions_start ON sessions (session_start);

-- Index for finding sessions that converted (had transactions)
CREATE INDEX IF NOT EXISTS idx_sessions_transactions ON sessions (transaction_count) 
    WHERE transaction_count > 0;

-- ==============================================================================
-- ITEM METRICS TABLE
-- ==============================================================================

-- Stores aggregated metrics per item (for analytics)
CREATE TABLE IF NOT EXISTS item_metrics (
    item_id             BIGINT PRIMARY KEY,
    view_count          BIGINT NOT NULL DEFAULT 0,
    cart_count          BIGINT NOT NULL DEFAULT 0,
    purchase_count      BIGINT NOT NULL DEFAULT 0,
    unique_visitors     BIGINT NOT NULL DEFAULT 0,
    first_seen          TIMESTAMPTZ NOT NULL,
    last_seen           TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==============================================================================
-- VISITOR METRICS TABLE
-- ==============================================================================

-- Stores aggregated metrics per visitor (for analytics)
CREATE TABLE IF NOT EXISTS visitor_metrics (
    visitor_id          BIGINT PRIMARY KEY,
    total_sessions      INTEGER NOT NULL DEFAULT 0,
    total_events        BIGINT NOT NULL DEFAULT 0,
    total_views         BIGINT NOT NULL DEFAULT 0,
    total_carts         BIGINT NOT NULL DEFAULT 0,
    total_purchases     BIGINT NOT NULL DEFAULT 0,
    unique_items_viewed BIGINT NOT NULL DEFAULT 0,
    first_seen          TIMESTAMPTZ NOT NULL,
    last_seen           TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==============================================================================
-- HELPER VIEWS
-- ==============================================================================

-- View for conversion funnel analysis
CREATE OR REPLACE VIEW conversion_funnel AS
SELECT 
    DATE_TRUNC('day', event_time) AS day,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    COUNT(DISTINCT CASE WHEN event = 'view' THEN visitor_id END) AS viewers,
    COUNT(DISTINCT CASE WHEN event = 'addtocart' THEN visitor_id END) AS carters,
    COUNT(DISTINCT CASE WHEN event = 'transaction' THEN visitor_id END) AS purchasers
FROM events
GROUP BY DATE_TRUNC('day', event_time)
ORDER BY day;

-- View for hourly traffic patterns
CREATE OR REPLACE VIEW hourly_traffic AS
SELECT 
    EXTRACT(HOUR FROM event_time) AS hour_of_day,
    COUNT(*) AS event_count,
    COUNT(DISTINCT visitor_id) AS unique_visitors
FROM events
GROUP BY EXTRACT(HOUR FROM event_time)
ORDER BY hour_of_day;
