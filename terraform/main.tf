# ==============================================================================
# DATA SOURCES
# ==============================================================================

data "aiven_organization" "this" {
  name = var.organization_name
}

data "aiven_billing_group" "this" {
  billing_group_id = var.billing_group_id
}

# ==============================================================================
# PROJECT
# ==============================================================================

resource "aiven_project" "this" {
  project       = var.project_name
  parent_id     = data.aiven_organization.this.id
  billing_group = data.aiven_billing_group.this.id
}

# ==============================================================================
# KAFKA SERVICE
# ==============================================================================
#
# Kafka handles event streaming from producer to consumer.
# Events are keyed by visitor_id for partition affinity.
#

resource "aiven_kafka" "this" {
  project                 = aiven_project.this.project
  cloud_name              = var.cloud_name
  plan                    = var.kafka_plan
  service_name            = "${var.project_name}-kafka"
  maintenance_window_dow  = var.maintenance_window_dow
  maintenance_window_time = var.maintenance_window_time

  kafka_user_config {
    kafka_version = var.kafka_version
  }
}

# ==============================================================================
# KAFKA TOPIC - Events
# ==============================================================================
#
# The events topic receives raw clickstream events from the producer.
# Partition count should match KAFKA_EVENTS_TOPIC_PARTITIONS in .env
# to enable parallel consumer processing.
#

resource "aiven_kafka_topic" "this" {
  project      = aiven_project.this.project
  service_name = aiven_kafka.this.service_name
  topic_name   = var.kafka_events_topic_name
  partitions   = var.kafka_events_topic_partitions
  replication  = var.kafka_events_topic_replication

  config {
    cleanup_policy = "delete"
    retention_ms   = var.kafka_events_topic_retention_ms
  }
}

# ==============================================================================
# POSTGRESQL SERVICE
# ==============================================================================
#
# PostgreSQL stores processed sessions and supports analytics queries.
# Schema is auto-initialized by 'clickstream consumer start'.
#

resource "aiven_pg" "this" {
  project                 = aiven_project.this.project
  cloud_name              = var.cloud_name
  plan                    = var.pg_plan
  service_name            = "${var.project_name}-pg"
  maintenance_window_dow  = var.maintenance_window_dow
  maintenance_window_time = var.maintenance_window_time

  pg_user_config {
    pg_version = var.pg_version
  }
}

resource "aiven_pg_database" "this" {
  project       = aiven_project.this.project
  service_name  = aiven_pg.this.service_name
  database_name = var.pg_database_name
}

# ==============================================================================
# OPENSEARCH SERVICE (Optional)
# ==============================================================================
#
# OpenSearch provides real-time dashboards for clickstream analytics.
# Index is auto-initialized by 'clickstream consumer start' when enabled.
#

resource "aiven_opensearch" "this" {
  count = var.opensearch_enabled ? 1 : 0

  project                 = aiven_project.this.project
  cloud_name              = var.cloud_name
  plan                    = var.opensearch_plan
  service_name            = "${var.project_name}-opensearch"
  maintenance_window_dow  = var.maintenance_window_dow
  maintenance_window_time = var.maintenance_window_time

  opensearch_user_config {
    opensearch_dashboards {
      enabled            = true
      max_old_space_size = 512
    }
  }
}

# ==============================================================================
# VALKEY SERVICE (Redis-compatible)
# ==============================================================================
#
# Valkey caches in-progress session state for the consumer.
# Sessions are tracked in memory until they timeout (default: 30 min),
# then flushed to PostgreSQL.
#

resource "aiven_valkey" "this" {
  project                 = aiven_project.this.project
  cloud_name              = var.cloud_name
  plan                    = var.valkey_plan
  service_name            = "${var.project_name}-valkey"
  maintenance_window_dow  = var.maintenance_window_dow
  maintenance_window_time = var.maintenance_window_time
}
