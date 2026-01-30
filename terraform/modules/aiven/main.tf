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

resource "aiven_kafka" "this" {
  project                 = aiven_project.this.project
  cloud_name              = var.cloud_name
  plan                    = var.kafka_plan
  service_name            = "${var.project_name}-kafka"
  maintenance_window_dow  = var.maintenance_window_dow
  maintenance_window_time = var.maintenance_window_time

  project_vpc_id = var.privatelink_enabled ? aiven_project_vpc.this[0].id : null

  kafka_user_config {
    kafka_version = var.kafka_version

    dynamic "privatelink_access" {
      for_each = var.privatelink_enabled ? [1] : []
      content {
        kafka           = true
        kafka_connect   = true
        kafka_rest      = true
        schema_registry = true
      }
    }
  }
}

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

resource "aiven_pg" "this" {
  project                 = aiven_project.this.project
  cloud_name              = var.cloud_name
  plan                    = var.pg_plan
  service_name            = "${var.project_name}-pg"
  maintenance_window_dow  = var.maintenance_window_dow
  maintenance_window_time = var.maintenance_window_time

  project_vpc_id = var.privatelink_enabled ? aiven_project_vpc.this[0].id : null

  pg_user_config {
    pg_version = var.pg_version

    dynamic "privatelink_access" {
      for_each = var.privatelink_enabled ? [1] : []
      content {
        pg        = true
        pgbouncer = true
      }
    }
  }
}

resource "aiven_pg_database" "this" {
  project       = aiven_project.this.project
  service_name  = aiven_pg.this.service_name
  database_name = var.pg_database_name
}

# ==============================================================================
# VALKEY SERVICE
# ==============================================================================

resource "aiven_valkey" "this" {
  project                 = aiven_project.this.project
  cloud_name              = var.cloud_name
  plan                    = var.valkey_plan
  service_name            = "${var.project_name}-valkey"
  maintenance_window_dow  = var.maintenance_window_dow
  maintenance_window_time = var.maintenance_window_time

  project_vpc_id = var.privatelink_enabled ? aiven_project_vpc.this[0].id : null

  dynamic "valkey_user_config" {
    for_each = var.privatelink_enabled ? [1] : []
    content {
      privatelink_access {
        valkey = true
      }
    }
  }
}

# ==============================================================================
# OPENSEARCH SERVICE (Optional)
# ==============================================================================

resource "aiven_opensearch" "this" {
  count = var.opensearch_enabled ? 1 : 0

  project                 = aiven_project.this.project
  cloud_name              = var.cloud_name
  plan                    = var.opensearch_plan
  service_name            = "${var.project_name}-opensearch"
  maintenance_window_dow  = var.maintenance_window_dow
  maintenance_window_time = var.maintenance_window_time

  project_vpc_id = var.privatelink_enabled ? aiven_project_vpc.this[0].id : null

  opensearch_user_config {
    opensearch_dashboards {
      enabled            = true
      max_old_space_size = 512
    }

    dynamic "privatelink_access" {
      for_each = var.privatelink_enabled ? [1] : []
      content {
        opensearch            = true
        opensearch_dashboards = true
      }
    }
  }
}
