# ==============================================================================
# PROJECT OUTPUTS
# ==============================================================================

output "project_name" {
  description = "Aiven project name"
  value       = aiven_project.this.project
}

output "project_ca_cert" {
  description = "Project CA certificate for service connections"
  value       = aiven_project.this.ca_cert
  sensitive   = true
}

# ==============================================================================
# KAFKA OUTPUTS
# ==============================================================================

output "kafka_service_uri" {
  description = "Kafka service URI"
  value       = aiven_kafka.this.service_uri
  sensitive   = true
}

output "kafka_bootstrap_servers" {
  description = "Kafka bootstrap servers (comma-separated)"
  value       = join(",", aiven_kafka.this.kafka[0].uris)
  sensitive   = true
}

output "kafka_access_cert" {
  description = "Kafka client certificate for mTLS authentication"
  value       = aiven_kafka.this.kafka[0].access_cert
  sensitive   = true
}

output "kafka_access_key" {
  description = "Kafka client private key for mTLS authentication"
  value       = aiven_kafka.this.kafka[0].access_key
  sensitive   = true
}

# ==============================================================================
# POSTGRESQL OUTPUTS
# ==============================================================================

output "pg_service_uri" {
  description = "PostgreSQL service URI"
  value       = aiven_pg.this.service_uri
  sensitive   = true
}

output "pg_host" {
  description = "PostgreSQL host"
  value       = aiven_pg.this.service_host
}

output "pg_port" {
  description = "PostgreSQL port"
  value       = aiven_pg.this.service_port
}

output "pg_user" {
  description = "PostgreSQL username"
  value       = aiven_pg.this.service_username
  sensitive   = true
}

output "pg_password" {
  description = "PostgreSQL password"
  value       = aiven_pg.this.service_password
  sensitive   = true
}

output "pg_database" {
  description = "PostgreSQL database name"
  value       = aiven_pg_database.this.database_name
}

# ==============================================================================
# OPENSEARCH OUTPUTS (conditional)
# ==============================================================================

output "opensearch_service_uri" {
  description = "OpenSearch service URI"
  value       = var.opensearch_enabled ? aiven_opensearch.this[0].service_uri : null
  sensitive   = true
}

output "opensearch_host" {
  description = "OpenSearch host"
  value       = var.opensearch_enabled ? aiven_opensearch.this[0].service_host : null
}

output "opensearch_port" {
  description = "OpenSearch port"
  value       = var.opensearch_enabled ? aiven_opensearch.this[0].service_port : null
}

output "opensearch_user" {
  description = "OpenSearch username"
  value       = var.opensearch_enabled ? aiven_opensearch.this[0].service_username : null
  sensitive   = true
}

output "opensearch_password" {
  description = "OpenSearch password"
  value       = var.opensearch_enabled ? aiven_opensearch.this[0].service_password : null
  sensitive   = true
}

output "opensearch_dashboards_uri" {
  description = "OpenSearch Dashboards URL"
  value       = var.opensearch_enabled ? "https://${aiven_opensearch.this[0].service_host}/app/login" : null
}

# ==============================================================================
# VALKEY OUTPUTS
# ==============================================================================

output "valkey_service_uri" {
  description = "Valkey service URI"
  value       = aiven_valkey.this.service_uri
  sensitive   = true
}

output "valkey_host" {
  description = "Valkey host"
  value       = aiven_valkey.this.service_host
}

output "valkey_port" {
  description = "Valkey port"
  value       = aiven_valkey.this.service_port
}

output "valkey_password" {
  description = "Valkey password"
  value       = aiven_valkey.this.service_password
  sensitive   = true
}
