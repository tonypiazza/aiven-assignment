# ==============================================================================
# AUTHENTICATION
# ==============================================================================

variable "aiven_api_token" {
  description = "Aiven API token"
  type        = string
  sensitive   = true
}

# ==============================================================================
# ORGANIZATION
# ==============================================================================

variable "organization_name" {
  description = "Aiven organization name"
  type        = string
}

variable "billing_group_id" {
  description = "Aiven billing group ID"
  type        = string
}

# ==============================================================================
# PROJECT
# ==============================================================================

variable "project_name" {
  description = "Aiven project name"
  type        = string
}

variable "cloud_name" {
  description = "Cloud provider and region (e.g., aws-us-east-2)"
  type        = string
}

# ==============================================================================
# SERVICE PLANS
# ==============================================================================

variable "kafka_plan" {
  description = "Kafka service plan"
  type        = string
  default     = "startup-4"
}

variable "pg_plan" {
  description = "PostgreSQL service plan"
  type        = string
  default     = "startup-4"
}

variable "valkey_plan" {
  description = "Valkey service plan"
  type        = string
  default     = "startup-4"
}

variable "opensearch_plan" {
  description = "OpenSearch service plan"
  type        = string
  default     = "startup-4"
}

variable "opensearch_enabled" {
  description = "Enable OpenSearch service"
  type        = bool
  default     = false
}

# ==============================================================================
# SERVICE VERSIONS
# ==============================================================================

variable "kafka_version" {
  description = "Kafka version"
  type        = string
  default     = "3.9"
}

variable "pg_version" {
  description = "PostgreSQL version"
  type        = string
  default     = "17"
}

# ==============================================================================
# KAFKA TOPIC SETTINGS
# ==============================================================================

variable "kafka_events_topic_name" {
  description = "Kafka events topic name"
  type        = string
  default     = "clickstream-events"
}

variable "kafka_events_topic_partitions" {
  description = "Kafka events topic partitions"
  type        = number
  default     = 3
}

variable "kafka_events_topic_replication" {
  description = "Kafka events topic replication factor"
  type        = number
  default     = 2
}

variable "kafka_events_topic_retention_ms" {
  description = "Kafka events topic retention in milliseconds"
  type        = string
  default     = "604800000"
}

# ==============================================================================
# POSTGRESQL SETTINGS
# ==============================================================================

variable "pg_database_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "clickstream"
}

# ==============================================================================
# MAINTENANCE WINDOW
# ==============================================================================

variable "maintenance_window_dow" {
  description = "Day of week for maintenance window"
  type        = string
  default     = "sunday"
}

variable "maintenance_window_time" {
  description = "Time for maintenance window (UTC)"
  type        = string
  default     = "04:00:00"
}

# ==============================================================================
# PRIVATELINK CONFIGURATION
# ==============================================================================

variable "privatelink_enabled" {
  description = "Enable AWS PrivateLink (deploys services in Aiven VPC)"
  type        = bool
  default     = false
}

variable "aiven_vpc_cidr" {
  description = "CIDR block for Aiven Project VPC"
  type        = string
  default     = "192.168.0.0/24"
}

variable "aws_account_id" {
  description = "AWS Account ID for PrivateLink principal ARN"
  type        = string
  default     = ""
}
