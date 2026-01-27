# ==============================================================================
# AUTHENTICATION
# ==============================================================================

variable "aiven_api_token" {
  description = "Aiven API token"
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.aiven_api_token) > 0
    error_message = "Aiven API token must not be empty."
  }
}

# ==============================================================================
# ORGANIZATION
# ==============================================================================

variable "organization_name" {
  description = "Aiven organization name"
  type        = string

  validation {
    condition     = length(var.organization_name) > 0
    error_message = "Organization name must not be empty."
  }
}

variable "billing_group_id" {
  description = "Aiven billing group ID"
  type        = string

  validation {
    condition     = length(var.billing_group_id) > 0
    error_message = "Billing group ID must not be empty."
  }
}

# ==============================================================================
# PROJECT
# ==============================================================================

variable "project_name" {
  description = "Aiven project name"
  type        = string
  default     = "clickstream-demo"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,62}$", var.project_name))
    error_message = "Project name must start with a lowercase letter, contain only lowercase letters, numbers, and hyphens, and be 1-63 characters long."
  }
}

variable "cloud_name" {
  description = "Cloud provider and region"
  type        = string
  default     = "aws-us-east-2"

  validation {
    condition     = can(regex("^(aws|azure|do|google|upcloud)-[a-z0-9-]+$", var.cloud_name))
    error_message = "Cloud name must be in format '<provider>-<region>' (e.g., 'aws-us-east-2', 'google-us-central1')."
  }
}

# ==============================================================================
# SERVICE PLANS
# ==============================================================================

variable "kafka_plan" {
  description = "Kafka service plan"
  type        = string
  default     = "startup-2"

  validation {
    condition     = can(regex("^(hobbyist|(startup|business|premium)-[0-9]+)$", var.kafka_plan))
    error_message = "Kafka plan must be 'hobbyist' or in format '<tier>-<size>' (e.g., 'startup-2', 'business-4', 'premium-8')."
  }
}

variable "pg_plan" {
  description = "PostgreSQL service plan"
  type        = string
  default     = "startup-4"

  validation {
    condition     = can(regex("^(hobbyist|(startup|business|premium)-[0-9]+)$", var.pg_plan))
    error_message = "PostgreSQL plan must be 'hobbyist' or in format '<tier>-<size>' (e.g., 'startup-4', 'business-8', 'premium-16')."
  }
}

variable "opensearch_plan" {
  description = "OpenSearch service plan"
  type        = string
  default     = "startup-4"

  validation {
    condition     = can(regex("^(hobbyist|(startup|business|premium)-[0-9]+)$", var.opensearch_plan))
    error_message = "OpenSearch plan must be 'hobbyist' or in format '<tier>-<size>' (e.g., 'startup-4', 'business-8', 'premium-16')."
  }
}

variable "valkey_plan" {
  description = "Valkey (Redis-compatible) service plan"
  type        = string
  default     = "startup-4"

  validation {
    condition     = can(regex("^(hobbyist|(startup|business|premium)-[0-9]+)$", var.valkey_plan))
    error_message = "Valkey plan must be 'hobbyist' or in format '<tier>-<size>' (e.g., 'startup-4', 'business-8', 'premium-16')."
  }
}

# ==============================================================================
# SERVICE VERSIONS
# ==============================================================================

variable "kafka_version" {
  description = "Kafka version"
  type        = string
  default     = "3.8"

  validation {
    condition     = can(regex("^[0-9]+\\.[0-9]+$", var.kafka_version))
    error_message = "Kafka version must be in format 'X.Y' (e.g., '3.8', '3.7')."
  }
}

variable "pg_version" {
  description = "PostgreSQL version"
  type        = string
  default     = "17"

  validation {
    condition     = can(regex("^[0-9]+$", var.pg_version))
    error_message = "PostgreSQL version must be a number (e.g., '17', '16', '15')."
  }
}

# ==============================================================================
# MAINTENANCE WINDOW
# ==============================================================================

variable "maintenance_window_dow" {
  description = "Day of week for maintenance window"
  type        = string
  default     = "sunday"

  validation {
    condition     = contains(["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"], var.maintenance_window_dow)
    error_message = "Maintenance window day must be one of: monday, tuesday, wednesday, thursday, friday, saturday, sunday."
  }
}

variable "maintenance_window_time" {
  description = "Time for maintenance window (UTC)"
  type        = string
  default     = "04:00:00"

  validation {
    condition     = can(regex("^([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$", var.maintenance_window_time))
    error_message = "Maintenance window time must be in format 'HH:MM:SS' (e.g., '04:00:00', '23:30:00')."
  }
}

# ==============================================================================
# SERVICE CONFIGURATION
# ==============================================================================

variable "opensearch_enabled" {
  description = "Enable OpenSearch service (optional, for dashboards)"
  type        = bool
  default     = false
}

variable "termination_protection" {
  description = "Enable termination protection for services"
  type        = bool
  default     = false
}

# ==============================================================================
# KAFKA TOPIC SETTINGS
# Must match corresponding values in .env file
# ==============================================================================

# Must match KAFKA_EVENTS_TOPIC in .env
variable "kafka_events_topic_name" {
  description = "Kafka events topic name"
  type        = string
  default     = "clickstream-events"

  validation {
    condition     = can(regex("^[a-zA-Z0-9._-]+$", var.kafka_events_topic_name)) && length(var.kafka_events_topic_name) <= 249
    error_message = "Kafka topic name must contain only alphanumeric characters, dots, underscores, and hyphens, and be at most 249 characters."
  }
}

# Must match KAFKA_EVENTS_TOPIC_PARTITIONS in .env
variable "kafka_events_topic_partitions" {
  description = "Kafka events topic partitions"
  type        = number
  default     = 3

  validation {
    condition     = var.kafka_events_topic_partitions >= 1 && var.kafka_events_topic_partitions <= 1000
    error_message = "Kafka topic partitions must be between 1 and 1000."
  }
}

# Max depends on plan's broker count
variable "kafka_events_topic_replication" {
  description = "Kafka events topic replication factor"
  type        = number
  default     = 2

  validation {
    condition     = var.kafka_events_topic_replication >= 1 && var.kafka_events_topic_replication <= 10
    error_message = "Kafka replication factor must be between 1 and 10."
  }
}

variable "kafka_events_topic_retention_ms" {
  description = "Kafka events topic retention in milliseconds"
  type        = string
  default     = "604800000" # 7 days

  validation {
    condition     = can(regex("^[0-9]+$", var.kafka_events_topic_retention_ms)) && tonumber(var.kafka_events_topic_retention_ms) >= 0
    error_message = "Kafka retention must be a non-negative number in milliseconds."
  }
}

# ==============================================================================
# POSTGRESQL SETTINGS
# Must match corresponding values in .env file
# ==============================================================================

# Must match PG_DATABASE in .env
variable "pg_database_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "clickstream"

  validation {
    condition     = can(regex("^[a-z_][a-z0-9_]*$", var.pg_database_name)) && length(var.pg_database_name) <= 63
    error_message = "PostgreSQL database name must start with a letter or underscore, contain only lowercase letters, numbers, and underscores, and be at most 63 characters."
  }
}
