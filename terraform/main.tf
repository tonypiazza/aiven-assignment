# ==============================================================================
# LOCAL VARIABLES
# ==============================================================================

locals {
  # Derive flags from deployment_scenario
  deploy_aws         = var.deployment_scenario != "local"
  enable_privatelink = var.deployment_scenario == "ec2_privatelink"
}

# ==============================================================================
# AIVEN MODULE
# ==============================================================================

module "aiven" {
  source = "./modules/aiven"

  # Authentication
  aiven_api_token = var.aiven_api_token

  # Organization
  organization_name = var.organization_name
  billing_group_id  = var.billing_group_id

  # Project
  project_name = var.project_name
  cloud_name   = var.cloud_name

  # Service plans
  kafka_plan         = var.kafka_plan
  pg_plan            = var.pg_plan
  valkey_plan        = var.valkey_plan
  opensearch_plan    = var.opensearch_plan
  opensearch_enabled = var.opensearch_enabled

  # Service versions
  kafka_version = var.kafka_version
  pg_version    = var.pg_version

  # Kafka topic settings
  kafka_events_topic_name         = var.kafka_events_topic_name
  kafka_events_topic_partitions   = var.kafka_events_topic_partitions
  kafka_events_topic_replication  = var.kafka_events_topic_replication
  kafka_events_topic_retention_ms = var.kafka_events_topic_retention_ms

  # PostgreSQL settings
  pg_database_name = var.pg_database_name

  # Maintenance window
  maintenance_window_dow  = var.maintenance_window_dow
  maintenance_window_time = var.maintenance_window_time

  # PrivateLink configuration
  privatelink_enabled = local.enable_privatelink
  aiven_vpc_cidr      = var.aiven_vpc_cidr
  aws_account_id      = var.aws_account_id
}

# ==============================================================================
# AWS MODULE
# ==============================================================================

module "aws" {
  source = "./modules/aws"
  count  = local.deploy_aws ? 1 : 0

  # AWS configuration
  aws_region          = var.aws_region
  aws_vpc_cidr        = var.aws_vpc_cidr
  public_subnet_cidr  = var.public_subnet_cidr
  private_subnet_cidr = var.private_subnet_cidr

  # EC2 configuration
  instance_type    = var.instance_type
  key_pair_name    = var.key_pair_name
  ssh_allowed_cidr = var.ssh_allowed_cidr
  github_repo_url  = var.github_repo_url

  # PrivateLink configuration
  privatelink_enabled = local.enable_privatelink

  aiven_kafka_privatelink_service_name      = local.enable_privatelink ? module.aiven.kafka_privatelink_service_name : ""
  aiven_pg_privatelink_service_name         = local.enable_privatelink ? module.aiven.pg_privatelink_service_name : ""
  aiven_valkey_privatelink_service_name     = local.enable_privatelink ? module.aiven.valkey_privatelink_service_name : ""
  aiven_opensearch_privatelink_service_name = local.enable_privatelink ? module.aiven.opensearch_privatelink_service_name : ""
}
