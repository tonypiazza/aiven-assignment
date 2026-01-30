# ==============================================================================
# DEPLOYMENT INFO
# ==============================================================================

output "deployment_scenario" {
  description = "Current deployment scenario"
  value       = var.deployment_scenario
}

# ==============================================================================
# AIVEN PROJECT OUTPUTS
# ==============================================================================

output "project_name" {
  description = "Aiven project name"
  value       = module.aiven.project_name
}

output "project_ca_cert" {
  description = "Project CA certificate for service connections"
  value       = module.aiven.project_ca_cert
  sensitive   = true
}

# ==============================================================================
# AIVEN KAFKA OUTPUTS
# ==============================================================================

output "kafka_service_uri" {
  description = "Kafka service URI"
  value       = module.aiven.kafka_service_uri
  sensitive   = true
}

output "kafka_bootstrap_servers" {
  description = "Kafka bootstrap servers"
  value       = module.aiven.kafka_bootstrap_servers
  sensitive   = true
}

output "kafka_access_cert" {
  description = "Kafka client certificate for mTLS authentication"
  value       = module.aiven.kafka_access_cert
  sensitive   = true
}

output "kafka_access_key" {
  description = "Kafka client private key for mTLS authentication"
  value       = module.aiven.kafka_access_key
  sensitive   = true
}

# ==============================================================================
# AIVEN POSTGRESQL OUTPUTS
# ==============================================================================

output "pg_service_uri" {
  description = "PostgreSQL service URI"
  value       = module.aiven.pg_service_uri
  sensitive   = true
}

output "pg_host" {
  description = "PostgreSQL host"
  value       = module.aiven.pg_host
}

output "pg_port" {
  description = "PostgreSQL port"
  value       = module.aiven.pg_port
}

output "pg_user" {
  description = "PostgreSQL username"
  value       = module.aiven.pg_user
  sensitive   = true
}

output "pg_password" {
  description = "PostgreSQL password"
  value       = module.aiven.pg_password
  sensitive   = true
}

output "pg_database" {
  description = "PostgreSQL database name"
  value       = module.aiven.pg_database
}

# ==============================================================================
# AIVEN VALKEY OUTPUTS
# ==============================================================================

output "valkey_service_uri" {
  description = "Valkey service URI"
  value       = module.aiven.valkey_service_uri
  sensitive   = true
}

output "valkey_host" {
  description = "Valkey host"
  value       = module.aiven.valkey_host
}

output "valkey_port" {
  description = "Valkey port"
  value       = module.aiven.valkey_port
}

output "valkey_password" {
  description = "Valkey password"
  value       = module.aiven.valkey_password
  sensitive   = true
}

# ==============================================================================
# AIVEN OPENSEARCH OUTPUTS (conditional)
# ==============================================================================

output "opensearch_service_uri" {
  description = "OpenSearch service URI"
  value       = module.aiven.opensearch_service_uri
  sensitive   = true
}

output "opensearch_host" {
  description = "OpenSearch host"
  value       = module.aiven.opensearch_host
}

output "opensearch_port" {
  description = "OpenSearch port"
  value       = module.aiven.opensearch_port
}

output "opensearch_user" {
  description = "OpenSearch username"
  value       = module.aiven.opensearch_user
  sensitive   = true
}

output "opensearch_password" {
  description = "OpenSearch password"
  value       = module.aiven.opensearch_password
  sensitive   = true
}

output "opensearch_dashboards_uri" {
  description = "OpenSearch Dashboards URL"
  value       = module.aiven.opensearch_dashboards_uri
}

# ==============================================================================
# AIVEN VPC / PRIVATELINK OUTPUTS
# ==============================================================================

output "aiven_vpc_id" {
  description = "Aiven Project VPC ID"
  value       = module.aiven.vpc_id
}

output "aiven_vpc_state" {
  description = "Aiven Project VPC state"
  value       = module.aiven.vpc_state
}

output "kafka_privatelink_service_name" {
  description = "Kafka PrivateLink AWS service name"
  value       = module.aiven.kafka_privatelink_service_name
}

output "pg_privatelink_service_name" {
  description = "PostgreSQL PrivateLink AWS service name"
  value       = module.aiven.pg_privatelink_service_name
}

output "valkey_privatelink_service_name" {
  description = "Valkey PrivateLink AWS service name"
  value       = module.aiven.valkey_privatelink_service_name
}

output "opensearch_privatelink_service_name" {
  description = "OpenSearch PrivateLink AWS service name"
  value       = module.aiven.opensearch_privatelink_service_name
}

# ==============================================================================
# AWS OUTPUTS (conditional)
# ==============================================================================

output "aws_vpc_id" {
  description = "AWS VPC ID"
  value       = local.deploy_aws ? module.aws[0].vpc_id : null
}

output "ec2_instance_id" {
  description = "EC2 instance ID"
  value       = local.deploy_aws ? module.aws[0].ec2_instance_id : null
}

output "ec2_public_ip" {
  description = "Public IP address of the benchmark EC2 instance"
  value       = local.deploy_aws ? module.aws[0].ec2_public_ip : null
}

output "ec2_public_dns" {
  description = "Public DNS name of the benchmark EC2 instance"
  value       = local.deploy_aws ? module.aws[0].ec2_public_dns : null
}

output "ssh_command" {
  description = "SSH command to connect to EC2"
  value       = local.deploy_aws ? module.aws[0].ssh_command : null
}

# ==============================================================================
# AWS PRIVATELINK ENDPOINT OUTPUTS (conditional)
# ==============================================================================

output "kafka_endpoint_dns" {
  description = "DNS name for Kafka PrivateLink endpoint"
  value       = local.enable_privatelink ? module.aws[0].kafka_endpoint_dns : null
}

output "pg_endpoint_dns" {
  description = "DNS name for PostgreSQL PrivateLink endpoint"
  value       = local.enable_privatelink ? module.aws[0].pg_endpoint_dns : null
}

output "valkey_endpoint_dns" {
  description = "DNS name for Valkey PrivateLink endpoint"
  value       = local.enable_privatelink ? module.aws[0].valkey_endpoint_dns : null
}

output "opensearch_endpoint_dns" {
  description = "DNS name for OpenSearch PrivateLink endpoint"
  value       = local.enable_privatelink ? module.aws[0].opensearch_endpoint_dns : null
}
