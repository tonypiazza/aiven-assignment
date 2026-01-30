# ==============================================================================
# AWS REGION
# ==============================================================================

variable "aws_region" {
  description = "AWS region for benchmark infrastructure"
  type        = string
  default     = "us-east-2"
}

# ==============================================================================
# VPC CONFIGURATION
# ==============================================================================

variable "aws_vpc_cidr" {
  description = "CIDR block for AWS VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidr" {
  description = "CIDR block for public subnet (EC2)"
  type        = string
  default     = "10.0.1.0/24"
}

variable "private_subnet_cidr" {
  description = "CIDR block for private subnet (VPC Endpoints)"
  type        = string
  default     = "10.0.2.0/24"
}

# ==============================================================================
# EC2 CONFIGURATION
# ==============================================================================

variable "instance_type" {
  description = "EC2 instance type for benchmark"
  type        = string
  default     = "c6i.2xlarge"
}

variable "key_pair_name" {
  description = "Name of existing EC2 key pair for SSH access"
  type        = string
}

variable "ssh_allowed_cidr" {
  description = "CIDR block allowed to SSH to EC2 (e.g., your IP/32 or 0.0.0.0/0)"
  type        = string
  default     = "0.0.0.0/0"
}

variable "github_repo_url" {
  description = "GitHub repository URL to clone on EC2"
  type        = string
  default     = "https://github.com/tonypiazza/aiven-clickstream-demo.git"
}

# ==============================================================================
# PRIVATELINK CONFIGURATION
# ==============================================================================

variable "privatelink_enabled" {
  description = "Enable AWS VPC Endpoints for Aiven PrivateLink"
  type        = bool
  default     = false
}

variable "aiven_kafka_privatelink_service_name" {
  description = "Aiven Kafka PrivateLink AWS service name"
  type        = string
  default     = ""
}

variable "aiven_pg_privatelink_service_name" {
  description = "Aiven PostgreSQL PrivateLink AWS service name"
  type        = string
  default     = ""
}

variable "aiven_valkey_privatelink_service_name" {
  description = "Aiven Valkey PrivateLink AWS service name"
  type        = string
  default     = ""
}

variable "aiven_opensearch_privatelink_service_name" {
  description = "Aiven OpenSearch PrivateLink AWS service name"
  type        = string
  default     = ""
}
