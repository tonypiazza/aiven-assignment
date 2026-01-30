# ==============================================================================
# VPC OUTPUTS
# ==============================================================================

output "vpc_id" {
  description = "AWS VPC ID"
  value       = aws_vpc.benchmark.id
}

output "public_subnet_id" {
  description = "Public subnet ID"
  value       = aws_subnet.public.id
}

output "private_subnet_id" {
  description = "Private subnet ID"
  value       = aws_subnet.private.id
}

# ==============================================================================
# EC2 OUTPUTS
# ==============================================================================

output "ec2_instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.benchmark.id
}

output "ec2_public_ip" {
  description = "Public IP address of the benchmark EC2 instance"
  value       = aws_instance.benchmark.public_ip
}

output "ec2_public_dns" {
  description = "Public DNS name of the benchmark EC2 instance"
  value       = aws_instance.benchmark.public_dns
}

output "ssh_command" {
  description = "SSH command to connect to EC2"
  value       = "ssh -i ~/.ssh/${var.key_pair_name}.pem ec2-user@${aws_instance.benchmark.public_ip}"
}

# ==============================================================================
# PRIVATELINK ENDPOINT OUTPUTS
# ==============================================================================

output "kafka_endpoint_dns" {
  description = "DNS name for Kafka PrivateLink endpoint"
  value       = var.privatelink_enabled && length(aws_vpc_endpoint.kafka) > 0 ? aws_vpc_endpoint.kafka[0].dns_entry[0].dns_name : null
}

output "pg_endpoint_dns" {
  description = "DNS name for PostgreSQL PrivateLink endpoint"
  value       = var.privatelink_enabled && length(aws_vpc_endpoint.pg) > 0 ? aws_vpc_endpoint.pg[0].dns_entry[0].dns_name : null
}

output "valkey_endpoint_dns" {
  description = "DNS name for Valkey PrivateLink endpoint"
  value       = var.privatelink_enabled && length(aws_vpc_endpoint.valkey) > 0 ? aws_vpc_endpoint.valkey[0].dns_entry[0].dns_name : null
}

output "opensearch_endpoint_dns" {
  description = "DNS name for OpenSearch PrivateLink endpoint"
  value       = var.privatelink_enabled && length(aws_vpc_endpoint.opensearch) > 0 ? aws_vpc_endpoint.opensearch[0].dns_entry[0].dns_name : null
}
