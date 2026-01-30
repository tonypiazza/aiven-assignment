# ==============================================================================
# SECURITY GROUP FOR PRIVATELINK ENDPOINTS
# ==============================================================================

resource "aws_security_group" "privatelink" {
  count = var.privatelink_enabled ? 1 : 0

  name        = "clickstream-privatelink-sg"
  description = "Security group for Aiven PrivateLink VPC endpoints"
  vpc_id      = aws_vpc.benchmark.id

  ingress {
    description = "Kafka brokers"
    from_port   = 10000
    to_port     = 31000
    protocol    = "tcp"
    cidr_blocks = [var.aws_vpc_cidr]
  }

  ingress {
    description = "PostgreSQL"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.aws_vpc_cidr]
  }

  ingress {
    description = "Valkey"
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = [var.aws_vpc_cidr]
  }

  ingress {
    description = "OpenSearch"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.aws_vpc_cidr]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "clickstream-privatelink-sg"
  }
}

# ==============================================================================
# VPC ENDPOINTS FOR AIVEN PRIVATELINK
# ==============================================================================

resource "aws_vpc_endpoint" "kafka" {
  count = var.privatelink_enabled && var.aiven_kafka_privatelink_service_name != "" ? 1 : 0

  vpc_id              = aws_vpc.benchmark.id
  service_name        = var.aiven_kafka_privatelink_service_name
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.privatelink[0].id]
  private_dns_enabled = false

  tags = {
    Name = "clickstream-kafka-endpoint"
  }
}

resource "aws_vpc_endpoint" "pg" {
  count = var.privatelink_enabled && var.aiven_pg_privatelink_service_name != "" ? 1 : 0

  vpc_id              = aws_vpc.benchmark.id
  service_name        = var.aiven_pg_privatelink_service_name
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.privatelink[0].id]
  private_dns_enabled = false

  tags = {
    Name = "clickstream-pg-endpoint"
  }
}

resource "aws_vpc_endpoint" "valkey" {
  count = var.privatelink_enabled && var.aiven_valkey_privatelink_service_name != "" ? 1 : 0

  vpc_id              = aws_vpc.benchmark.id
  service_name        = var.aiven_valkey_privatelink_service_name
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.privatelink[0].id]
  private_dns_enabled = false

  tags = {
    Name = "clickstream-valkey-endpoint"
  }
}

resource "aws_vpc_endpoint" "opensearch" {
  count = var.privatelink_enabled && var.aiven_opensearch_privatelink_service_name != "" ? 1 : 0

  vpc_id              = aws_vpc.benchmark.id
  service_name        = var.aiven_opensearch_privatelink_service_name
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.privatelink[0].id]
  private_dns_enabled = false

  tags = {
    Name = "clickstream-opensearch-endpoint"
  }
}
