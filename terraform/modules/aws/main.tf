# ==============================================================================
# VPC
# ==============================================================================

resource "aws_vpc" "benchmark" {
  cidr_block           = var.aws_vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "clickstream-benchmark-vpc"
  }
}

# ==============================================================================
# SUBNETS
# ==============================================================================

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.benchmark.id
  cidr_block              = var.public_subnet_cidr
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = {
    Name = "clickstream-benchmark-public"
  }
}

resource "aws_subnet" "private" {
  vpc_id            = aws_vpc.benchmark.id
  cidr_block        = var.private_subnet_cidr
  availability_zone = "${var.aws_region}a"

  tags = {
    Name = "clickstream-benchmark-private"
  }
}

# ==============================================================================
# INTERNET GATEWAY
# ==============================================================================

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.benchmark.id

  tags = {
    Name = "clickstream-benchmark-igw"
  }
}

# ==============================================================================
# ROUTE TABLES
# ==============================================================================

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.benchmark.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = {
    Name = "clickstream-benchmark-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# ==============================================================================
# SECURITY GROUPS
# ==============================================================================

resource "aws_security_group" "benchmark" {
  name        = "clickstream-benchmark-sg"
  description = "Security group for benchmark EC2 instance"
  vpc_id      = aws_vpc.benchmark.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allowed_cidr]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "clickstream-benchmark-sg"
  }
}

# ==============================================================================
# EC2 INSTANCE
# ==============================================================================

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "benchmark" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = var.instance_type
  key_name               = var.key_pair_name
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.benchmark.id]

  user_data = templatefile("${path.module}/user_data.sh", {
    github_repo = var.github_repo_url
  })

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
  }

  tags = {
    Name = "clickstream-benchmark"
  }
}
