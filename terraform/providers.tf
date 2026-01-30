# ==============================================================================
# TERRAFORM CONFIGURATION
# ==============================================================================

terraform {
  required_version = ">= 1.5.7"

  required_providers {
    aiven = {
      source  = "aiven/aiven"
      version = "4.49.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

# ==============================================================================
# PROVIDERS
# ==============================================================================

provider "aiven" {
  api_token = var.aiven_api_token
}

provider "aws" {
  region = var.aws_region
}
