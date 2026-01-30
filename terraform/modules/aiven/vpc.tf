# ==============================================================================
# AIVEN PROJECT VPC
# ==============================================================================
# Required for AWS PrivateLink - services must be deployed in this VPC

resource "aiven_project_vpc" "this" {
  count = var.privatelink_enabled ? 1 : 0

  project      = aiven_project.this.project
  cloud_name   = var.cloud_name
  network_cidr = var.aiven_vpc_cidr

  timeouts {
    create = "10m"
  }
}
