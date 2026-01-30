# ==============================================================================
# AWS PRIVATELINK RESOURCES
# ==============================================================================
# Creates PrivateLink endpoints for each Aiven service

resource "aiven_aws_privatelink" "kafka" {
  count = var.privatelink_enabled ? 1 : 0

  project      = aiven_project.this.project
  service_name = aiven_kafka.this.service_name

  principals = [
    "arn:aws:iam::${var.aws_account_id}:root"
  ]

  depends_on = [aiven_project_vpc.this]
}

resource "aiven_aws_privatelink" "pg" {
  count = var.privatelink_enabled ? 1 : 0

  project      = aiven_project.this.project
  service_name = aiven_pg.this.service_name

  principals = [
    "arn:aws:iam::${var.aws_account_id}:root"
  ]

  depends_on = [aiven_project_vpc.this]
}

resource "aiven_aws_privatelink" "valkey" {
  count = var.privatelink_enabled ? 1 : 0

  project      = aiven_project.this.project
  service_name = aiven_valkey.this.service_name

  principals = [
    "arn:aws:iam::${var.aws_account_id}:root"
  ]

  depends_on = [aiven_project_vpc.this]
}

resource "aiven_aws_privatelink" "opensearch" {
  count = var.privatelink_enabled && var.opensearch_enabled ? 1 : 0

  project      = aiven_project.this.project
  service_name = aiven_opensearch.this[0].service_name

  principals = [
    "arn:aws:iam::${var.aws_account_id}:root"
  ]

  depends_on = [aiven_project_vpc.this]
}
