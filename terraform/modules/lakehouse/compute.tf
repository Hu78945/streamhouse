# MSK Serverless: IAM auth only (no SASL/SCRAM secrets to rotate), scales partitions without broker sizing.
resource "aws_msk_serverless_cluster" "events" {
  cluster_name = "${var.name}-events"

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

# EMR Serverless runs both the long-lived streaming job and the nightly maintenance jobs.
resource "aws_emrserverless_application" "spark" {
  name          = "${var.name}-spark"
  release_label = var.emr_release_label
  type          = "spark"

  maximum_capacity {
    cpu    = "32 vCPU"
    memory = "128 GB"
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  network_configuration {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }
}
