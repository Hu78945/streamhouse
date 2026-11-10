terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
  # Remote state belongs here in a real deployment, e.g.:
  # backend "s3" { bucket = "<state-bucket>" key = "streamhouse/terraform.tfstate" region = "eu-west-1" use_lockfile = true }
}
