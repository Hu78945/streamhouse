provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "streamhouse", environment = var.environment, managed_by = "terraform" }
  }
}

module "lakehouse" {
  source = "./modules/lakehouse"

  name               = "streamhouse-${var.environment}"
  subnet_ids         = var.subnet_ids
  security_group_ids = var.security_group_ids
}
