variable "region" {
  type    = string
  default = "eu-west-1"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "subnet_ids" {
  description = "Private subnets (2+ AZs) for MSK Serverless and the EMR Serverless application."
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security groups allowing the Spark job to reach MSK on 9098 (IAM auth)."
  type        = list(string)
}
