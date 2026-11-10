variable "name" {
  description = "Prefix for every resource name, e.g. streamhouse-dev."
  type        = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "topic_prefix" {
  description = "Kafka topics the Spark job may read (fleet.telemetry, fleet.telemetry.dlq, ...)."
  type        = string
  default     = "fleet.telemetry"
}

variable "noncurrent_version_days" {
  description = "Keep overwritten/deleted object versions this long. Iceberg snapshot expiry is the primary retention control; this is the safety net under it."
  type        = number
  default     = 14
}

variable "bronze_infrequent_access_days" {
  description = "Move raw bronze data files to STANDARD_IA after this many days (they are rarely re-read)."
  type        = number
  default     = 30
}

variable "emr_release_label" {
  description = "EMR Serverless release (EMR 7.x ships Spark 3.5)."
  type        = string
  default     = "emr-7.9.0"
}
