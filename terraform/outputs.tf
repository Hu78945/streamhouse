output "bucket" {
  value = module.lakehouse.bucket
}

output "warehouse" {
  value = module.lakehouse.warehouse
}

output "spark_job_role_arn" {
  value = module.lakehouse.spark_job_role_arn
}

output "emr_application_id" {
  value = module.lakehouse.emr_application_id
}

output "msk_cluster_arn" {
  value = module.lakehouse.msk_cluster_arn
}
