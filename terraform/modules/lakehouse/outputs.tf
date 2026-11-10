output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "warehouse" {
  description = "Iceberg warehouse root (ICEBERG_WAREHOUSE)."
  value       = "s3://${aws_s3_bucket.lake.bucket}/warehouse"
}

output "glue_databases" {
  value = { for layer, db in aws_glue_catalog_database.layer : layer => db.name }
}

output "spark_job_role_arn" {
  value = aws_iam_role.spark_job.arn
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "msk_cluster_arn" {
  value = aws_msk_serverless_cluster.events.arn
}
