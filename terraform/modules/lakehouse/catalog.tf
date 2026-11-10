# One Glue database per medallion layer, named exactly like the Spark namespaces the jobs use
# (bronze/silver/gold). That assumes one environment per AWS account, the usual multi-account setup.
# Iceberg's GlueCatalog keeps each table's current metadata pointer in Glue and commits with optimistic locking.
resource "aws_glue_catalog_database" "layer" {
  for_each     = toset(["bronze", "silver", "gold"])
  name         = each.key
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/warehouse/${each.key}.db"
}
