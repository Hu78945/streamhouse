# Execution role for the Spark jobs. Least privilege: only the warehouse/checkpoint/log prefixes of
# this bucket, only this KMS key, only the three Glue databases, and read-only on the telemetry topics.
locals {
  account   = data.aws_caller_identity.current.account_id
  region    = data.aws_region.current.region
  msk_scope = trimprefix(aws_msk_serverless_cluster.events.arn, "arn:aws:kafka:${local.region}:${local.account}:cluster/") # name/uuid
  glue_dbs  = [for db in aws_glue_catalog_database.layer : db.name]
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account]
    }
  }
}

resource "aws_iam_role" "spark_job" {
  name               = "${var.name}-spark-job"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "spark_job" {
  statement {
    sid       = "ListLakePrefixes"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["warehouse/*", "checkpoints/*", "logs/*"]
    }
  }

  statement {
    sid     = "ReadWriteLakeObjects"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [
      "${aws_s3_bucket.lake.arn}/warehouse/*",
      "${aws_s3_bucket.lake.arn}/checkpoints/*",
      "${aws_s3_bucket.lake.arn}/logs/*",
    ]
  }

  statement {
    sid       = "UseLakeKey"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.lake.arn]
  }

  statement {
    sid = "IcebergGlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
    ]
    resources = concat(
      ["arn:aws:glue:${local.region}:${local.account}:catalog"],
      [for db in local.glue_dbs : "arn:aws:glue:${local.region}:${local.account}:database/${db}"],
      [for db in local.glue_dbs : "arn:aws:glue:${local.region}:${local.account}:table/${db}/*"],
    )
  }

  statement {
    sid       = "MskConnect"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [aws_msk_serverless_cluster.events.arn]
  }

  statement {
    sid       = "MskReadTelemetryTopics"
    actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:ReadData"]
    resources = ["arn:aws:kafka:${local.region}:${local.account}:topic/${local.msk_scope}/${var.topic_prefix}*"]
  }

  statement {
    # The Spark Kafka source manages offsets in its checkpoint, but still uses consumer groups named spark-kafka-source-*.
    sid       = "MskSparkConsumerGroups"
    actions   = ["kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"]
    resources = ["arn:aws:kafka:${local.region}:${local.account}:group/${local.msk_scope}/spark-kafka-source-*"]
  }
}

resource "aws_iam_role_policy" "spark_job" {
  name   = "lakehouse-access"
  role   = aws_iam_role.spark_job.id
  policy = data.aws_iam_policy_document.spark_job.json
}
