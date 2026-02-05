locals {
  log_group_name = "/aws-glue/jobs/${var.name}"

  base_default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-language"                     = "python"
    "--TempDir"                          = var.temp_dir
    "--extra-py-files"                   = var.extra_py_files_s3_path
    "--client-id"                        = "REQUIRED_AT_RUNTIME"
    "--batch-id"                         = "REQUIRED_AT_RUNTIME"
    "--pipeline"                         = "REQUIRED_AT_RUNTIME"
  }
}

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "glue_job_role" {
  name               = "${var.name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "glue_job_policy" {
  statement {
    sid    = "GlueLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "InputRead"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = concat(
      var.s3_input_prefixes,
      [for prefix in var.s3_input_prefixes : "${prefix}*"]
    )
  }

  statement {
    sid    = "OutputWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = concat(
      var.s3_output_prefixes,
      [for prefix in var.s3_output_prefixes : "${prefix}*"]
    )
  }
}

resource "aws_iam_role_policy" "glue_job_policy" {
  name   = "${var.name}-inline-policy"
  role   = aws_iam_role.glue_job_role.id
  policy = data.aws_iam_policy_document.glue_job_policy.json
}

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = local.log_group_name
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_glue_job" "job" {
  name              = var.name
  description       = var.description
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  max_retries       = var.max_retries
  timeout           = var.timeout
  tags              = var.tags

  command {
    name            = "glueetl"
    script_location = var.script_s3_path
    python_version  = var.python_version
  }

  default_arguments = merge(local.base_default_arguments, var.default_arguments)

  depends_on = [aws_cloudwatch_log_group.glue_job]
}

resource "aws_cloudwatch_metric_alarm" "failed_runs" {
  alarm_name          = "${var.name}-failed-runs"
  alarm_description   = "Alert when Glue job ${var.name} has failed runs."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  treat_missing_data  = "notBreaching"
  dimensions = {
    JobName = aws_glue_job.job.name
  }
  alarm_actions = var.alarm_sns_topic_arn == null ? [] : [var.alarm_sns_topic_arn]
  ok_actions    = var.alarm_sns_topic_arn == null ? [] : [var.alarm_sns_topic_arn]
  tags          = var.tags
}
