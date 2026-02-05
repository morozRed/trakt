terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  base_tags = merge(
    {
      environment = "dev"
      managed_by  = "terraform"
      service     = "trakt"
    },
    var.tags
  )
}

module "glue_job" {
  source = "../../modules/glue_job"

  name                   = var.job_name
  description            = "Trakt ETL Glue job (dev)"
  script_s3_path         = var.script_s3_path
  extra_py_files_s3_path = var.extra_py_files_s3_path
  temp_dir               = var.temp_dir
  glue_version           = var.glue_version
  python_version         = var.python_version
  worker_type            = var.worker_type
  number_of_workers      = var.number_of_workers
  max_retries            = var.max_retries
  timeout                = var.timeout
  s3_input_prefixes      = var.s3_input_prefixes
  s3_output_prefixes     = var.s3_output_prefixes
  log_retention_days     = var.log_retention_days
  alarm_sns_topic_arn    = var.alarm_sns_topic_arn
  default_arguments      = var.default_arguments
  tags                   = local.base_tags
}
