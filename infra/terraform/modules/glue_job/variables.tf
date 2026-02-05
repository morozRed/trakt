variable "name" {
  description = "Glue job name."
  type        = string
}

variable "description" {
  description = "Glue job description."
  type        = string
  default     = "Trakt ETL Glue job"
}

variable "script_s3_path" {
  description = "S3 URI to Glue entrypoint script (glue_main.py wrapper)."
  type        = string
}

variable "extra_py_files_s3_path" {
  description = "S3 URI to wheel bundle passed via --extra-py-files."
  type        = string
}

variable "temp_dir" {
  description = "Glue temporary directory (s3://...)."
  type        = string
}

variable "glue_version" {
  description = "AWS Glue version."
  type        = string
  default     = "4.0"
}

variable "python_version" {
  description = "Python version passed to Glue command."
  type        = string
  default     = "3"
}

variable "worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of Glue workers."
  type        = number
  default     = 2
}

variable "max_retries" {
  description = "Maximum retry attempts."
  type        = number
  default     = 0
}

variable "timeout" {
  description = "Job timeout in minutes."
  type        = number
  default     = 60
}

variable "s3_input_prefixes" {
  description = "List of S3 prefixes the job can read from."
  type        = list(string)
}

variable "s3_output_prefixes" {
  description = "List of S3 prefixes the job can write to."
  type        = list(string)
}

variable "log_retention_days" {
  description = "CloudWatch log group retention in days."
  type        = number
  default     = 30
}

variable "alarm_sns_topic_arn" {
  description = "Optional SNS topic ARN for Glue alarms."
  type        = string
  default     = null
}

variable "default_arguments" {
  description = "Additional Glue job arguments."
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Resource tags."
  type        = map(string)
  default     = {}
}
