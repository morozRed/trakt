variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "job_name" {
  type        = string
  description = "Glue job name."
}

variable "script_s3_path" {
  type        = string
  description = "S3 path to glue_main.py script."
}

variable "extra_py_files_s3_path" {
  type        = string
  description = "S3 path to wheel package."
}

variable "temp_dir" {
  type        = string
  description = "Glue temp directory."
}

variable "glue_version" {
  type    = string
  default = "4.0"
}

variable "python_version" {
  type    = string
  default = "3"
}

variable "worker_type" {
  type    = string
  default = "G.1X"
}

variable "number_of_workers" {
  type    = number
  default = 4
}

variable "max_retries" {
  type    = number
  default = 1
}

variable "timeout" {
  type    = number
  default = 60
}

variable "s3_input_prefixes" {
  type = list(string)
}

variable "s3_output_prefixes" {
  type = list(string)
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "alarm_sns_topic_arn" {
  type    = string
  default = null
}

variable "default_arguments" {
  type    = map(string)
  default = {}
}

variable "tags" {
  type    = map(string)
  default = {}
}
