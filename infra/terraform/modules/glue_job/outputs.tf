output "job_name" {
  description = "Glue job name."
  value       = aws_glue_job.job.name
}

output "job_arn" {
  description = "Glue job ARN."
  value       = aws_glue_job.job.arn
}

output "job_role_arn" {
  description = "Glue IAM role ARN."
  value       = aws_iam_role.glue_job_role.arn
}

output "log_group_name" {
  description = "CloudWatch log group for Glue logs."
  value       = aws_cloudwatch_log_group.glue_job.name
}
