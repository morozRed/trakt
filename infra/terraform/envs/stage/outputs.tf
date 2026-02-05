output "glue_job_name" {
  value = module.glue_job.job_name
}

output "glue_job_arn" {
  value = module.glue_job.job_arn
}

output "glue_role_arn" {
  value = module.glue_job.job_role_arn
}
