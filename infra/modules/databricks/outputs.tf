output "cluster_id" {
  description = "ID of the created Databricks cluster"
  value       = databricks_cluster.curve_formation.id
}

output "policy_id" {
  description = "ID of the created cluster policy"
  value       = databricks_cluster_policy.curve_formation.id
}

output "job_id" {
  description = "ID of the created Databricks job"
  value       = databricks_job.curve_formation.id
}

output "job_url" {
  description = "URL of the created Databricks job"
  value       = databricks_job.curve_formation.url
}
