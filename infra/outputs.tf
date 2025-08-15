output "notebook_url" {
  value = databricks_notebook.curve_formation.url
}

output "job_id" {
  value = databricks_job.monthly_curve.id
}
