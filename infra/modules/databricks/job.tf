resource "databricks_job" "curve_formation_job" {
  name = "Curve Formation Pipeline"
  
  job_cluster {
    job_cluster_key = "curve_formation_cluster"
    
    new_cluster {
      spark_version = "13.3.x-scala2.12"
      node_type_id  = "Standard_DS3_v2"
      
      spark_conf = {
        "spark.databricks.delta.preview.enabled" : "true"
      }
      
      custom_tags = {
        "Environment" = var.environment
        "Project"     = "curve_formation"
      }

      autoscale {
        min_workers = 2
        max_workers = 8
      }
    }
  }

  schedule {
    quartz_cron_expression = "0 30 5 * * ?" # Run daily at 5:30 UTC
    timezone_id = "UTC"
  }

  notebook_task {
    notebook_path = "/Shared/curve_formation/curve_formation_orchestrator"
    base_parameters = {
      env = var.environment
    }
  }

  email_notifications {
    on_success = [var.notification_email]
    on_failure = [var.notification_email]
  }

  max_concurrent_runs = 1
  timeout_seconds    = 7200 # 2 hours timeout
}
