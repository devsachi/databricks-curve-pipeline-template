terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

# Cluster policy for curve formation
resource "databricks_cluster_policy" "curve_formation" {
  name       = "${var.environment}_curve_formation_policy"
  definition = jsonencode({
    "node_type_id": {
      "type": "fixed",
      "value": var.node_type_id
    },
    "spark_version": {
      "type": "fixed",
      "value": var.spark_version
    },
    "autoscale.min_workers": {
      "type": "fixed",
      "value": var.min_workers
    },
    "autoscale.max_workers": {
      "type": "fixed",
      "value": var.max_workers
    },
    "custom_tags.environment": {
      "type": "fixed",
      "value": var.environment
    },
    "custom_tags.purpose": {
      "type": "fixed",
      "value": "curve_formation"
    }
  })
}

# Job cluster for curve formation pipeline
resource "databricks_cluster" "curve_formation" {
  cluster_name            = "${var.environment}_curve_formation"
  spark_version          = var.spark_version
  node_type_id           = var.node_type_id
  policy_id              = databricks_cluster_policy.curve_formation.id
  autotermination_minutes = var.autotermination_minutes

  autoscale {
    min_workers = var.min_workers
    max_workers = var.max_workers
  }

  spark_conf = {
    "spark.databricks.cluster.profile"                   = "serverless"
    "spark.databricks.repl.allowedLanguages"            = "python,sql"
    "spark.databricks.delta.preview.enabled"            = "true"
    "spark.databricks.python.defaultPythonVersion"       = "3.9"
    "spark.databricks.delta.optimizeWrite.enabled"       = "true"
    "spark.databricks.delta.autoCompact.enabled"        = "true"
    "spark.sql.shuffle.partitions"                      = "auto"
    "spark.databricks.adaptive.autoOptimizeShuffle.enabled" = "true"
  }

  library {
    pypi {
      package = "pandas"
    }
  }

  library {
    pypi {
      package = "numpy"
    }
  }

  library {
    pypi {
      package = "scipy"
    }
  }

  custom_tags = {
    environment = var.environment
    purpose     = "curve_formation"
  }
}

# Databricks job for curve formation pipeline
resource "databricks_job" "curve_formation" {
  name = "${var.environment}_curve_formation_pipeline"

  job_cluster {
    job_cluster_key = "curve_formation_cluster"
    new_cluster {
      cluster_name            = "${var.environment}_curve_formation_job"
      spark_version          = var.spark_version
      node_type_id           = var.node_type_id
      policy_id              = databricks_cluster_policy.curve_formation.id
      
      autoscale {
        min_workers = var.min_workers
        max_workers = var.max_workers
      }
    }
  }

  task {
    task_key = "curve_formation"
    notebook_task {
      notebook_path = var.notebook_path
      base_parameters = {
        environment = var.environment
      }
    }
    job_cluster_key = "curve_formation_cluster"
  }

  schedule {
    quartz_cron_expression = var.schedule_cron
    timezone_id = "UTC"
  }

  email_notifications {
    on_success = var.notification_emails
    on_failure = var.notification_emails
  }

  max_concurrent_runs = 1
  timeout_seconds    = var.timeout_seconds
}
