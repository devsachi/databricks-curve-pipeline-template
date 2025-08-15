variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
}

variable "databricks_token" {
  description = "Databricks access token"
  type        = string
  sensitive   = true
}

variable "spark_version" {
  description = "Spark version for the cluster"
  type        = string
  default     = "13.3.x-scala2.12"
}

variable "node_type_id" {
  description = "Instance type for cluster nodes"
  type        = string
  default     = "i3.xlarge"
}

variable "min_workers" {
  description = "Minimum number of workers in autoscaling"
  type        = number
  default     = 2
}

variable "max_workers" {
  description = "Maximum number of workers in autoscaling"
  type        = number
  default     = 8
}

variable "autotermination_minutes" {
  description = "Time in minutes after which cluster will auto-terminate if idle"
  type        = number
  default     = 30
}

variable "notebook_path" {
  description = "Path to the notebook that will be executed by the job"
  type        = string
}

variable "schedule_cron" {
  description = "Cron expression for job scheduling"
  type        = string
  default     = "0 0 * * *"  # Daily at midnight UTC
}

variable "notification_emails" {
  description = "List of email addresses for job notifications"
  type        = list(string)
  default     = []
}

variable "timeout_seconds" {
  description = "Timeout in seconds for the job"
  type        = number
  default     = 7200  # 2 hours
}
