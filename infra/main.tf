terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

module "databricks_resources" {
  source = "./modules/databricks"
  
  environment       = var.environment
  notification_email = var.notification_email
  databricks_host   = var.databricks_host
  databricks_token  = var.databricks_token
}
