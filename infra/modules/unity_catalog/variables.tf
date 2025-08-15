variable "metastore_name" {
  description = "Name of the Unity Catalog metastore"
  type        = string
}

variable "storage_root" {
  description = "Storage root path for the metastore"
  type        = string
}

variable "storage_credential" {
  description = "Name of the storage credential to use"
  type        = string
}

variable "force_destroy" {
  description = "Whether to force destroy the metastore"
  type        = bool
  default     = false
}

variable "owner" {
  description = "Owner of the metastore"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}
