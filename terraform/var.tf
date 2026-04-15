variable "project_id" {
  type        = string
  description = "Existing GCP project ID"
}

variable "region" {
  type        = string
  description = "GCP region for resources"
}

variable "bucket_name" {
  type        = string
  description = "GCS bucket name for raw GH Archive files"
  default     = "gharchive-events-platform-raw-dev"
}

variable "dataset_id" {
  type        = string
  description = "BigQuery dataset for source and analytics tables"
  default     = "gh_events_trends_dev"
}