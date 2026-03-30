variable "project_id" {
  type        = string
  description = "Existing GCP project ID"
  default = "gharchive-events-platform-01"
}

variable "region" {
  type        = string
  description = "GCP region for resources"
  default     = "us-central1"
}

variable "bucket_name" {
  type        = string
  description = "GCS bucket name for raw GH Archive files"
  default     = "gharchive-events-platform-raw-dev"
}

variable "bq_temp_bucket_name" {
  type        = string
  description = "Temporary GCS bucket used by Spark BigQuery connector"
  default     = "gharchive-events-platform-bq-temp"
}

variable "dataset_id" {
  type        = string
  description = "BigQuery dataset for source and analytics tables"
  default     = "gh_events_trends_dev"
}