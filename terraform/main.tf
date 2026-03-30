terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "gharchive_raw" {
  name                        = var.bucket_name
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }
}

resource "google_storage_bucket" "bq_temp" {
  name                        = var.bq_temp_bucket_name
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 3
    }
  }
}

resource "google_bigquery_dataset" "github_trends_dataset" {
  dataset_id = var.dataset_id
  location   = var.region
}