terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Consider using "gcs" for Google Cloud or "s3" for AWS to manage state files in the cloud
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

locals {
  common_tags = {
    project     = var.project_id
    managed_by  = "terraform"
    environment = terraform.workspace
  }
}

# GCS Buckets for ETL
resource "google_storage_bucket" "etl_raw" {
  name                        = "${var.project_id}-etl-raw-${terraform.workspace}"
  location                    = var.region
  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  labels                      = local.common_tags
  versioning {
    enabled = true
  }
  force_destroy = true
}

resource "google_storage_bucket" "etl_processed" {
  name                        = "${var.project_id}-etl-processed-${terraform.workspace}"
  location                    = var.region
  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  labels                      = local.common_tags
  versioning {
    enabled = true
  }
  force_destroy = true
}

resource "google_storage_bucket" "etl_archive" {
  name                        = "${var.project_id}-etl-archive-${terraform.workspace}"
  location                    = var.region
  storage_class               = "ARCHIVE"
  uniform_bucket_level_access = true
  labels                      = local.common_tags
  versioning {
    enabled = true
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365  # days
    }
  }
  force_destroy = true
}

# BigQuery Datasets for ETL
resource "google_bigquery_dataset" "etl_src" {
  dataset_id = "etl_src"
  project    = var.project_id
  location   = var.region
  default_table_expiration_ms = 5184000000  # 60 days to milliseconds
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "etl_ods" {
  dataset_id = "etl_ods"
  project    = var.project_id
  location   = var.region
  default_table_expiration_ms = 5184000000
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "etl_dim" {
  dataset_id = "etl_dim"
  project    = var.project_id
  location   = var.region
  default_table_expiration_ms = 5184000000
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "etl_fact" {
  dataset_id = "etl_fact"
  project    = var.project_id
  location   = var.region
  default_table_expiration_ms = 5184000000
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "etl_mart" {
  dataset_id = "etl_mart"
  project    = var.project_id
  location   = var.region
  default_table_expiration_ms = 5184000000
  delete_contents_on_destroy = true
}

# IAM for Airflow Service Account
resource "google_service_account" "airflow" {
  account_id   = "airflow-account"
  display_name = "Airflow Service Account"
}

resource "google_project_iam_member" "airflow_gcs_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_gcs_creator" {
  project = var.project_id
  role    = "roles/storage.objectCreator"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_gcs_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_connection_user" {
  project = var.project_id
  role    = "roles/bigquery.connectionUser"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_connection_delegate" {
  project = var.project_id
  role    = "roles/bigquery.connectionAdmin"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_service_account" "viewer_sa" {
  account_id   = "viewer-account"
  display_name = "Viewer Service Account"
}

resource "google_project_iam_member" "viewer_sa_viewer" {
  project = var.project_id
  role    = "roles/viewer"
  member  = "serviceAccount:${google_service_account.viewer_sa.email}"
}

data "google_iam_policy" "bq_ods_policy" {
  binding {
    role = "roles/bigquery.dataOwner"
    members = toset(var.bq_ods_members)
  }
}

resource "google_bigquery_table_iam_policy" "bq_ods_places_policy" {
  dataset_id = google_bigquery_dataset.etl_ods.dataset_id
  table_id = var.bq_ods_places_tablename
  policy_data = data.google_iam_policy.bq_ods_policy.policy_data
}

resource "google_bigquery_table_iam_policy" "bq_ods_reviews_policy" {
  dataset_id = google_bigquery_dataset.etl_ods.dataset_id
  table_id = var.bq_ods_reviews_tablename
  policy_data = data.google_iam_policy.bq_ods_policy.policy_data
}
