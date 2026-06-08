# ── Service Account ──────────────────────────────────────────────────────────

resource "google_service_account" "main" {
  account_id   = var.service_account_name
  display_name = "NYC UOIP Service Account"
  description  = "Service account for NYC Urban Operations Intelligence Platform"
}

# ── Service Account Key ──────────────────────────────────────────────────────
# NOTE: Key creation may be disabled by organization policy.
# If terraform apply fails with "Key creation is not allowed",
# create the key manually:
#   gcloud iam service-accounts keys create keys/nyc-uoip-sa-key.json \
#     --iam-account=nyc-uoip-sa@${var.project_id}.iam.gserviceaccount.com
#
# Or use Workload Identity Federation for production (recommended).
# See: https://cloud.google.com/blog/products/identity-security/enabling-keyless-authentication-from-any-environment

# ── IAM Bindings ─────────────────────────────────────────────────────────────

resource "google_project_iam_member" "storage_object_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.main.email}"
}

resource "google_project_iam_member" "bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.main.email}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.main.email}"
}

resource "google_project_iam_member" "dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.main.email}"
}

# ── GCS Bucket (Bronze/Silver/Gold unified bucket) ──────────────────────────

resource "google_storage_bucket" "bronze" {
  name          = var.gcs_bucket_name
  location      = var.region
  storage_class = "STANDARD"

  labels = {
    environment = var.environment
    platform     = "nyc-uoip"
    layer        = "bronze"
  }

  uniform_bucket_level_access = true
  public_access_prevention    = "inherited"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90  # Archive after 90 days
    }
    action {
      type = "Delete"
    }
  }
}

# ── BigQuery Dataset ──────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "main" {
  dataset_id    = var.bigquery_dataset
  friendly_name  = "NYC UOIP Data Warehouse"
  description    = "NYC Urban Operations Intelligence Platform - Gold Layer"
  location       = var.region
  default_table_expiration_ms = null  # No expiration

  labels = {
    environment = var.environment
    platform     = "nyc-uoip"
  }

  # Allow external access via Data Transfer API
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
}