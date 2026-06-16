# ── GCP APIs ─────────────────────────────────────────────────────────────────

resource "google_project_service" "composer" {
  project                    = var.project_id
  service                    = "composer.googleapis.com"
  disable_on_destroy         = false
}

resource "google_project_service" "secretmanager" {
  project            = var.project_id
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudbuild" {
  project            = var.project_id
  service            = "cloudbuild.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "container" {
  project            = var.project_id
  service            = "container.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iamcredentials" {
  project            = var.project_id
  service            = "iamcredentials.googleapis.com"
  disable_on_destroy = false
}

# ── Composer v2 Service Agent permission ─────────────────────────────────────
# Composer 2 requires its GCP-managed Service Agent to have this role.
# The agent SA is auto-created by GCP; we only grant it the required role.

data "google_project" "project" {
  project_id = var.project_id
}

resource "google_project_iam_member" "cloudservices_editor" {
  project    = var.project_id
  role       = "roles/editor"
  member     = "serviceAccount:${data.google_project.project.number}@cloudservices.gserviceaccount.com"
  depends_on = [google_project_service.composer]
}

resource "google_project_iam_member" "composer_agent_v2_ext" {
  project    = var.project_id
  role       = "roles/composer.ServiceAgentV2Ext"
  member     = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
  depends_on = [
    google_project_service.composer,
    google_project_service.iamcredentials,
  ]
}

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
  location      = var.storage_location
  storage_class = "STANDARD"

  labels = {
    environment = var.environment
    platform     = "nyc-uoip"
    layer        = "bronze"
  }

  uniform_bucket_level_access = true
  public_access_prevention    = "inherited"
  force_destroy               = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age = 90
    }
  #   condition {
  #     age = 90  # Archive after 90 days, 90后GCP不免费了
  #   }
  #   action {
  #     type = "Delete"
  #   }
  }
}

# ── Cloud Composer 2 ──────────────────────────────────────────────────────────
# COST WARNING: Composer 2 Small ≈ $10/day. Delete after backfill completes.
#   terraform destroy -target=google_composer_environment.main

resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.main.email}"
}

resource "google_composer_environment" "main" {
  name    = var.composer_env_name
  region  = var.region
  depends_on = [
    google_project_service.composer,
    google_project_service.cloudbuild,
    google_project_service.container,
    google_project_service.iamcredentials,
    google_project_iam_member.composer_agent_v2_ext,
    google_project_iam_member.cloudservices_editor,
    google_project_iam_member.composer_worker,
  ]

  config {
    software_config {
      # Composer 2 + Airflow 2.9 — Python 3.11 built-in
      image_version = "composer-2-airflow-2"

      env_variables = {
        GCS_BUCKET_NAME  = var.gcs_bucket_name
        DEPLOYMENT_PHASE = "1"
      }

      # Third-party deps our ingestion/ package needs (stdlib + google-cloud-storage already included)
      pypi_packages = {
        "pydantic"      = ">=2.0"
        "pyyaml"        = ""
        "requests"      = ">=2.31.0"
        "python-dotenv" = ""
      }
      # SOCRATA_APP_TOKEN is set post-apply via gcloud (see Makefile: make set-composer-secret).
      # Terraform does not manage secret values — they never touch terraform state.
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        min_count  = 1
        max_count  = 2
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = google_service_account.main.email
      zone            = "us-east1-d"
    }
  }
}

# ── BigQuery Dataset ──────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "main" {
  dataset_id    = var.bigquery_dataset
  friendly_name  = "NYC UOIP Data Warehouse"
  description    = "NYC Urban Operations Intelligence Platform - Gold Layer"
  location       = var.storage_location
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