variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region for resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment name (dev/staging/prod)"
  type        = string
  default     = "dev"
}

variable "service_account_name" {
  description = "Name of the service account"
  type        = string
  default     = "nyc-uoip-sa"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket for Bronze layer"
  type        = string
}

variable "bigquery_dataset" {
  description = "Name of the BigQuery dataset"
  type        = string
  default     = "nyc_uoip"
}