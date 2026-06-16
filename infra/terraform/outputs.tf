output "service_account_email" {
  description = "Email of the service account"
  value       = google_service_account.main.email
}

output "gcs_bucket_name" {
  description = "Name of the Bronze layer GCS bucket"
  value       = google_storage_bucket.bronze.name
}

output "bigquery_dataset" {
  description = "Name of the BigQuery dataset"
  value       = google_bigquery_dataset.main.dataset_id
}

output "key_creation_command" {
  description = "Command to create service account key manually"
  value       = "gcloud iam service-accounts keys create keys/nyc-uoip-sa-key.json --iam-account=${google_service_account.main.email}"
}

output "composer_dags_gcs_prefix" {
  description = "GCS prefix for Composer DAGs (use for deploy-composer make target)"
  value       = google_composer_environment.main.config[0].dag_gcs_prefix
}

output "composer_airflow_uri" {
  description = "Airflow web UI URL"
  value       = google_composer_environment.main.config[0].airflow_uri
}