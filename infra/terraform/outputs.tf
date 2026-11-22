output "gcs_bucket_url" {
  description = "Provisioned GCS bucket URL."
  value       = "gs://${google_storage_bucket.datalake.name}"
}

output "bigquery_dataset_id" {
  description = "Provisioned BigQuery Gold warehouse dataset ID."
  value       = google_bigquery_dataset.ecommerce_warehouse.dataset_id
}

output "pipeline_service_account_email" {
  description = "Least-privilege service account email for pipeline cloud_sync."
  value       = google_service_account.pipeline_sync.email
}
