variable "project_id" {
  description = "GCP Project ID hosting the e-commerce lakehouse and BigQuery warehouse."
  type        = string
}

variable "region" {
  description = "Default GCP compute region."
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "Globally unique GCS bucket name for compacted Gold Parquet snapshots."
  type        = string
}

variable "gcs_location" {
  description = "GCS bucket location (US stays within Free Tier region eligibility)."
  type        = string
  default     = "US"
}

variable "bq_dataset_id" {
  description = "BigQuery dataset ID for Gold analytical marts."
  type        = string
  default     = "ecommerce_warehouse"
}

variable "bq_location" {
  description = "BigQuery dataset location."
  type        = string
  default     = "US"
}
