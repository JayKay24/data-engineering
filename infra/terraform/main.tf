terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.40"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "datalake" {
  name                        = var.gcs_bucket_name
  location                    = var.gcs_location
  uniform_bucket_level_access = true
  force_destroy               = true

  # Auto-expire artifacts after 7 days to guarantee GCS Free Tier compliance.
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "ecommerce_warehouse" {
  dataset_id                  = var.bq_dataset_id
  friendly_name               = "E-Commerce Gold Warehouse"
  description                 = "Partitioned and clustered Gold marts synced from DuckDB."
  location                    = var.bq_location
  delete_contents_on_destroy  = true
}

resource "google_bigquery_table" "daily_category_sales" {
  dataset_id          = google_bigquery_dataset.ecommerce_warehouse.dataset_id
  table_id            = "daily_category_sales"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "sales_date"
  }

  clustering = ["category"]

  schema = jsonencode([
    { name = "sales_date", type = "DATE", mode = "REQUIRED" },
    { name = "category", type = "STRING", mode = "REQUIRED" },
    { name = "daily_revenue", type = "FLOAT64", mode = "REQUIRED" },
    { name = "daily_units", type = "INT64", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "daily_url_conversion" {
  dataset_id          = google_bigquery_dataset.ecommerce_warehouse.dataset_id
  table_id            = "daily_url_conversion"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "conversion_date"
  }

  clustering = ["url"]

  schema = jsonencode([
    { name = "conversion_date", type = "DATE", mode = "REQUIRED" },
    { name = "url", type = "STRING", mode = "REQUIRED" },
    { name = "total_views", type = "INT64", mode = "REQUIRED" },
    { name = "total_purchases", type = "INT64", mode = "REQUIRED" },
    { name = "avg_conversion_rate", type = "FLOAT64", mode = "REQUIRED" },
  ])
}

resource "google_service_account" "pipeline_sync" {
  account_id   = "ecommerce-pipeline-sync"
  display_name = "E-Commerce Pipeline GCS & BigQuery Sync"
}

resource "google_storage_bucket_iam_member" "datalake_object_admin" {
  bucket = google_storage_bucket.datalake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sync.email}"
}

resource "google_bigquery_dataset_iam_member" "warehouse_editor" {
  dataset_id = google_bigquery_dataset.ecommerce_warehouse.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sync.email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sync.email}"
}
