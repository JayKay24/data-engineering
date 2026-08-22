# Cloud Lakehouse & Analytical Storage (GCS & BigQuery)

This project provides utilities for interacting with cloud data lakes (Google Cloud Storage) and cloud data warehouses (Google Cloud BigQuery) using official Google Cloud client libraries and pandas.

---

## 📁 Project Structure

```text
projects/storage/cloud/
├── config.py                    # GCP project, GCS bucket, and BigQuery dataset settings
├── read_gcs_data.py             # Reads raw/lakehouse objects from GCS via streaming or pandas
├── query_bigquery.py            # Executes parameterized SQL analytics against BigQuery
├── BUILD                        # Pants python_sources and pex_binary targets
└── README.md                    # Setup and execution guide
```

---

## 🚀 How to Run

### 1. Prerequisites & GCP Authentication
Ensure you have authenticated to Google Cloud using Application Default Credentials (ADC) or by providing a service account key file:

```bash
# Authenticate using your Google Cloud account
gcloud auth application-default login
```

Alternatively, set the service account key path:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### 2. Environment Variables Configuration
Configure your target GCP resources:
* `GCP_PROJECT`: Your Google Cloud Project ID.
* `GCS_BUCKET`: Target GCS bucket name.
* `BIGQUERY_DATASET`: Target BigQuery dataset name.

### 3. Read Objects from Google Cloud Storage
List and inspect data blobs (JSON, CSV, Parquet):
```bash
./pants run projects/storage/cloud:read_gcs_data -- --bucket my-lakehouse-bucket --prefix raw/purchases/
```

### 4. Query BigQuery Warehouse
Execute an interactive query and inspect results formatted as a DataFrame:
```bash
./pants run projects/storage/cloud:query_bigquery -- --query "SELECT * FROM \`my-project.retail_warehouse.fct_customer_spending\` LIMIT 10"
```
