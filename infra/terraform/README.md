# GCP Lakehouse & BigQuery IaC (Terraform / OpenTofu)

Declarative infrastructure for the optional GCP lakehouse and warehouse sync (`projects/ecommerce/cloud_sync/export_to_gcp.py`), engineered to remain within **GCP Always-Free Tier** limits:

- **GCS Bucket**: Uniform bucket-level access with a **7-day object auto-delete lifecycle rule**.
- **BigQuery Warehouse**: Time-partitioned (`DAY`) and clustered (`category`, `url`) Gold tables.
- **Least-Privilege IAM**: Dedicated `ecommerce-pipeline-sync` Service Account scoped strictly to the provisioned bucket and dataset.

## Usage

```bash
cd infra/terraform
terraform init
terraform plan -var="project_id=your-gcp-project" -var="gcs_bucket_name=your-unique-bucket"
terraform apply -var="project_id=your-gcp-project" -var="gcs_bucket_name=your-unique-bucket"
```
