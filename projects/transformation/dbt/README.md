# Dimensional Data Transformation (dbt + DuckDB)

This project implements an in-process SQL transformation and dimensional data modeling pipeline using **dbt** and **DuckDB**. 

It runs entirely inside a dedicated, isolated Docker container with **zero local dependencies**, providing an isolated development environment for SQL modeling, schema testing, and data mart generation.

---

## 📁 Project Structure

```text
projects/transformation/dbt/
├── Dockerfile                  # Container definition with dbt-duckdb and duckdb CLI
├── docker-compose.yml          # Container volume mounting and configuration
├── Makefile                    # CLI shortcut commands (make dbt-build, make query)
├── dbt_project.yml             # dbt project configurations & materialization policies
├── profiles.yml                # DuckDB adapter connection profiles
├── data/
│   └── raw/                    # Raw JSON transactional datasets
├── models/
│   ├── staging/
│   │   ├── schema.yml          # Staging documentation and schema tests
│   │   ├── stg_customers.sql   # Cleansed customer profile views
│   │   ├── stg_products.sql    # Cleansed product catalog views
│   │   └── stg_purchases.sql   # Cleansed and quarter-enriched transaction views
│   ├── intermediate/
│   │   └── int_enriched_purchases.sql # Denormalized multi-table join view
│   └── marts/
│       ├── schema.yml          # Mart documentation and quality tests
│       ├── fct_customer_spending.sql  # Aggregated quarterly customer spending mart
│       └── fct_category_revenue.sql   # Aggregated quarterly product category revenue mart
└── BUILD                       # Pants build system integration
```

---

## 🚀 How to Run (via Docker & Make)

No local installation of Python packages, dbt, or DuckDB is required on your host system.

### 1. Build the Docker Image
```bash
cd projects/transformation/dbt
make build
```

### 2. Test Connection
Validate the dbt-DuckDB profile connection:
```bash
make dbt-debug
```

### 3. Execute Transformation Pipeline & Tests
Run models and execute data quality tests:
```bash
# Run models + test assertions
make dbt-build

# Or run separately
make dbt-run
make dbt-test
```

### 4. Interactive DuckDB Querying
Open an interactive DuckDB shell inside the container to query the generated tables:
```bash
make query
```

Example SQL inside the prompt:
```sql
SELECT * FROM fct_customer_spending;
SELECT * FROM fct_category_revenue;
.exit
```

### 5. Clean Up
To remove local database files and compiled artifacts:
```bash
make clean
```
