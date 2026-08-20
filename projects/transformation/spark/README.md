# PySpark Transformation & Delta Lake Curation (Chapter 5)

This project implements the batch data transformation, multi-table enrichment, and Delta Lake table curation patterns derived from Chapter 5 of *Hello Modern Data Pipelines*.

---

## 📁 Project Contents

*   [data_processing_job.py](projects/transformation/spark/data_processing_job.py): PySpark job that cleanses raw purchases, joins them with customer and product dimensions, computes business aggregations, and saves curated tables in Delta Lake format.
*   [data/raw/customers.json](projects/transformation/spark/data/raw/customers.json): Customer profile dataset (segments, regions, registration dates).
*   [data/raw/products.json](projects/transformation/spark/data/raw/products.json): Product catalog dataset (categories, price, brands).
*   [data/raw/purchases.json](projects/transformation/spark/data/raw/purchases.json): Transactional purchase records.
*   [BUILD](projects/transformation/spark/BUILD): Pants build definition with `python_sources`, `resources`, and `pex_binary` targets.

---

## 🚀 How to Run

Ensure your virtual environment is active and `JAVA_HOME` (Java 17) is exported in your environment (as configured in `.env`):
```bash
source .venv/bin/activate
export $(cat .env | xargs)
```

### 1. Run via Pants
Execute the transformation pipeline using Pants:
```bash
./pants run projects/transformation/spark:data_processing_job -- --curated-data-dir $(pwd)/projects/transformation/spark/data/curated
```

### 2. Run via Direct Python
Execute the transformation script directly:
```bash
python projects/transformation/spark/data_processing_job.py --curated-data-dir projects/transformation/spark/data/curated
```

### 3. Curated Outputs
The job generates Delta Lake tables in `projects/transformation/spark/data/curated/`:
* `enriched_purchases/`: Partitioned by `purchase_quarter`.
* `customer_spending/`: Aggregated quarterly spend and transaction counts by customer.
* `category_revenue/`: Aggregated quarterly revenue and units sold by product category.
