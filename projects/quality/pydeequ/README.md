# PyDeequ Data Quality & Verification Pipeline

This sub-project implements scalable, distributed data quality assertions on Apache Spark using **Amazon Deequ (PyDeequ)**.

---

## 📁 Project Structure

```text
projects/quality/pydeequ/
├── validate_pyspark_deequ.py    # PyDeequ Spark 3.5.8 verification script
├── BUILD                        # Pants target definitions
└── README.md                    # Sub-project documentation
```

---

## 🚀 How to Run

Execute the PyDeequ verification suite using Pants:

```bash
./pants run projects/quality/pydeequ:validate
```

### Custom Input and Output Arguments
```bash
./pants run projects/quality/pydeequ:validate -- \
  --input-csv projects/quality/data/raw/user_data.csv \
  --output-dir projects/quality/output/pydeequ_metrics
```

---

## 🧪 Validated Checks
* `hasSize(lambda x: x > 0)`: Verifies dataset is not empty.
* `isComplete("user_id")`: Checks for absence of null values in primary key.
* `isUnique("user_id")`: Enforces uniqueness on primary key.
* `isComplete("email")`: Validates completeness of required email field.
* `isContainedIn("country", [...])`: Checks categorical integrity against allowed country codes.
