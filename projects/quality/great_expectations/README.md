# Great Expectations Validation Pipeline

This sub-project implements automated data validation and quality assertions using the **Great Expectations Fluent API (v0.18+)** on raw tabular datasets.

---

## 📁 Project Structure

```text
projects/quality/great_expectations/
├── validate_user_data.py        # Great Expectations Fluent API validation script
├── BUILD                        # Pants target definitions
└── README.md                    # Sub-project documentation
```

---

## 🚀 How to Run

Execute the validation pipeline using Pants:

```bash
./pants run projects/quality/great_expectations:validate
```

### Custom Input and Output Arguments
```bash
./pants run projects/quality/great_expectations:validate -- \
  --input-csv projects/quality/data/raw/user_data.csv \
  --output-report projects/quality/output/gx_validation_results.json
```

---

## 🧪 Validated Assertions
* `expect_column_values_to_not_be_null("email")`
* `expect_column_values_to_match_strftime_format("signup_date", "%Y-%m-%d %H:%M:%S")`
* `expect_column_values_to_be_in_set("country", ["US", "IN", "UK", "FR", "CA"])`
* `expect_column_values_to_be_unique("user_id")`
