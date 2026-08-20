# Core Data Processing Utilities

This project contains foundational PySpark data processing and partitioning routines. It demonstrates local batch data extraction, text processing, and dataset partitioning techniques using PySpark.

---

## 📁 Project Contents

*   [word_count.py](projects/essentials/word_count.py): Processes unstructured text data to perform a classic word-count calculation.
*   [employee_partition_by_hire_date.py](projects/essentials/employee_partition_by_hire_date.py): Demonstrates PySpark DataFrame API usage, reading CSV employee data and partitioning the output by hire date.
*   `input_data/`: Contains sample CSV and text input files, such as [employee_data.csv](projects/essentials/input_data/employee_data.csv), for testing the scripts.

---

## 🚀 How to Run

Ensure your virtual environment is active and `JAVA_HOME` (Java 17) is exported in your environment (as configured in `.env`):
```bash
source .venv/bin/activate
export $(cat .env | xargs)
```

### 1. Run WordCount
Run the word count script via Pants or Python:
```bash
# Using Pants
./pants run projects/essentials:word_count

# Or using Python directly
python projects/essentials/word_count.py
```
This generates the results in `projects/essentials/output_data/word_count/`.

### 2. Run Employee Partitioning
Run the partitioning script via Pants or Python:
```bash
# Using Pants
./pants run projects/essentials:employee_partition

# Or using Python directly
python projects/essentials/employee_partition_by_hire_date.py
```
This partitions the employee data and writes it to `projects/essentials/output_data/employee_partition/`.
