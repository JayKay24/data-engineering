# Essentials Project (Chapter 2)

This project contains initial Spark processing examples derived from Chapter 2 of *Hello Modern Data Pipelines*. It demonstrates basic local batch data processing using PySpark.

---

## 📁 Project Contents

*   [word_count.py](projects/essentials/word_count.py): Processes unstructured text data to perform a classic word-count calculation.
*   [employee_partition_by_hire_date.py](projects/essentials/employee_partition_by_hire_date.py): Demonstrates PySpark DataFrame API usage, reading CSV employee data and partitioning the output by hire date.
*   `input_data/`: Contains sample CSV and text input files, such as [employee_data.csv](projects/essentials/input_data/employee_data.csv), for testing the scripts.

---

## 🚀 How to Run

Before running the scripts, ensure your virtual environment is active:
```bash
source .venv/bin/activate
```

### 1. Run WordCount
Run the word count script:
```bash
python projects/essentials/word_count.py
```
This generates the results in `projects/essentials/output_data/word_count/`.

### 2. Run Employee Partitioning
Run the partitioning script:
```bash
python projects/essentials/employee_partition_by_hire_date.py
```
This partitions the employee data and writes it to `projects/essentials/output_data/employee_partition/`.
