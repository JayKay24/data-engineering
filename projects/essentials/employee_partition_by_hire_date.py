import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

# Set up paths relative to this script's directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.path.join(SCRIPT_DIR, "input_data", "employee_data.csv")
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "output_data", "employee_partitioned")

# Step 1: Initialize Spark Session
spark = (
    SparkSession.builder.appName("Employee Data Partitioning")
    .master("local[*]")
    .getOrCreate()
)

# Step 2: Read the CSV file
employee_df = (
    spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
)

# Step 3: Add year and month columns for partitioning
employee_df_with_date = employee_df.withColumn(
    "hire_year", year("hire_date")
).withColumn("hire_month", month("hire_date"))

# Step 4: Write the data partitioned by hire_year and hire_month
employee_df_with_date.write.partitionBy("hire_year", "hire_month").mode(
    "overwrite"
).parquet(OUTPUT_PATH)

print(f"Partitioned data successfully written to {OUTPUT_PATH}")

# Step 5: Stop the Spark session
spark.stop()
