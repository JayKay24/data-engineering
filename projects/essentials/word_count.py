# %%
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Set up paths relative to this script's directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.path.join(SCRIPT_DIR, "input_data", "sample_text.txt")
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "output_data", "word_counts")

# %% Initialize SparkSession
spark = (
    SparkSession.builder.appName("WordCountExample").master("local[*]").getOrCreate()
)

# %% Read text file
lines = spark.read.text(INPUT_PATH)

# %% Split lines into words
words = lines.select(explode(split(col("value"), "\\\\s+")).alias("word"))

# %% Count word frequencies
word_counts = words.groupBy("word").count()

# %% Write results to CSV
word_counts.write.mode("overwrite").csv(OUTPUT_PATH, header=True)

print(f"Word counts successfully written to {OUTPUT_PATH}")

# Stop SparkSession
spark.stop()
