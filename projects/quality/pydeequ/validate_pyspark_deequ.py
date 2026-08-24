import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from projects.common.logger import get_logger

# Configure environment variable before importing PyDeequ to target Spark 3.5
os.environ["SPARK_VERSION"] = "3.5"

import pydeequ  # noqa: E402
from pydeequ.checks import Check, CheckLevel  # noqa: E402
from pydeequ.verification import VerificationResult, VerificationSuite  # noqa: E402

logger = get_logger("PyDeequValidator")


def init_spark_deequ(app_name: str = "PyDeequValidation") -> SparkSession:
    """Initializes and returns a SparkSession configured with Amazon Deequ 2.0.7 for Spark 3.5."""
    logger.info("Initializing SparkSession with PyDeequ Maven package...")
    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            pydeequ.deequ_maven_coord,
        )
        .config(
            "spark.jars.excludes",
            pydeequ.f2j_maven_coord,
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_user_data_deequ(
    spark: SparkSession,
    input_csv_path: str,
    output_dir: str | None = None,
) -> DataFrame:
    """Executes data quality checks on user data using Amazon Deequ VerificationSuite.

    Checks:
      1. Dataset size must be > 0.
      2. 'user_id' must be complete (non-null).
      3. 'user_id' must be unique.
      4. 'email' must be complete (non-null).
      5. 'country' must be contained within allowed set ('US', 'IN', 'UK', 'FR', 'CA').
    """
    if not os.path.exists(input_csv_path):
        logger.error("Input CSV dataset not found at: %s", input_csv_path)
        raise FileNotFoundError(f"File not found: {input_csv_path}")

    logger.info("Loading CSV dataset into PySpark DataFrame from %s...", input_csv_path)
    df = spark.read.option("header", "true").csv(input_csv_path)
    row_count = df.count()
    logger.info("Loaded %d rows for PyDeequ verification.", row_count)

    logger.info("Constructing PyDeequ Check definitions...")
    check = (
        Check(spark, CheckLevel.Error, "UserDataQualityCheck")
        .hasSize(lambda x: x > 0)
        .isComplete("user_id")
        .isUnique("user_id")
        .isComplete("email")
        .isContainedIn("country", ["US", "IN", "UK", "FR", "CA"])
    )

    logger.info("Running VerificationSuite...")
    verification_suite = VerificationSuite(spark).onData(df).addCheck(check).run()

    result_df = VerificationResult.checkResultsAsDataFrame(spark, verification_suite)
    logger.info("Deequ Verification Results Preview:")
    result_df.show(truncate=False)

    if output_dir:
        logger.info("Saving verification results to %s...", output_dir)
        result_df.coalesce(1).write.mode("overwrite").json(output_dir)
        logger.info("Verification results persisted successfully!")

    return result_df


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_input = os.path.normpath(
        os.path.join(script_dir, "../data/raw/user_data.csv")
    )
    default_output = os.path.normpath(
        os.path.join(script_dir, "../output/pydeequ_metrics")
    )

    parser = argparse.ArgumentParser(
        description="Validate user dataset using PySpark and Amazon Deequ"
    )
    parser.add_argument(
        "--input-csv",
        type=str,
        default=default_input,
        help="Path to input CSV dataset",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=default_output,
        help="Directory to save output JSON verification metrics",
    )

    args = parser.parse_args()

    spark_session = None
    try:
        spark_session = init_spark_deequ()
        validate_user_data_deequ(spark_session, args.input_csv, args.output_dir)
    except Exception as e:
        logger.error("PyDeequ validation failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        if spark_session is not None:
            spark_session.stop()
        # Shutdown Py4J gateway to release JVM thread
        try:
            spark_session._jvm.py4j.GatewayServer.turnLoggingOff()
        except Exception:
            pass
        os._exit(0)
