import argparse
from datetime import datetime, timezone
import json
import os
import sys

# Configure Spark version before importing PyDeequ
os.environ.setdefault("SPARK_VERSION", "3.5")

import pydeequ  # noqa: E402
from pydeequ.checks import Check, CheckLevel  # noqa: E402
from pydeequ.verification import VerificationResult, VerificationSuite  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from projects.common.logger import get_logger  # noqa: E402

logger = get_logger("EcommercePyDeequ")


def run_pydeequ_verification(
    spark: SparkSession,
    df: DataFrame,
    output_base_dir: str = "projects/ecommerce/output_data",
) -> dict:
    """Runs Amazon Deequ data quality verification on a bounded e-commerce DataFrame.

    Validates schema completeness, allowed event types, and non-negative pricing,
    and persists a JSON summary report for downstream observability and Streamlit.
    """
    if df.isStreaming:
        logger.warning(
            "Skipping PyDeequ verification because the input DataFrame is unbounded streaming."
        )
        return {"status": "SKIPPED", "reason": "streaming_dataframe", "checks": []}

    row_count = df.count()
    logger.info("Running PyDeequ VerificationSuite on %d event rows...", row_count)

    check = (
        Check(spark, CheckLevel.Error, "EcommerceEventsQualityCheck")
        .hasSize(lambda x: x > 0)
        .isComplete("user_id")
        .isComplete("url")
        .isComplete("event_type")
        .isComplete("event_time")
        .isContainedIn("event_type", ["view", "add_to_cart", "purchase"])
        .isNonNegative("price")
    )

    verification_result = VerificationSuite(spark).onData(df).addCheck(check).run()
    result_df = VerificationResult.checkResultsAsDataFrame(spark, verification_result)

    rows = [row.asDict() for row in result_df.collect()]
    overall_status = (
        "Success"
        if rows and all(r.get("constraint_status") == "Success" for r in rows)
        else "Failure"
    )

    report = {
        "engine": "pydeequ",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": row_count,
        "overall_status": overall_status,
        "checks": rows,
    }

    metrics_dir = os.path.join(output_base_dir, "pydeequ_metrics")
    os.makedirs(metrics_dir, exist_ok=True)
    report_path = os.path.join(metrics_dir, "latest_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(
        "PyDeequ verification finished with status=%s (%d constraints). Report: %s",
        overall_status,
        len(rows),
        report_path,
    )
    return report


def _run_standalone(input_json: str, output_base_dir: str) -> None:
    """Initializes a SparkSession with Deequ and validates the raw JSON event feed."""
    spark = (
        SparkSession.builder.appName("EcommercePyDeequCheck")
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = spark.read.json(input_json)
        report = run_pydeequ_verification(spark, df, output_base_dir)
        if report["overall_status"] != "Success":
            logger.error("PyDeequ quality check failed: %s", report)
            sys.exit(1)
    finally:
        spark.stop()
        os._exit(0)


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Run PyDeequ quality checks on e-commerce events."
    )
    parser.add_argument(
        "--input",
        default=os.path.join(script_dir, "input_data/ecommerce_events.json"),
        help="Path to raw JSON lines e-commerce events file.",
    )
    parser.add_argument(
        "--output-dir",
        default="projects/ecommerce/output_data",
        help="Base output directory for pydeequ_metrics report.",
    )
    args = parser.parse_args()
    _run_standalone(args.input, args.output_dir)
