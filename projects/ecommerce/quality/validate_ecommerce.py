import argparse
from datetime import datetime, timezone
import json
import os
import sys

import great_expectations as gx
import pandas as pd

from projects.common.logger import get_logger
from projects.ecommerce.duckdb_utils import connect_duckdb_readonly

logger = get_logger("EcommerceGXValidator")


def _extract_suite_results(suite_name: str, validation_result) -> dict:
    """Normalizes a Great Expectations ValidationResult into a serializable summary."""
    checks = []
    for res in validation_result.results:
        exp_type = (
            res.expectation_config.type
            if res.expectation_config
            else "UnknownExpectation"
        )
        exp_col = (
            res.expectation_config.kwargs.get("column", "N/A")
            if res.expectation_config
            else "N/A"
        )
        unexpected_count = res.result.get("unexpected_count", 0) if res.result else 0
        checks.append(
            {
                "expectation_type": exp_type,
                "column": exp_col,
                "success": bool(res.success),
                "unexpected_count": int(unexpected_count or 0),
            }
        )

    passed = sum(1 for c in checks if c["success"])
    return {
        "suite_name": suite_name,
        "success": bool(validation_result.success),
        "passed_count": passed,
        "total_count": len(checks),
        "checks": checks,
    }


def _validate_dataframe(
    context,
    source_name: str,
    suite_name: str,
    df: pd.DataFrame,
    expectations: list,
) -> dict:
    """Runs an Expectation list against a Pandas DataFrame in an ephemeral GX context."""
    data_source = context.data_sources.add_pandas(name=source_name)
    data_asset = data_source.add_dataframe_asset(name=f"{source_name}_asset")
    batch_def = data_asset.add_batch_definition_whole_dataframe(
        f"{source_name}_batch_def"
    )
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for exp in expectations:
        suite.add_expectation(exp)

    result = batch.validate(suite)
    summary = _extract_suite_results(suite_name, result)
    logger.info(
        "Suite '%s': success=%s (%d/%d checks passed)",
        suite_name,
        summary["success"],
        summary["passed_count"],
        summary["total_count"],
    )
    return summary


def validate_ecommerce_pipeline(
    events_json_path: str,
    duckdb_path: str,
    output_report_path: str,
) -> bool:
    """Validates raw e-commerce events and available DuckDB Gold marts with Great Expectations."""
    context = gx.get_context(mode="ephemeral")
    suites_report: list[dict] = []

    # 1. Raw Event Feed Validation
    if not os.path.exists(events_json_path):
        raise FileNotFoundError(f"Events JSON file not found: {events_json_path}")

    df_events = pd.read_json(events_json_path, lines=True)
    logger.info("Loaded %d raw e-commerce events for GX validation.", len(df_events))

    events_expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="url"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="event_type"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="event_time"),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="event_type", value_set=["view", "add_to_cart", "purchase"]
        ),
    ]
    suites_report.append(
        _validate_dataframe(
            context,
            "raw_events_src",
            "ecommerce_raw_events_suite",
            df_events,
            events_expectations,
        )
    )

    # 2. Gold Warehouse DuckDB Marts Validation (when DuckDB exists)
    if os.path.exists(duckdb_path):
        with connect_duckdb_readonly(duckdb_path) as conn:
            existing_tables = {
                row[0]
                for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
            }

            if "daily_category_sales" in existing_tables:
                df_cat = conn.execute("SELECT * FROM daily_category_sales").fetchdf()
                if not df_cat.empty:
                    suites_report.append(
                        _validate_dataframe(
                            context,
                            "gold_category_sales_src",
                            "gold_daily_category_sales_suite",
                            df_cat,
                            [
                                gx.expectations.ExpectColumnValuesToNotBeNull(
                                    column="category"
                                ),
                                gx.expectations.ExpectColumnValuesToBeBetween(
                                    column="daily_revenue", min_value=0
                                ),
                                gx.expectations.ExpectColumnValuesToBeBetween(
                                    column="daily_units", min_value=0
                                ),
                            ],
                        )
                    )

            if "daily_url_conversion" in existing_tables:
                df_conv = conn.execute("SELECT * FROM daily_url_conversion").fetchdf()
                if not df_conv.empty:
                    suites_report.append(
                        _validate_dataframe(
                            context,
                            "gold_url_conversion_src",
                            "gold_daily_url_conversion_suite",
                            df_conv,
                            [
                                gx.expectations.ExpectColumnValuesToNotBeNull(
                                    column="url"
                                ),
                                gx.expectations.ExpectColumnValuesToBeBetween(
                                    column="avg_conversion_rate",
                                    min_value=0.0,
                                    max_value=1.0,
                                ),
                            ],
                        )
                    )

            if "customer_segment_spending" in existing_tables:
                df_seg = conn.execute(
                    "SELECT * FROM customer_segment_spending"
                ).fetchdf()
                if not df_seg.empty:
                    suites_report.append(
                        _validate_dataframe(
                            context,
                            "gold_customer_segment_src",
                            "gold_customer_segment_spending_suite",
                            df_seg,
                            [
                                gx.expectations.ExpectColumnValuesToNotBeNull(
                                    column="user_id"
                                ),
                                gx.expectations.ExpectColumnValuesToNotBeNull(
                                    column="segment"
                                ),
                                gx.expectations.ExpectColumnValuesToBeBetween(
                                    column="total_revenue", min_value=0
                                ),
                            ],
                        )
                    )
    else:
        logger.info(
            "DuckDB database not found at %s; skipping Gold mart suites.", duckdb_path
        )

    overall_success = all(s["success"] for s in suites_report)
    report = {
        "engine": "great_expectations",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_success": overall_success,
        "suites": suites_report,
    }

    os.makedirs(os.path.dirname(os.path.abspath(output_report_path)), exist_ok=True)
    with open(output_report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    logger.info("Saved Great Expectations report to %s", output_report_path)

    return overall_success


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    packaged_events = os.path.normpath(
        os.path.join(script_dir, "../streaming_layer/input_data/ecommerce_events.json")
    )
    workspace_events = (
        "projects/ecommerce/streaming_layer/input_data/ecommerce_events.json"
    )

    default_events = os.getenv(
        "ECOMMERCE_EVENTS_PATH",
        workspace_events if os.path.exists(workspace_events) else packaged_events,
    )
    default_duckdb = os.getenv(
        "DUCKDB_PATH",
        "projects/ecommerce/batch_layer/data/ecommerce.duckdb",
    )
    default_output = os.path.join(
        os.getenv("CLICK_STREAM_OUTPUT_PREFIX", "projects/ecommerce/output_data"),
        "quality_reports/gx_validation_results.json",
    )

    parser = argparse.ArgumentParser(
        description="Validate e-commerce raw events and DuckDB marts with Great Expectations."
    )
    parser.add_argument("--events-json", default=default_events)
    parser.add_argument("--duckdb-path", default=default_duckdb)
    parser.add_argument("--output-report", default=default_output)
    parser.add_argument("--fail-on-error", action="store_true")
    args = parser.parse_args()

    try:
        is_valid = validate_ecommerce_pipeline(
            args.events_json, args.duckdb_path, args.output_report
        )
        if not is_valid and args.fail_on_error:
            logger.error("Great Expectations validation failed.")
            sys.exit(1)
    except Exception as exc:
        logger.error("Validation error: %s", exc, exc_info=True)
        sys.exit(1)
