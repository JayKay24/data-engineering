"""Great Expectations validation gate for the e-commerce data pipeline.

Validates live ingested Silver Delta events (with optional static JSON fallback
for offline runs), quarantine DLQ ratios, and DuckDB Gold marts, emitting
structured JSON and Parquet observability reports.
"""

import argparse
from datetime import datetime, timezone
import json
import os
import sys
import tempfile
from typing import Any

import duckdb
import great_expectations as gx
import pandas as pd
import pyarrow  # noqa: F401 - ensures pyarrow engine is registered for Parquet export

from projects.common.logger import get_logger
from projects.ecommerce.duckdb_utils import connect_duckdb_readonly

logger = get_logger("EcommerceGXValidator")


def _extract_suite_results(suite_name: str, validation_result: Any) -> dict[str, Any]:
    """Normalize a Great Expectations ValidationResult into a JSON-serializable summary.

    Args:
        suite_name: Identifier of the ExpectationSuite that was executed.
        validation_result: Raw validation result object returned by
            ``batch.validate(...)``.

    Returns:
        A dictionary containing the suite name, overall ``success`` flag,
        ``passed_count``, ``total_count``, and a per-expectation ``checks`` list.
    """
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
    context: Any,
    source_name: str,
    suite_name: str,
    df: pd.DataFrame,
    expectations: list[Any],
) -> dict[str, Any]:
    """Run an Expectation list against a Pandas DataFrame in an ephemeral GX context.

    Args:
        context: Active Great Expectations DataContext (ephemeral mode).
        source_name: Unique name for the Pandas data source and derived asset/batch.
        suite_name: Name assigned to the ExpectationSuite in the GX context.
        df: In-memory Pandas DataFrame to validate.
        expectations: List of Great Expectations ``Expectation`` instances to
            evaluate against ``df``.

    Returns:
        Normalized suite summary dictionary produced by
        ``_extract_suite_results``.
    """
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


def _read_delta_via_duckdb(table_path: str) -> pd.DataFrame:
    """Load a Delta Lake table into a DataFrame via DuckDB's native delta extension."""
    with duckdb.connect() as conn:
        if not os.environ.get("HOME"):
            conn.execute(f"SET home_directory='{tempfile.gettempdir()}';")
        conn.execute("INSTALL delta; LOAD delta;")
        return conn.execute("SELECT * FROM delta_scan(?)", [table_path]).fetchdf()


def validate_ecommerce_pipeline(
    events_json_path: str,
    duckdb_path: str,
    output_report_path: str,
    output_prefix: str | None = None,
    fallback_json: bool = False,
    max_quarantine_ratio: float = 0.5,
) -> bool:
    """Validate live ingested Silver events, DLQ ratios, and DuckDB Gold marts.

    Reads the live ``silver_ecommerce_events`` Delta table via DuckDB
    ``delta_scan()`` when present (falling back to ``events_json_path`` when
    ``fallback_json=True`` or when invoked without a Silver Delta table), checks
    quarantine DLQ ratios, and validates Gold marts in ``duckdb_path``.

    Args:
        events_json_path: Fallback path to the newline-delimited JSON events file.
        duckdb_path: Path to the DuckDB warehouse file containing Gold marts.
        output_report_path: Destination file path for the JSON validation report.
        output_prefix: Root output directory containing Delta Lake tables
            (defaults to the parent of ``quality_reports/``).
        fallback_json: If ``True``, falls back to ``events_json_path`` when
            ``silver_ecommerce_events`` is missing.
        max_quarantine_ratio: Maximum allowed fraction of quarantined rows
            relative to total ingested rows.

    Returns:
        ``True`` if every executed ExpectationSuite and quarantine gate
        succeeded; ``False`` otherwise.

    Raises:
        FileNotFoundError: If neither live Silver Delta nor ``events_json_path``
            is available.
    """
    context = gx.get_context(mode="ephemeral")
    suites_report: list[dict[str, Any]] = []

    report_dir = os.path.dirname(os.path.abspath(output_report_path))
    resolved_prefix = output_prefix or os.path.dirname(report_dir)
    silver_dir = os.path.join(resolved_prefix, "silver_ecommerce_events")
    quarantine_dir = os.path.join(resolved_prefix, "quarantine_events")

    # 1. Live Silver Event Validation (fallback to static JSON only when Silver absent)
    if os.path.exists(os.path.join(silver_dir, "_delta_log")):
        df_events = _read_delta_via_duckdb(silver_dir)
        source_label = f"silver_delta ({silver_dir})"
        suite_name = "ecommerce_silver_events_suite"
    elif fallback_json or os.path.exists(events_json_path):
        if not os.path.exists(events_json_path):
            raise FileNotFoundError(
                f"Neither Silver Delta ({silver_dir}) nor fallback JSON ({events_json_path}) found."
            )
        logger.warning(
            "Live Silver Delta not found at %s; falling back to static JSON %s.",
            silver_dir,
            events_json_path,
        )
        df_events = pd.read_json(events_json_path, lines=True)
        source_label = f"fallback_json ({events_json_path})"
        suite_name = "ecommerce_raw_events_suite"
    else:
        raise FileNotFoundError(
            f"Silver Delta table not found at {silver_dir} and no fallback file exists."
        )

    logger.info(
        "Loaded %d events from %s for GX validation.", len(df_events), source_label
    )

    events_expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="url"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="event_type"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="event_time"),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="event_type", value_set=["view", "add_to_cart", "purchase"]
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0.0),
    ]
    suites_report.append(
        _validate_dataframe(
            context,
            "silver_events_src",
            suite_name,
            df_events,
            events_expectations,
        )
    )

    # 2. Quarantine DLQ Ratio Check (when quarantine Delta table exists)
    quarantine_count = 0
    if os.path.exists(os.path.join(quarantine_dir, "_delta_log")):
        df_quarantine = _read_delta_via_duckdb(quarantine_dir)
        quarantine_count = len(df_quarantine)
        total_ingested = len(df_events) + quarantine_count
        ratio = quarantine_count / total_ingested if total_ingested > 0 else 0.0
        ratio_ok = ratio <= max_quarantine_ratio
        logger.info(
            "Quarantine DLQ check: %d/%d rows quarantined (ratio=%.3f, max=%.3f, ok=%s)",
            quarantine_count,
            total_ingested,
            ratio,
            max_quarantine_ratio,
            ratio_ok,
        )
        suites_report.append(
            {
                "suite_name": "quarantine_dlq_threshold_suite",
                "success": bool(ratio_ok),
                "passed_count": 1 if ratio_ok else 0,
                "total_count": 1,
                "checks": [
                    {
                        "expectation_type": "expect_quarantine_ratio_within_threshold",
                        "column": "quarantine_reason",
                        "success": bool(ratio_ok),
                        "unexpected_count": 0 if ratio_ok else int(quarantine_count),
                    }
                ],
            }
        )

    # 3. Gold Warehouse DuckDB Marts Validation (skipped gracefully if not yet built)
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

            if "brand_category_performance" in existing_tables:
                df_brand = conn.execute(
                    "SELECT * FROM brand_category_performance"
                ).fetchdf()
                if not df_brand.empty:
                    suites_report.append(
                        _validate_dataframe(
                            context,
                            "gold_brand_category_src",
                            "gold_brand_category_performance_suite",
                            df_brand,
                            [
                                gx.expectations.ExpectColumnValuesToNotBeNull(
                                    column="brand"
                                ),
                                gx.expectations.ExpectColumnValuesToNotBeNull(
                                    column="category"
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
        "source": source_label,
        "quarantine_count": int(quarantine_count),
        "overall_success": overall_success,
        "suites": suites_report,
    }

    os.makedirs(report_dir, exist_ok=True)
    with open(output_report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    logger.info("Saved Great Expectations report to %s", output_report_path)

    # Emit a flat Parquet check table alongside JSON so dbt/DuckDB and Superset can query it directly.
    flat_gx_rows = [
        {
            "engine": "great_expectations",
            "generated_at": report["generated_at"],
            "overall_success": bool(overall_success),
            "suite_name": suite["suite_name"],
            "suite_success": bool(suite["success"]),
            "expectation_type": check["expectation_type"],
            "column_name": check["column"],
            "check_success": bool(check["success"]),
            "unexpected_count": int(check["unexpected_count"]),
        }
        for suite in suites_report
        for check in suite["checks"]
    ]
    gx_parquet_path = os.path.join(report_dir, "gx_checks.parquet")
    pd.DataFrame(flat_gx_rows).to_parquet(
        gx_parquet_path, index=False, engine="pyarrow"
    )
    logger.info("Saved Great Expectations Parquet checks to %s", gx_parquet_path)

    # Ensure pydeequ_checks.parquet exists with valid schema so dbt monitor tables never fail.
    pydeequ_dir = os.path.join(resolved_prefix, "pydeequ_metrics")
    pydeequ_parquet_path = os.path.join(pydeequ_dir, "pydeequ_checks.parquet")
    if not os.path.exists(pydeequ_parquet_path):
        os.makedirs(pydeequ_dir, exist_ok=True)
        empty_pydeequ_df = pd.DataFrame(
            {
                "engine": pd.Series(dtype="string"),
                "generated_at": pd.Series(dtype="string"),
                "row_count": pd.Series(dtype="int64"),
                "overall_status": pd.Series(dtype="string"),
                "check_name": pd.Series(dtype="string"),
                "check_level": pd.Series(dtype="string"),
                "constraint": pd.Series(dtype="string"),
                "constraint_status": pd.Series(dtype="string"),
                "constraint_message": pd.Series(dtype="string"),
            }
        )
        empty_pydeequ_df.to_parquet(pydeequ_parquet_path, index=False, engine="pyarrow")

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
    default_prefix = os.getenv(
        "CLICK_STREAM_OUTPUT_PREFIX", "projects/ecommerce/output_data"
    )
    default_output = os.path.join(
        default_prefix,
        "quality_reports/gx_validation_results.json",
    )

    parser = argparse.ArgumentParser(
        description="Validate live Silver Delta events and DuckDB Gold marts with Great Expectations."
    )
    parser.add_argument("--events-json", default=default_events)
    parser.add_argument("--duckdb-path", default=default_duckdb)
    parser.add_argument("--output-prefix", default=default_prefix)
    parser.add_argument("--output-report", default=default_output)
    parser.add_argument("--fallback-json", action="store_true")
    parser.add_argument("--max-quarantine-ratio", type=float, default=0.5)
    parser.add_argument("--fail-on-error", action="store_true")
    args = parser.parse_args()

    try:
        is_valid = validate_ecommerce_pipeline(
            args.events_json,
            args.duckdb_path,
            args.output_report,
            output_prefix=args.output_prefix,
            fallback_json=args.fallback_json,
            max_quarantine_ratio=args.max_quarantine_ratio,
        )
        if not is_valid and args.fail_on_error:
            logger.error("Great Expectations validation failed.")
            sys.exit(1)
    except Exception as exc:
        logger.error("Validation error: %s", exc, exc_info=True)
        sys.exit(1)
