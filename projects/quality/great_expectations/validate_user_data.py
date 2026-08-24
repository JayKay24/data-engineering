import argparse
import json
import os
import sys
import great_expectations as gx
import pandas as pd
from projects.common.logger import get_logger

logger = get_logger("GXValidator")


def validate_user_data(
    input_csv_path: str,
    output_report_path: str | None = None,
) -> bool:
    """Validates user data CSV file using Great Expectations Fluent API.

    Executes data quality assertions:
      1. 'email' column values must not be null.
      2. 'signup_date' must match timestamp format '%Y-%m-%d %H:%M:%S'.
      3. 'country' must be in allowed set ('US', 'IN', 'UK', 'FR', 'CA').
      4. 'user_id' must be unique.

    Args:
        input_csv_path: Path to the input CSV file.
        output_report_path: Optional path to persist JSON validation results.

    Returns:
        bool: True if all expectations passed, False if any expectation failed.
    """
    if not os.path.exists(input_csv_path):
        logger.error("Input data file not found at: %s", input_csv_path)
        raise FileNotFoundError(f"File not found: {input_csv_path}")

    logger.info("Reading input dataset from %s...", input_csv_path)
    df = pd.read_csv(input_csv_path)
    logger.info("Loaded %d rows for validation.", len(df))

    # Initialize Ephemeral GX Context (in-memory)
    context = gx.get_context(mode="ephemeral")

    # Connect to Pandas Data Source
    data_source_name = "pandas_user_datasource"
    data_asset_name = "user_data_asset"
    data_source = context.data_sources.add_pandas(name=data_source_name)
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)

    # Build Batch Request
    batch_definition_name = "user_data_batch_def"
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        batch_definition_name
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # Define Expectation Suite
    suite_name = "user_data_quality_suite"
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="email"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
            column="signup_date", strftime_format="%Y-%m-%d %H:%M:%S"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="country", value_set=["US", "IN", "UK", "FR", "CA"]
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="user_id")
    )

    logger.info("Executing Great Expectations validation suite '%s'...", suite_name)
    validation_result = batch.validate(suite)

    success = validation_result.success
    total_expectations = len(validation_result.results)
    successful_expectations = sum(1 for r in validation_result.results if r.success)

    logger.info(
        "Validation completed. Overall Success: %s (%d/%d expectations passed)",
        success,
        successful_expectations,
        total_expectations,
    )

    for r in validation_result.results:
        exp_type = (
            r.expectation_config.type if r.expectation_config else "UnknownExpectation"
        )
        exp_col = (
            r.expectation_config.kwargs.get("column", "N/A")
            if r.expectation_config
            else "N/A"
        )
        status_icon = "PASSED" if r.success else "FAILED"
        unexpected_count = r.result.get("unexpected_count", 0) if r.result else 0
        logger.info(
            "  [%s] %s on column '%s' (Unexpected values: %d)",
            status_icon,
            exp_type,
            exp_col,
            unexpected_count,
        )

    # Persist JSON report if requested
    if output_report_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_report_path)), exist_ok=True)
        report_dict = validation_result.to_json_dict()
        with open(output_report_path, "w", encoding="utf-8") as f:
            json.dump(report_dict, f, indent=2)
        logger.info("Validation report saved to: %s", output_report_path)

    return success


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_input = os.path.normpath(
        os.path.join(script_dir, "../data/raw/user_data.csv")
    )
    default_output = os.path.normpath(
        os.path.join(script_dir, "../output/gx_validation_results.json")
    )

    parser = argparse.ArgumentParser(
        description="Validate user dataset using Great Expectations Fluent API"
    )
    parser.add_argument(
        "--input-csv",
        type=str,
        default=default_input,
        help="Path to input CSV dataset",
    )
    parser.add_argument(
        "--output-report",
        type=str,
        default=default_output,
        help="Path to save output JSON validation report",
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Exit with non-zero exit code (1) if validation expectations fail (ideal for CI/CD pipelines)",
    )

    args = parser.parse_args()

    try:
        is_valid = validate_user_data(args.input_csv, args.output_report)
        if not is_valid:
            if args.fail_on_error:
                logger.error(
                    "Data quality validation failed on one or more expectations. Failing job due to --fail-on-error."
                )
                sys.exit(1)
            else:
                logger.warning(
                    "Data quality validation failed on one or more expectations (expected for sample dataset with intentional edge cases)."
                )
    except Exception as e:
        logger.error(
            "Great Expectations validation failed with error: %s", e, exc_info=True
        )
        sys.exit(1)
