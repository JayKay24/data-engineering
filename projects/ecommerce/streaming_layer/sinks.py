"""Streaming and batch sink adapters for Spark DataFrames (Delta Lake and console)."""

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from projects.common.logger import get_logger
from projects.ecommerce.streaming_layer.aggregations import (
    aggregate_cart_metrics,
    aggregate_category_sales,
    aggregate_product_sales,
    aggregate_session_funnels,
    aggregate_top_urls_per_user,
    aggregate_url_conversion,
    aggregate_url_counts,
    aggregate_user_activity,
    split_valid_and_quarantine_events,
)
from projects.ecommerce.streaming_layer.quality_checks import (
    run_pydeequ_verification,
)

logger = get_logger("EcommerceSinks")


def build_kpi_dataframes(
    df: DataFrame, win_cfg: dict[str, Any]
) -> dict[str, DataFrame]:
    """Build the full suite of 8 e-commerce KPI DataFrames from a parsed DataFrame.

    Args:
        df: Validated and parsed e-commerce event DataFrame (streaming or batch).
        win_cfg: Windowing configuration dictionary from ``ecommerce_config.yml``.

    Returns:
        Mapping from sink table name to aggregated Spark DataFrame.
    """
    ts_f = win_cfg.get("timestamp_field", "event_time")
    win_d = win_cfg.get("window_duration", "10 seconds")
    slide_d = win_cfg.get("slide_duration", "10 seconds")
    gap_sec = int(win_cfg.get("session_gap_seconds", 900))

    return {
        "url_counts": aggregate_url_counts(df, ts_f, win_d, slide_d),
        "user_counts": aggregate_user_activity(df, ts_f, win_d, slide_d),
        "url_conversion": aggregate_url_conversion(df, ts_f, win_d, slide_d),
        "category_sales": aggregate_category_sales(df, ts_f, win_d, slide_d),
        "product_sales": aggregate_product_sales(df, ts_f, win_d, slide_d),
        "cart_metrics": aggregate_cart_metrics(df, ts_f, win_d, slide_d),
        "top_urls_per_user": aggregate_top_urls_per_user(df, ts_f, win_d, slide_d),
        "session_funnels": aggregate_session_funnels(df, ts_f, gap_sec),
    }


def write_silver_and_quarantine_microbatch(
    spark: SparkSession,
    batch_df: DataFrame,
    batch_id: int | None,
    output_base_dir: str,
    enable_pydeequ: bool = True,
    silver_table: str = "silver_ecommerce_events",
    quarantine_table: str = "quarantine_events",
    timestamp_field: str = "event_time",
) -> dict[str, int]:
    """Persist a single micro-batch to Silver and Quarantine Delta tables with PyDeequ.

    Caches ``batch_df`` once so Kafka deserialization runs in a single pass across
    PyDeequ verification, valid Silver ingestion, and dead-letter quarantine routing.

    Args:
        spark: Active SparkSession.
        batch_df: Bounded micro-batch (or batch) DataFrame of parsed events.
        batch_id: Micro-batch identifier supplied by Spark ``foreachBatch``, or
            ``None`` in standalone batch runs.
        output_base_dir: Root directory where Delta Lake tables are written.
        enable_pydeequ: When ``True``, executes PyDeequ verification on the batch.
        silver_table: Directory name for the validated Silver Delta table.
        quarantine_table: Directory name for the Dead-Letter Quarantine Delta table.
        timestamp_field: Event timestamp column name.

    Returns:
        Dictionary with ``total_rows``, ``valid_rows``, and ``quarantine_rows``.
    """
    os.makedirs(output_base_dir, exist_ok=True)
    silver_path = os.path.join(output_base_dir, silver_table)
    quarantine_path = os.path.join(output_base_dir, quarantine_table)

    batch_df.persist()
    try:
        total_rows = batch_df.count()
        if total_rows == 0:
            # Ensure Delta logs exist on first run so DuckDB delta_scan() never errors on empty runs.
            _, empty_quarantine = split_valid_and_quarantine_events(
                batch_df, timestamp_field=timestamp_field
            )
            if not os.path.exists(os.path.join(quarantine_path, "_delta_log")):
                empty_quarantine.write.format("delta").mode("append").save(
                    quarantine_path
                )
            return {"total_rows": 0, "valid_rows": 0, "quarantine_rows": 0}

        if enable_pydeequ:
            run_pydeequ_verification(
                spark, batch_df, output_base_dir=output_base_dir, batch_id=batch_id
            )

        valid_df, quarantine_df = split_valid_and_quarantine_events(
            batch_df, timestamp_field=timestamp_field
        )
        valid_count = valid_df.count()
        quarantine_count = total_rows - valid_count

        if valid_count > 0 or not os.path.exists(
            os.path.join(silver_path, "_delta_log")
        ):
            (
                valid_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("event_date")
                .save(silver_path)
            )

        if quarantine_count > 0 or not os.path.exists(
            os.path.join(quarantine_path, "_delta_log")
        ):
            (
                quarantine_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(quarantine_path)
            )

        logger.info(
            "Micro-batch %s committed: total=%d, silver=%d, quarantine=%d",
            batch_id,
            total_rows,
            valid_count,
            quarantine_count,
        )
        return {
            "total_rows": total_rows,
            "valid_rows": valid_count,
            "quarantine_rows": quarantine_count,
        }
    finally:
        batch_df.unpersist()


def materialize_kpi_sinks_from_silver(
    spark: SparkSession,
    silver_path: str,
    sink_type: str,
    output_base_dir: str,
    win_cfg: dict[str, Any],
) -> None:
    """Recompute all 8 compacted KPI Delta sinks in a single cached pass over Silver.

    Args:
        spark: Active SparkSession.
        silver_path: Filesystem path to the ``silver_ecommerce_events`` Delta table.
        sink_type: Target sink backend (``"delta"`` or ``"console"``).
        output_base_dir: Root directory where KPI Delta tables are written.
        win_cfg: Windowing configuration dictionary.
    """
    if not os.path.exists(os.path.join(silver_path, "_delta_log")):
        logger.warning(
            "Silver Delta table not found at %s; skipping KPI materialization.",
            silver_path,
        )
        return

    silver_df = spark.read.format("delta").load(silver_path).cache()
    try:
        if silver_df.isEmpty():
            logger.info("Silver Delta table is empty; skipping KPI materialization.")
            return
        kpi_streams = build_kpi_dataframes(silver_df, win_cfg)
        write_batch_sinks(kpi_streams, sink_type, output_base_dir)
    finally:
        silver_df.unpersist()


def write_streaming_sinks(
    streams_dict: dict[str, DataFrame],
    sink_type: str,
    output_mode: str,
    trigger_time: str,
    output_base_dir: str,
    checkpoint_base_dir: str,
) -> list[StreamingQuery]:
    """Attach streaming sinks (console or Delta Lake) to aggregated DataFrames.

    Args:
        streams_dict: Mapping of stream table names to Spark streaming DataFrames.
        sink_type: Target sink backend (``"console"`` or ``"delta"``).
        output_mode: Spark Structured Streaming output mode (e.g., ``"append"``).
        trigger_time: Micro-batch processing trigger interval (e.g., ``"30 seconds"``).
        output_base_dir: Root directory for Delta Lake table outputs.
        checkpoint_base_dir: Root directory for Spark streaming checkpoints.

    Returns:
        List of started ``StreamingQuery`` handles.
    """
    os.makedirs(output_base_dir, exist_ok=True)
    os.makedirs(checkpoint_base_dir, exist_ok=True)
    active_queries = []

    for name, df in streams_dict.items():
        # Each StreamingQuery must use an isolated checkpoint directory to prevent offset/state collisions.
        if sink_type == "console":
            query = (
                df.writeStream.outputMode(output_mode)
                .format("console")
                .option("truncate", "false")
                .option(
                    "checkpointLocation",
                    os.path.join(checkpoint_base_dir, f"console_{name}"),
                )
                .trigger(processingTime=trigger_time)
                .queryName(f"{name}_console")
                .start()
            )
        else:
            table_path = os.path.join(output_base_dir, name)
            checkpoint_path = os.path.join(checkpoint_base_dir, f"delta_{name}")
            query = (
                df.writeStream.outputMode(output_mode)
                .format("delta")
                .option("path", table_path)
                .option("checkpointLocation", checkpoint_path)
                .trigger(processingTime=trigger_time)
                .queryName(f"{name}_delta")
                .start()
            )
        active_queries.append(query)

    return active_queries


def write_batch_sinks(
    streams_dict: dict[str, DataFrame],
    sink_type: str,
    output_base_dir: str,
) -> None:
    """Write bounded batch DataFrames to console stdout or Delta Lake tables.

    Args:
        streams_dict: Mapping of table names to bounded Spark DataFrames.
        sink_type: Target sink backend (``"console"`` or ``"delta"``).
        output_base_dir: Root directory where Delta Lake tables are overwritten.
    """
    for name, df in streams_dict.items():
        if sink_type == "console":
            print(f"\n--- Batch Output: {name} ---")
            df.show(truncate=False)
        else:
            table_path = os.path.join(output_base_dir, name)
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(table_path)
            )
