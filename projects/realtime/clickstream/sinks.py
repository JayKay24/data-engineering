import os
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def write_streaming_sinks(
    url_counts_df: DataFrame,
    user_counts_df: DataFrame,
    sink_type: str,
    output_mode: str,
    trigger_time: str,
    url_path: str,
    user_path: str,
    checkpoint_base: str,
) -> list[StreamingQuery]:
    """Attaches streaming sinks (console or Delta Lake) to aggregated DataFrames."""
    if sink_type == "console":
        q1 = (
            url_counts_df.writeStream.outputMode(output_mode)
            .format("console")
            .option("truncate", "false")
            .option(
                "checkpointLocation",
                os.path.join(checkpoint_base, "console_urls"),
            )
            .trigger(processingTime=trigger_time)
            .queryName("url_counts_console")
            .start()
        )
        q2 = (
            user_counts_df.writeStream.outputMode(output_mode)
            .format("console")
            .option("truncate", "false")
            .option(
                "checkpointLocation",
                os.path.join(checkpoint_base, "console_users"),
            )
            .trigger(processingTime=trigger_time)
            .queryName("user_counts_console")
            .start()
        )
        return [q1, q2]

    # Delta Lake Sinks
    q1 = (
        url_counts_df.writeStream.outputMode(output_mode)
        .format("delta")
        .option("path", url_path)
        .option(
            "checkpointLocation",
            os.path.join(checkpoint_base, "delta_urls"),
        )
        .trigger(processingTime=trigger_time)
        .queryName("url_counts_delta")
        .start()
    )
    q2 = (
        user_counts_df.writeStream.outputMode(output_mode)
        .format("delta")
        .option("path", user_path)
        .option(
            "checkpointLocation",
            os.path.join(checkpoint_base, "delta_users"),
        )
        .trigger(processingTime=trigger_time)
        .queryName("user_counts_delta")
        .start()
    )
    return [q1, q2]


def write_batch_sinks(
    url_counts_df: DataFrame,
    user_counts_df: DataFrame,
    sink_type: str,
    url_path: str,
    user_path: str,
) -> None:
    """Outputs batch DataFrames to console stdout or Delta Lake tables."""
    if sink_type == "console":
        url_counts_df.show(truncate=False)
        user_counts_df.show(truncate=False)
    else:
        url_counts_df.write.format("delta").mode("overwrite").save(url_path)
        user_counts_df.write.format("delta").mode("overwrite").save(user_path)
