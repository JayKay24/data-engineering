import os
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def write_streaming_sinks(
    streams_dict: dict[str, DataFrame],
    sink_type: str,
    output_mode: str,
    trigger_time: str,
    output_base_dir: str,
    checkpoint_base_dir: str,
) -> list[StreamingQuery]:
    """Attaches streaming sinks (console or Delta Lake) to aggregated DataFrames."""
    os.makedirs(output_base_dir, exist_ok=True)
    os.makedirs(checkpoint_base_dir, exist_ok=True)
    active_queries = []

    for name, df in streams_dict.items():
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
    """Outputs batch DataFrames to console stdout or Delta Lake tables."""
    for name, df in streams_dict.items():
        if sink_type == "console":
            print(f"\n--- Batch Output: {name} ---")
            df.show(truncate=False)
        else:
            table_path = os.path.join(output_base_dir, name)
            df.write.format("delta").mode("overwrite").save(table_path)
