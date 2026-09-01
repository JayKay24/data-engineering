from datetime import datetime, timedelta
import glob
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator


class DeltaStreamSensor(BaseSensorOperator):
    """Sensor that checks whether streaming Delta/Parquet outputs have committed data."""

    def __init__(
        self,
        output_prefix: str = "/opt/project/output_data",
        min_ready_tables: int = 3,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.output_prefix = output_prefix
        self.min_ready_tables = min_ready_tables
        self.targets = [
            "url_counts",
            "user_counts",
            "url_conversion",
            "category_sales",
            "cart_metrics",
            "session_funnels",
            "top_urls_per_user",
        ]

    def poke(self, context) -> bool:
        prefix = os.environ.get("CLICK_STREAM_OUTPUT_PREFIX", self.output_prefix)
        ready_count = 0
        for table in self.targets:
            delta_log = os.path.join(prefix, table, "_delta_log", "*.json")
            parquet_files = os.path.join(prefix, table, "*.parquet")
            if glob.glob(delta_log) or glob.glob(parquet_files):
                ready_count += 1

        self.log.info(
            "DeltaStreamSensor checked %s: %d/%d tables ready (minimum required: %d)",
            prefix,
            ready_count,
            len(self.targets),
            self.min_ready_tables,
        )
        return ready_count >= self.min_ready_tables


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_batch_refresh",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    default_args=default_args,
    description="Refreshes DuckDB analytical marts from streaming Delta/Parquet outputs",
    tags=["ecommerce", "realtime", "batch"],
) as dag:
    wait_for_stream = DeltaStreamSensor(
        task_id="wait_for_stream_outputs",
        output_prefix="/opt/project/output_data",
        min_ready_tables=3,
        poke_interval=20,
        timeout=600,
        mode="reschedule",
    )

    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command="cd ${DBT_PROJ_DIR:-/opt/project/batch_layer} && dbt build -s 'path:models/staging/**'",
    )

    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command="cd ${DBT_PROJ_DIR:-/opt/project/batch_layer} && dbt build -s 'path:models/marts/**'",
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command="cd ${DBT_PROJ_DIR:-/opt/project/batch_layer} && dbt test",
    )

    wait_for_stream >> dbt_build_staging >> dbt_build_marts >> dbt_test_all
