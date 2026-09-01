from datetime import datetime, timedelta
import glob
import os
import time
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def wait_for_stream_outputs(min_dirs: int = 3, timeout_min: int = 15) -> bool:
    """Waits until at least `min_dirs` streaming directories contain valid parquet and delta commits."""
    click_prefix = os.environ.get(
        "CLICK_STREAM_OUTPUT_PREFIX", "/opt/project/output_data"
    )
    start = time.time()
    targets = [
        "url_counts",
        "user_counts",
        "url_conversion",
        "category_sales",
        "cart_metrics",
        "session_funnels",
        "top_urls_per_user",
    ]
    while True:
        ready = 0
        for t in targets:
            delta_log_path = os.path.join(click_prefix, t, "_delta_log", "*.json")
            parquet_path = os.path.join(click_prefix, t, "*.parquet")
            if glob.glob(delta_log_path) or glob.glob(parquet_path):
                ready += 1
        if ready >= min_dirs:
            return True
        if time.time() - start > timeout_min * 60:
            raise TimeoutError(
                f"Timed out waiting for streaming parquet files in {click_prefix}"
            )
        time.sleep(10)


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
    wait_for_stream = PythonOperator(
        task_id="wait_for_stream_outputs",
        python_callable=wait_for_stream_outputs,
        op_kwargs={"min_dirs": 3, "timeout_min": 15},
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
