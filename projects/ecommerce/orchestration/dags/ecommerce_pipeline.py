from datetime import datetime, timedelta
import glob
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator
import yaml

_CONFIG_PATH = Path(
    os.environ.get(
        "ECOMMERCE_CONFIG_PATH",
        Path(__file__).resolve().parent.parent.parent
        / "streaming_layer"
        / "config"
        / "ecommerce_config.yml",
    )
)

_FALLBACK_SINKS = [
    "url_counts",
    "user_counts",
    "url_conversion",
    "category_sales",
    "product_sales",
    "cart_metrics",
    "session_funnels",
    "top_urls_per_user",
]

_PIPELINE_PYTHON = "PIPELINE_PY=$( [ -x /opt/pipeline-venv/bin/python ] && echo /opt/pipeline-venv/bin/python || echo python )"
_PIPELINE_DBT = "PIPELINE_DBT=$( [ -x /opt/pipeline-venv/bin/dbt ] && echo /opt/pipeline-venv/bin/dbt || echo dbt )"


def _load_sink_names(config_path: Path | None = None) -> list[str]:
    """Loads sink table names from ecommerce_config.yml with a safe fallback list."""
    path = config_path or _CONFIG_PATH
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        sinks = config.get("sinks", [])
        if sinks:
            return sinks
    except (OSError, yaml.YAMLError):
        pass
    return _FALLBACK_SINKS


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
        self.targets = _load_sink_names()

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
    description="Refreshes OLTP dimensions, DuckDB marts, GX quality checks, and optional GCP sync",
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

    sync_postgres_dimensions = BashOperator(
        task_id="sync_postgres_dimensions",
        bash_command=(
            f"{_PIPELINE_PYTHON} && "
            "$PIPELINE_PY /opt/project/oltp_sync/sync_dimensions.py "
            "--output-dir ${CLICK_STREAM_OUTPUT_PREFIX:-/opt/project/output_data} "
            "--seed-from-defaults-on-offline"
        ),
    )

    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=(
            f"{_PIPELINE_DBT} && "
            "cd ${DBT_PROJ_DIR:-/opt/project/batch_layer} && "
            "$PIPELINE_DBT build -s 'path:models/staging/**'"
        ),
    )

    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=(
            f"{_PIPELINE_DBT} && "
            "cd ${DBT_PROJ_DIR:-/opt/project/batch_layer} && "
            "$PIPELINE_DBT build -s 'path:models/marts/**'"
        ),
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=(
            f"{_PIPELINE_DBT} && "
            "cd ${DBT_PROJ_DIR:-/opt/project/batch_layer} && "
            "$PIPELINE_DBT test"
        ),
    )

    run_great_expectations = BashOperator(
        task_id="run_great_expectations",
        bash_command=(
            f"{_PIPELINE_PYTHON} && "
            "$PIPELINE_PY /opt/project/quality/validate_ecommerce.py --fail-on-error"
        ),
    )

    sync_cloud_warehouse = BashOperator(
        task_id="sync_cloud_warehouse",
        bash_command=(
            f"{_PIPELINE_PYTHON} && "
            "$PIPELINE_PY /opt/project/cloud_sync/export_to_gcp.py"
        ),
    )

    (
        wait_for_stream
        >> sync_postgres_dimensions
        >> dbt_build_staging
        >> dbt_build_marts
        >> dbt_test_all
        >> run_great_expectations
        >> sync_cloud_warehouse
    )
