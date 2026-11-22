"""Airflow DAG orchestrating the e-commerce Medallion Lakehouse & WAP pipeline."""

from datetime import datetime, timedelta
import glob
import os
from pathlib import Path
import shutil
import sys

from airflow import DAG  # pants: no-infer-dep
from airflow.operators.bash import BashOperator  # pants: no-infer-dep
from airflow.sensors.base import BaseSensorOperator  # pants: no-infer-dep
from cosmos import (  # pants: no-infer-dep
    DbtTaskGroup,  # pants: no-infer-dep
    ExecutionConfig,  # pants: no-infer-dep
    ExecutionMode,  # pants: no-infer-dep
    LoadMode,  # pants: no-infer-dep
    ProfileConfig,  # pants: no-infer-dep
    ProjectConfig,  # pants: no-infer-dep
    RenderConfig,  # pants: no-infer-dep
)
import yaml

_THIS_DIR = Path(__file__).resolve().parent
_ECOMMERCE_DIR = Path(
    os.environ.get(
        "ECOMMERCE_PROJECT_DIR",
        "/opt/project"
        if Path("/opt/project").is_dir()
        else str(_THIS_DIR.parent.parent),
    )
)
_REPO_ROOT = Path(
    os.environ.get(
        "REPO_ROOT",
        "/opt/repo"
        if Path("/opt/repo").is_dir()
        else str(_ECOMMERCE_DIR.parent.parent),
    )
)

_CONFIG_PATH = Path(
    os.environ.get(
        "ECOMMERCE_CONFIG_PATH",
        _ECOMMERCE_DIR / "streaming_layer" / "config" / "ecommerce_config.yml",
    )
)
_DBT_PROJ_DIR = Path(os.environ.get("DBT_PROJ_DIR", _ECOMMERCE_DIR / "batch_layer"))
_OUTPUT_PREFIX = os.environ.get(
    "CLICK_STREAM_OUTPUT_PREFIX", str(_ECOMMERCE_DIR / "output_data")
)
_DUCKDB_SERVING_PATH = os.environ.get(
    "DUCKDB_SERVING_PATH",
    os.environ.get("DUCKDB_PATH", str(_DBT_PROJ_DIR / "data" / "ecommerce.duckdb")),
)
_DUCKDB_STAGING_PATH = os.environ.get(
    "DUCKDB_STAGING_PATH",
    str(_DBT_PROJ_DIR / "data" / "ecommerce_staging.duckdb"),
)

_FALLBACK_SINKS = [
    "silver_ecommerce_events",
    "url_counts",
    "user_counts",
    "url_conversion",
    "category_sales",
    "product_sales",
    "cart_metrics",
    "session_funnels",
    "top_urls_per_user",
]

# Airflow tasks invoke the isolated pipeline virtualenv when present to avoid Airflow dependency conflicts.
_PIPELINE_PYTHON = (
    "PIPELINE_PY=$( [ -x /opt/pipeline-venv/bin/python ] && echo /opt/pipeline-venv/bin/python "
    f"|| echo {sys.executable} )"
)
_PIPELINE_DBT = (
    "PIPELINE_DBT=$( [ -x /opt/pipeline-venv/bin/dbt ] && echo /opt/pipeline-venv/bin/dbt "
    "|| command -v dbt )"
)


def _resolve_dbt_executable() -> str:
    """Resolve the dbt executable path for Astronomer Cosmos local execution."""
    candidates = [
        os.environ.get("PIPELINE_DBT_PATH"),
        "/opt/pipeline-venv/bin/dbt",
        str(_REPO_ROOT / ".venv" / "bin" / "dbt"),
        shutil.which("dbt"),
        str(Path(sys.executable).parent / "dbt"),
    ]
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    return "dbt"


def _load_sink_names(config_path: Path | None = None) -> list[str]:
    """Load expected Delta sink names from ``ecommerce_config.yml`` with fallback.

    Args:
        config_path: Optional override path to the YAML configuration file.

    Returns:
        List of expected streaming Delta table names including the Silver table.
    """
    path = config_path or _CONFIG_PATH
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        silver_table = config.get("storage", {}).get(
            "silver_table", "silver_ecommerce_events"
        )
        sinks = config.get("sinks", [])
        if sinks:
            ordered = [silver_table] + [s for s in sinks if s != silver_table]
            return ordered
    except (OSError, yaml.YAMLError):
        pass
    return _FALLBACK_SINKS


class DeltaStreamSensor(BaseSensorOperator):
    """Sensor checking whether Medallion Silver and KPI Delta outputs have committed."""

    def __init__(
        self,
        output_prefix: str = "/opt/project/output_data",
        min_ready_tables: int = 3,
        **kwargs,
    ):
        """Initialize the DeltaStreamSensor.

        Args:
            output_prefix: Base directory containing streaming Delta/Parquet
                table subdirectories.
            min_ready_tables: Minimum number of sink tables containing data
                before the sensor succeeds.
            **kwargs: Additional keyword arguments forwarded to
                ``BaseSensorOperator``.
        """
        super().__init__(**kwargs)
        self.output_prefix = output_prefix
        self.min_ready_tables = min_ready_tables
        self.targets = _load_sink_names()

    def poke(self, context) -> bool:
        """Check whether the Silver table and minimum KPI sinks contain Delta logs.

        Args:
            context: Airflow task execution context dictionary.

        Returns:
            ``True`` if at least ``min_ready_tables`` tables contain Delta log
            or Parquet files; ``False`` otherwise.
        """
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


def _resolve_cosmos_render_config() -> RenderConfig:
    """Build Cosmos RenderConfig using manifest mode when supplied, else dbt ls."""
    manifest_path = os.environ.get("DBT_MANIFEST_PATH")
    if manifest_path and os.path.exists(manifest_path):
        return RenderConfig(
            exclude=["path:models/monitors"],
            load_method=LoadMode.DBT_MANIFEST,
        )
    return RenderConfig(
        exclude=["path:models/monitors"],
        load_method=LoadMode.DBT_LS,
    )


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
    description=(
        "End-to-end Medallion DAG: optional availableNow Spark ingestion, "
        "OLTP sync, WAP staging, Astronomer Cosmos dbt + SCD2, GX gate, "
        "atomic DuckDB publish, and FinOps GCP sync."
    ),
    tags=["ecommerce", "medallion", "cosmos", "wap"],
) as dag:
    wait_for_stream = DeltaStreamSensor(
        task_id="wait_for_stream_outputs",
        output_prefix=_OUTPUT_PREFIX,
        min_ready_tables=3,
        poke_interval=20,
        timeout=600,
        # Release the Airflow worker slot between pokes while waiting for streaming outputs.
        mode="reschedule",
    )

    sync_postgres_dimensions = BashOperator(
        task_id="sync_postgres_dimensions",
        bash_command=(
            f"{_PIPELINE_PYTHON} && "
            f"$PIPELINE_PY {_ECOMMERCE_DIR / 'oltp_sync' / 'sync_dimensions.py'} "
            "--output-dir ${CLICK_STREAM_OUTPUT_PREFIX:-" + _OUTPUT_PREFIX + "} "
            "--seed-from-defaults-on-offline"
        ),
    )

    prepare_wap_staging_db = BashOperator(
        task_id="prepare_wap_staging_db",
        bash_command=(
            f"{_PIPELINE_PYTHON} && "
            f"$PIPELINE_PY {_ECOMMERCE_DIR / 'wap_duckdb.py'} prepare "
            f"--serving-path {_DUCKDB_SERVING_PATH} "
            f"--staging-path {_DUCKDB_STAGING_PATH}"
        ),
    )

    manifest_override = os.environ.get("DBT_MANIFEST_PATH")
    dbt_medallion_transform = DbtTaskGroup(
        group_id="dbt_medallion_transform",
        project_config=ProjectConfig(
            dbt_project_path=str(_DBT_PROJ_DIR),
            manifest_path=manifest_override if manifest_override else None,
        ),
        profile_config=ProfileConfig(
            profile_name="ecommerce_batch",
            target_name="dev",
            profiles_yml_filepath=str(_DBT_PROJ_DIR / "profiles.yml"),
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            dbt_executable_path=_resolve_dbt_executable(),
        ),
        render_config=_resolve_cosmos_render_config(),
        operator_args={
            # Serialize DuckDB writes because LocalExecutor runs tasks in parallel subprocesses.
            "pool": "duckdb_write_pool",
            "install_deps": False,
            "append_env": True,
            "env": {
                "DUCKDB_PATH": _DUCKDB_STAGING_PATH,
                "CLICK_STREAM_OUTPUT_PREFIX": _OUTPUT_PREFIX,
            },
        },
    )

    run_great_expectations = BashOperator(
        task_id="run_great_expectations",
        bash_command=(
            f"{_PIPELINE_PYTHON} && "
            f"$PIPELINE_PY {_ECOMMERCE_DIR / 'quality' / 'validate_ecommerce.py'} "
            f"--duckdb-path {_DUCKDB_STAGING_PATH} "
            "--output-prefix ${CLICK_STREAM_OUTPUT_PREFIX:-" + _OUTPUT_PREFIX + "} "
            "--fail-on-error"
        ),
    )

    dbt_build_monitors = BashOperator(
        task_id="dbt_build_monitors",
        pool="duckdb_write_pool",
        bash_command=(
            f"{_PIPELINE_DBT} && "
            f"cd {_DBT_PROJ_DIR} && "
            f"DUCKDB_PATH={_DUCKDB_STAGING_PATH} "
            "$PIPELINE_DBT build -s 'path:models/monitors/**'"
        ),
    )

    publish_wap_duckdb = BashOperator(
        task_id="publish_wap_duckdb",
        bash_command=(
            f"{_PIPELINE_PYTHON} && "
            f"$PIPELINE_PY {_ECOMMERCE_DIR / 'wap_duckdb.py'} publish "
            f"--serving-path {_DUCKDB_SERVING_PATH} "
            f"--staging-path {_DUCKDB_STAGING_PATH}"
        ),
    )

    sync_cloud_warehouse = BashOperator(
        task_id="sync_cloud_warehouse",
        bash_command=(
            f"{_PIPELINE_PYTHON} && "
            f"$PIPELINE_PY {_ECOMMERCE_DIR / 'cloud_sync' / 'export_to_gcp.py'} "
            f"--duckdb-path {_DUCKDB_SERVING_PATH}"
        ),
    )

    upstream_gate = wait_for_stream
    if os.environ.get("SPARK_IN_AIRFLOW", "false").lower() in ("true", "1", "yes"):
        run_spark_available_now = BashOperator(
            task_id="run_spark_available_now",
            bash_command=(
                f"{_PIPELINE_PYTHON} && "
                f"$PIPELINE_PY {_ECOMMERCE_DIR / 'streaming_layer' / 'ecommerce_aggregation_job.py'} "
                "--mode available-now --sink delta"
            ),
        )
        if os.environ.get("PRODUCE_SYNTHETIC_EVENTS", "false").lower() in (
            "true",
            "1",
            "yes",
        ):
            produce_synthetic_events = BashOperator(
                task_id="produce_synthetic_events",
                bash_command=(
                    f"{_PIPELINE_PYTHON} && "
                    f"$PIPELINE_PY {_ECOMMERCE_DIR / 'streaming_layer' / 'ecommerce_producer.py'} "
                    "--mode synthetic --count 40 --rate 20 --anomaly-ratio 0.1"
                ),
            )
            produce_synthetic_events >> run_spark_available_now >> wait_for_stream
        else:
            run_spark_available_now >> wait_for_stream

    (
        upstream_gate
        >> sync_postgres_dimensions
        >> prepare_wap_staging_db
        >> dbt_medallion_transform
        >> run_great_expectations
        >> dbt_build_monitors
        >> publish_wap_duckdb
        >> sync_cloud_warehouse
    )
