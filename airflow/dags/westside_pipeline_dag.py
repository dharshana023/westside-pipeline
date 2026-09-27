"""
westside_pipeline_dag.py

Orchestrates the same extract/transform/quality/load functions from
`src/pipeline` (already unit-tested in tests/) as a daily Airflow DAG.

Install:
    Copy or symlink this file into your Airflow DAGS_FOLDER, and make sure
    `src/` is importable from the Airflow worker (see AIRFLOW_SETUP.md,
    step "Make the pipeline package importable").

Idempotency / backfill:
    Every task keys off {{ ds }} (the run's logical date). `load.already_succeeded`
    checks the run ledger before doing any work, so `airflow dags backfill` or a
    manual re-run for the same date is always safe -- matching the design in
    tests/test_load.py.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

# Make the pipeline package importable regardless of where Airflow was installed from.
REPO_ROOT = os.environ.get("WESTSIDE_REPO_ROOT", "/opt/airflow/westside-pipeline")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from pipeline import extract, load, quality, transform

DB_PATH = os.environ.get("WESTSIDE_DB_PATH", "/opt/airflow/data/westside.db")
API_URL = os.environ.get("WESTSIDE_API_URL", "https://fakestoreapi.com/products")

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="westside_pipeline",
    description="Extract -> transform -> validate -> idempotent load, daily.",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["westside", "batch"],
)
def westside_pipeline():

    @task
    def check_already_done(ds: str) -> bool:
        """Idempotency guard: skip the whole DAG run if this date already succeeded."""
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, isolation_level=None)
        load.init_schema(conn)
        done = load.already_succeeded(conn, ds)
        conn.close()

        if done:
            raise AirflowSkipException(
                f"{ds} already loaded successfully -- skipping."
            )

        return False

    @task
    def extract_task(_: bool) -> list[dict]:
        import requests

        return extract.fetch_products(
            requests,
            api_url=API_URL,
            retries=3,
            fallback=_OFFLINE_FALLBACK,
        )

    @task
    def transform_task(records: list[dict]) -> list[dict]:
        import pandas as pd

        records = [transform.flatten_rating(r) for r in records]
        df = pd.DataFrame(records).rename(columns={"title": "name"})
        df["subcategory"] = df.get(
            "category",
            "unknown",
        )  # this source has no subcategory

        df = transform.transform_catalog(
            df[["name", "price", "category", "subcategory"]]
        )

        return df.to_dict("records")

    @task
    def validate_task(records: list[dict], ds: str) -> dict:
        import pandas as pd

        df = pd.DataFrame(records)
        valid_df, _rejects_df, report = quality.validate(df)

        if report.reject_rate > 0.5:
            # Quality gate: fail the run instead of loading obviously-bad data.
            raise ValueError(
                f"Reject rate {report.reject_rate:.0%} exceeds 50% threshold: "
                f"{report.reject_reasons}"
            )

        return {
            "valid_records": valid_df.to_dict("records"),
            "report": report.__dict__,
        }

    @task
    def load_task(validated: dict, ds: str) -> None:
        conn = sqlite3.connect(DB_PATH, isolation_level=None)
        load.init_schema(conn)

        try:
            result = load.idempotent_merge_upsert(
                conn,
                validated["valid_records"],
                batch_id=ds,
            )

            load.record_run(
                conn,
                ds,
                "SUCCESS",
                result["inserted"] + result["updated"],
                str(result),
            )

        except Exception as e:
            load.record_run(
                conn,
                ds,
                "FAILED",
                0,
                str(e),
            )
            raise

        finally:
            conn.close()

    gate = check_already_done()
    raw = extract_task(gate)
    transformed = transform_task(raw)
    validated = validate_task(
        transformed
    )  # 'ds' is auto-injected by Airflow's TaskFlow context
    load_task(validated)


_OFFLINE_FALLBACK = [
    {
        "id": 1,
        "title": "Fjallraven Foldsack No. 1 Backpack",
        "price": 109.95,
        "category": "men's clothing",
        "rating": {"rate": 3.9, "count": 120},
    },
    {
        "id": 2,
        "title": "Mens Casual Slim Fit T-Shirt",
        "price": 22.3,
        "category": "men's clothing",
        "rating": {"rate": 4.1, "count": 259},
    },
]


westside_pipeline()