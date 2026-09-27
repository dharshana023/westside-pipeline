# Running this pipeline on Airflow — local setup

## 1. Install Airflow locally (pip, using the official constraints file)

```bash
export AIRFLOW_VERSION=2.9.3
export PYTHON_VERSION=3.11
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

python3 -m venv .airflow-venv && source .airflow-venv/bin/activate
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

export AIRFLOW_HOME=~/airflow
airflow db migrate
airflow users create \
  --username admin --password admin \
  --firstname Data --lastname Engineer \
  --role Admin --email admin@example.com
```

(Prefer Docker? `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'`
then `docker compose up airflow-init && docker compose up` — skip to step 3 if you go
this route, mounting the repo into the containers instead of using a venv.)

## 2. Make the pipeline package importable from the DAG

The DAG imports `from pipeline import extract, transform, quality, load`. Point it at
this repo two ways — pick one:

```bash
# Option A: environment variable the DAG reads (simplest)
export WESTSIDE_REPO_ROOT=/absolute/path/to/westside-pipeline

# Option B: install the package into the same venv Airflow runs in
pip install -e /absolute/path/to/westside-pipeline
```

## 3. Deploy the DAG file

```bash
mkdir -p "$AIRFLOW_HOME/dags"
cp airflow/dags/westside_pipeline_dag.py "$AIRFLOW_HOME/dags/"
```

Airflow scans `dags/` every ~30s; it'll appear in the UI shortly after.

## 4. Start Airflow and check the DAG

```bash
airflow webserver --port 8080 &
airflow scheduler &
```

Open http://localhost:8080 (login `admin`/`admin`), find `westside_pipeline`, and
turn it on. Trigger a manual run to confirm it works before relying on the schedule.

## 5. Backfill a date range

This is exactly the idempotent-rerun-safe scenario tested in `tests/test_load.py`:

```bash
airflow dags backfill westside_pipeline -s 2026-02-01 -e 2026-02-07
```

Dates that already succeeded are skipped by `check_already_done()` in the DAG; only
missing/failed dates actually do work.

## 6. Wire this into CI/CD

- Keep `airflow/dags/westside_pipeline_dag.py` in this same Git repo (already is), so a
  DAG change goes through the same PR review / `./scripts/run_local_ci.sh` gate as any
  other pipeline change (see `docs/GIT_BRANCHING_STRATEGY.md`).
- Add a quick DAG-integrity test so CI catches a broken DAG before it reaches Airflow:

```python
# tests/test_dag_integrity.py
import os, sys
sys.path.insert(0, "airflow/dags")

def test_dag_imports_without_errors():
    os.environ.setdefault("WESTSIDE_REPO_ROOT", os.getcwd())
    import westside_pipeline_dag  # noqa: F401  -- import alone proves it parses & builds
```

  (Needs `apache-airflow` added to `requirements-dev.txt` to run in CI.)
- On Databricks specifically: Airflow can trigger a Databricks job instead of running
  Spark itself, via the `DatabricksSubmitRunOperator` / `DatabricksRunNowOperator`
  (`pip install apache-airflow-providers-databricks`) — useful if heavy transforms
  should run on a Databricks cluster while Airflow just orchestrates and does the
  lightweight extract/validate steps shown here.
