# westside-pipeline

CI/CD and version control for the Westside product data pipeline: a Git repo layout,
an importable transformation/quality/load module, a pytest suite (including mocked
API tests), and two ways to trigger it automatically — a **local CI runner** (primary;
needs nothing but Python) and an optional **GitHub Actions** workflow.

## Quickstart — run everything locally, no cloud/CI account needed

```bash
git clone <this-repo>
cd westside-pipeline
python3 -m venv .venv && source .venv/bin/activate   # optional but recommended
./scripts/run_local_ci.sh
```

That one script installs dependencies, lints, and runs the full test suite with
coverage — exactly what `.github/workflows/ci.yml` runs, just on your own machine.
To run only the tests, skipping lint/install:

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## Project layout

```
westside-pipeline/
├── src/pipeline/              # the pipeline code — plain Python, no Spark/Databricks
│   ├── extract.py             #   API extraction, retries + injectable fallback
│   ├── transform.py           #   price cleaning, feature derivation, key/hash logic
│   ├── quality.py             #   schema/null/outlier/duplicate validation
│   └── load.py                #   idempotent upsert, atomic delete+insert, run ledger
├── tests/                     # pytest suite, one file per module above (incl. mocks)
├── scripts/run_local_ci.sh    # local CI runner (lint + test) — run this before every push
├── .github/workflows/ci.yml   # optional: the same checks, run by GitHub on push/PR
├── databricks/                # "pipeline as code": job/schedule defined as YAML
│   ├── databricks.yml         #   dev / staging / prod targets
│   └── resources/pipeline_job.yml
├── docs/GIT_BRANCHING_STRATEGY.md
├── docs/GITHUB_SETUP.md       # connect this repo to GitHub, protect branches, first CI run
├── docs/AIRFLOW_SETUP.md      # run the pipeline on a local Airflow instance
├── airflow/dags/westside_pipeline_dag.py   # the DAG: extract -> transform -> validate -> load
└── notebooks/                 # the original Databricks lab notebooks, for reference
```

## Next steps

- **GitHub:** see `docs/GITHUB_SETUP.md` — push this repo, protect `main`/`develop`,
  add the two Databricks secrets if you want the prod-deploy job, open your first PR.
- **Airflow:** see `docs/AIRFLOW_SETUP.md` — install Airflow locally, point it at this
  repo, deploy `airflow/dags/westside_pipeline_dag.py`, and try a backfill.

## Why the pipeline code lives outside the notebooks

`src/pipeline/*.py` holds the same transformation/validation/load logic your Databricks
notebooks use, but as plain, dependency-free functions. That's what makes it
unit-testable in milliseconds on a laptop, with no cluster and no network:

- `extract.fetch_products()` takes an **injected session** (see `tests/test_extract.py`)
  so tests exercise real retry/backoff/fallback logic against a mock, never a live API.
- `load.*` runs against a plain `sqlite3` connection in tests; the same
  insert/update/skip and transaction logic is what a Databricks job would run against
  Delta (`MERGE INTO`, `BEGIN`/`COMMIT`) in production.

Your Databricks notebooks (`notebooks/*.ipynb`) stay as the orchestration/exploration
layer and can simply `import` this package once it's installed as a wheel (see
`databricks/resources/pipeline_job.yml`) — logic lives in one tested place, not
duplicated across notebook cells.

## CI/CD flow

1. **Local, every commit:** `./scripts/run_local_ci.sh` (lint + pytest + coverage).
2. **On push/PR to `main`/`develop`:** GitHub Actions runs the identical steps
   (`.github/workflows/ci.yml`).
3. **On merge to `main`:** a second job validates and deploys the Databricks Asset
   Bundle to the `prod` target — the pipeline's infra/schedule is versioned code, not a
   UI click (see `databricks/databricks.yml`).

See `docs/GIT_BRANCHING_STRATEGY.md` for the branch → environment mapping and the PR
checklist used for pipeline changes specifically (backward compatibility, backfill
plan, which DQ checks changed).

## Test coverage at a glance

| Module | What's tested |
|---|---|
| `test_transform.py` | price parsing (incl. currency symbols, invalid input), brand extraction, stable product keys, content hashing, JSON flattening, full-catalog transform |
| `test_quality.py` | schema check, null/negative-price/outlier quarantine, duplicate detection, internal count consistency of the validation report |
| `test_load.py` | idempotent upsert (insert/update/no-op), atomic delete+insert (successful batch, mid-batch failure rolls back fully, retry after fix succeeds), the run ledger used for backfill skip-logic |
| `test_extract.py` | success path, retry-then-succeed, exponential backoff timing, no-fallback failure, fallback-on-failure, HTTP-status-error handling — **all via mocks, zero real network calls** |
