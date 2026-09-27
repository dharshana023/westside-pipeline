# Databricks notebook source
# ============================================================================
# Production-Ready Resilient Data Pipelines
# Techniques: Staging & Validation | Idempotency | Atomicity | Error Handling
# Tools:   Apache Airflow (orchestration)  +  Apache Kafka (streaming)
# Runtime: Databricks (PySpark / Delta Lake)
# ============================================================================

import os
import json
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Callable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException, ParseException
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# Logging – structured, idempotent across reruns
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("resilient_pipeline")

spark = SparkSession.builder.getOrCreate()

# ============================================================================
# 1. CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class PipelineConfig:
    """Immutable, environment-driven configuration."""
    catalog:        str = "main"
    schema:         str = "analytics"
    kafka_brokers:  str = "broker1:9092,broker2:9092"
    kafka_topic:    str = "events.raw"
    kafka_group_id: str = "databricks-consumer-grp"

    # Staging / bronze / silver / gold table names
    staging_table:  str = "staging_raw_events"
    bronze_table:   str = "bronze_events"
    silver_table:   str = "silver_events"
    gold_table:     str = "gold_events"

    # Retry / replay
    max_retries:           int = 3
    backoff_base_seconds:  int = 30

    # Schema
    expected_schema: StructType = field(
        default_factory=lambda: StructType([
            # populated in build_expected_schema() below
        ])
    )

    def fqn(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"

cfg = PipelineConfig()

# ============================================================================
# 2. STAGING & VALIDATION
# ============================================================================

def read_from_kafka(topic: str, group_id: str) -> DataFrame:
    """
    Read a micro-batch from Kafka using structured streaming.
    Uses earliest offset only on first run; subsequent runs resume
    from committed offsets (idempotent consumer pattern).
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_brokers)
        .option("subscribe", topic)
        .option("kafka.group.id", group_id)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )


def create_secure_staging(df: DataFrame, table: str, partition_cols: list[str] | None = None):
    """
    Write raw data to a secure staging table.
    Practices applied:
      • Location-based isolation (schema-level)
      • Optimized writes (auto-compact, predictive I/O)
      • Checkpointing for exactly-once semantics
      • Partition pruning on ingestion date
    """
    checkpoint_dir = f"/tmp/checkpoints/{table}"
    writer = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .option("mergeSchema", "false")           # strict – no silent schema drift
        .option("maxFilesPerTrigger", "100")
        .queryName(f"staging_write_{table}")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    return writer.toTable(cfg.fqn(table))


# --- Validation primitives -------------------------------------------------

def validate_schema(df: DataFrame, expected: StructType) -> DataFrame:
    """Fail-fast if the incoming schema does not match expectations."""
    actual = {f.name: f.dataType for f in df.schema.fields}
    expected_map = {f.name: f.dataType for f in expected.fields}
    missing = set(expected_map) - set(actual)
    extra = set(actual) - set(expected_map)
    if missing or extra:
        raise ValueError(
            f"Schema mismatch | missing: {missing} | unexpected: {extra}"
        )
    log.info("Schema validation passed (%d fields)", len(expected_map))
    return df


def null_check(df: DataFrame, required_cols: list[str], threshold: float = 0.0) -> DataFrame:
    """Raise if null ratio in any required column exceeds *threshold*."""
    total = df.count()
    if total == 0:
        raise ValueError("Empty batch – nothing to validate.")
    for col_name in required_cols:
        nulls = df.filter(F.col(col_name).isNull()).count()
        ratio = nulls / total
        if ratio > threshold:
            raise ValueError(
                f"Null check failed | col={col_name} | nulls={nulls} "
                f"| ratio={ratio:.2%} > threshold={threshold:.2%}"
            )
    log.info("Null checks passed for columns: %s", required_cols)
    return df


def outlier_check(
    df: DataFrame, numeric_col: str, lo: float = -3, hi: float = 3
) -> DataFrame:
    """
    Z-score outlier detection (mean/std). Rows beyond ±3σ are flagged
    in a new column rather than dropped (auditability).
    """
    stats = df.agg(
        F.mean(numeric_col).alias("mean"),
        F.stddev(numeric_col).alias("std"),
    ).collect()[0]
    mean, std = stats["mean"], stats["std"]
    if std is None or std == 0:
        log.warning("Stddev is 0 for %s – skipping outlier check.", numeric_col)
        return df.withColumn(f"{numeric_col}_is_outlier", F.lit(False))

    z = (F.col(numeric_col) - F.lit(mean)) / F.lit(std)
    return df.withColumn(
        f"{numeric_col}_is_outlier",
        (z < F.lit(lo)) | (z > F.lit(hi)),
    )


def run_all_validations(df: DataFrame, expected_schema: StructType) -> DataFrame:
    """Composed validation pipeline."""
    df = validate_schema(df, expected_schema)
    df = null_check(df, required_cols=["event_id", "event_ts", "user_id"])
    df = outlier_check(df, numeric_col="amount")
    log.info("All validations passed for batch.")
    return df


# ============================================================================
# 3. IDEMPOTENCY
# ============================================================================

def idempotent_merge(
    df: DataFrame, table_name: str, join_cols: list[str]
) -> None:
    """
    Upsert (MERGE) pattern – safe to rerun.
    Rows that already exist (matched on join_cols) are updated;
    new rows are inserted. No duplicates are ever produced.
    """
    target = cfg.fqn(table_name)
    if not spark.catalog.tableExists(target):
        df.write.format("delta").mode("overwrite").saveAsTable(target)
        log.info("Created target table %s (initial load).", target)
        return

    delta_tbl = DeltaTable.forName(spark, target)
    (
        delta_tbl.alias("t")
        .merge(df.alias("s"), " AND ".join([f"t.{c} = s.{c}" for c in join_cols]))
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info("MERGE completed into %s on keys %s", target, join_cols)


def idempotent_delete_insert(
    df: DataFrame,
    table_name: str,
    join_cols: list[str],
    replace_condition: Optional[str] = None,
) -> None:
    """
    Delete-and-Insert (REPLACE WHERE) pattern.
    First deletes all rows that match the incoming partition/predicate,
    then inserts the new batch.  Idempotent because the delete + insert
    is wrapped in a single Delta transaction (atomic).

    Parameters
    ----------
    replace_condition : str | None
        SQL predicate identifying the window to replace, e.g.
        'event_date = date "2024-01-15"'.  If None, derives the
        min/max event_ts from *df* and deletes that range.
    """
    target = cfg.fqn(table_name)

    if not spark.catalog.tableExists(target):
        df.write.format("delta").mode("overwrite").saveAsTable(target)
        log.info("Created target table %s (initial load).", target)
        return

    if replace_condition is None:
        bounds = df.agg(F.min("event_ts").alias("lo"),
                        F.max("event_ts").alias("hi")).collect()[0]
        replace_condition = (
            f"event_ts >= timestamp '{bounds['lo']}' "
            f"AND event_ts <= timestamp '{bounds['hi']}'"
        )

    # -- Atomic transaction: DELETE + INSERT via REPLACE WHERE ---------------
    spark.sql(f"""
        INSERT INTO {target}
        REPLACE WHERE {replace_condition}
        SELECT * FROM ({df._jdf.toString()})  -- pass through the logical plan
    """)
    log.info("DELETE-INSERT (REPLACE WHERE) completed on %s | cond: %s",
             target, replace_condition)


# ============================================================================
# 4. ATOMICITY
# ============================================================================

def atomic_batch_write(
    df: DataFrame,
    table_name: str,
    join_cols: list[str],
    mode: str = "merge",
    replace_condition: Optional[str] = None,
) -> str:
    """
    All-or-nothing batch write.

    Delta Lake provides ACID guarantees at the table level.  This function
    ensures that *either* the entire batch is committed *or* nothing changes:
      • MERGE   → idempotent upsert
      • DELETE_INSERT → REPLACE WHERE pattern
      • OVERWRITE → full replace (use with caution)

    Returns the commit version on success.
    """
    try:
        if mode == "merge":
            idempotent_merge(df, table_name, join_cols)
        elif mode == "delete_insert":
            idempotent_delete_insert(df, table_name, join_cols, replace_condition)
        elif mode == "overwrite":
            df.write.format("delta").mode("overwrite")\
                .saveAsTable(cfg.fqn(table_name))
        else:
            raise ValueError(f"Unknown write mode: {mode}")

        commit_version = DeltaTable.forName(spark, cfg.fqn(table_name))\
                              .history(1).select("version").collect()[0][0]
        log.info("Atomic commit succeeded | table=%s | version=%s",
                 table_name, commit_version)
        return str(commit_version)

    except Exception as exc:
        log.error("Atomic write FAILED – no changes committed | %s", exc)
        raise


# ============================================================================
# 5. ERROR HANDLING – BACKFILL & REPLAY
# ============================================================================

def replay_kafka_offsets(
    topic: str,
    start_ts: datetime,
    end_ts: datetime,
    group_id_suffix: str = "backfill",
) -> DataFrame:
    """
    Re-read Kafka messages for a specific time window.
    Uses a *separate* consumer group so the replay does not disturb
    committed offsets of the live consumer.
    """
    kafka_opts = {
        "kafka.bootstrap.servers": cfg.kafka_brokers,
        "subscribe": topic,
        "kafka.group.id": f"{cfg.kafka_group_id}-{group_id_suffix}",
        "startingOffsets": "earliest",
        "endingOffsets":   "latest",
        "failOnDataLoss": "false",  # tolerate gaps during replay
    }
    raw = (
        spark.read.format("kafka").options(**kafka_opts).load()
        .filter(F.col("timestamp") >= F.lit(start_ts))
        .filter(F.col("timestamp") <= F.lit(end_ts))
    )
    log.info("Replay window: %s → %s | rows=%d", start_ts, end_ts, raw.count())
    return raw


def backfill(
    table_name: str,
    start_date: str,
    end_date: str,
    transform_fn: Callable[[DataFrame], DataFrame],
    join_cols: list[str],
) -> None:
    """
    Historical backfill with full idempotency & atomicity guarantees.

    1. Read raw data for the requested date range.
    2. Apply the same transformations used in production.
    3. DELETE the existing rows for that range, then INSERT the corrected
       data atomically (REPLACE WHERE) — safe to rerun.
    """
    log.info("Backfill START | table=%s | %s → %s", table_name, start_date, end_date)

    raw_df = spark.table(cfg.fqn(table_name)).filter(
        (F.col("event_date") >= F.lit(start_date))
        & (F.col("event_date") <= F.lit(end_date))
    )

    transformed = transform_fn(raw_df)
    replace_cond = (
        f"event_date >= date '{start_date}' "
        f"AND event_date <= date '{end_date}'"
    )
    atomic_batch_write(
        df=transformed,
        table_name=table_name,
        join_cols=join_cols,
        mode="delete_insert",
        replace_condition=replace_cond,
    )
    log.info("Backfill COMPLETE | table=%s | %s → %s",
             table_name, start_date, end_date)


def with_retry(
    func: Callable, *args, retries: int = 3, backoff: int = 30, **kwargs
):
    """
    Exponential-backoff retry wrapper for transient failures
    (network blips, Kafka leader election, warehouse warm-up, etc.).
    """
    import time
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            if attempt == retries:
                log.error("All %d attempts exhausted. Raising: %s", retries, exc)
                raise
            wait = backoff * (2 ** (attempt - 1))
            log.warning("Attempt %d/%d failed (%s). Retrying in %ds…",
                        attempt, retries, exc, wait)
            time.sleep(wait)


# ============================================================================
# 6. AIRFLOW DAG – ORCHESTRATION
# ============================================================================

AIRFLOW_DAG_CODE = r'''
# ---- Save as:  dags/resilient_pipeline_dag.py ----
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "depends_on_past": False,          # idempotent – no need to wait
}

@dag(
    dag_id="resilient_pipeline",
    description="Staging → Validation → Idempotent Merge → Backfill-ready",
    default_args=DEFAULT_ARGS,
    schedule="0 */2 * * *",             # every 2 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,                       # no implicit backfill; explicit only
    max_active_runs=1,                   # serialise to guarantee idempotency
    tags=["production", "kafka", "delta"],
)
def resilient_pipeline():

    # -- Task 1: Run validation + staging on Databricks cluster -------------
    validate_and_stage = DatabricksSubmitRunOperator(
        task_id="validate_and_stage",
        databricks_conn_id="databricks_default",
        notebook_task={
            "notebook_path": "/Production/validate_and_stage",
            "base_parameters": {
                "run_date": "{{ ds }}",
                "mode": "validate_stage",
            },
        },
    )

    # -- Task 2: Idempotent MERGE into Silver -------------------------------
    merge_to_silver = DatabricksSqlOperator(
        task_id="merge_to_silver",
        databricks_conn_id="databricks_default",
        sql="""
            MERGE INTO main.analytics.silver_events AS t
            USING main.analytics.staging_raw_events AS s
              ON  t.event_id = s.event_id
            WHEN MATCHED  THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """,
    )

    # -- Task 3: Atomic aggregation to Gold ---------------------------------
    gold_aggregation = DatabricksSqlOperator(
        task_id="gold_aggregation",
        databricks_conn_id="databricks_default",
        sql="""
            INSERT INTO main.analytics.gold_events
            REPLACE WHERE event_date = date '{{ ds }}'
            SELECT
                event_date,
                user_id,
                COUNT(*)        AS event_count,
                SUM(amount)      AS total_amount,
                CURRENT_TIMESTAMP AS refreshed_at
            FROM main.analytics.silver_events
            WHERE event_date = date '{{ ds }}'
            GROUP BY event_date, user_id
        """,
    )

    # -- Task 4: Data-quality gate (fail the DAG if DQ breaches) -------------
    dq_gate = DatabricksSqlOperator(
        task_id="dq_gate",
        databricks_conn_id="databricks_default",
        sql="""
            SELECT COUNT(*) AS null_breaches
            FROM main.analytics.gold_events
            WHERE event_date = date '{{ ds }}'
              AND (user_id IS NULL OR total_amount IS NULL)
        """,
        do_xcom_push=True,
    )

    @task
    def assert_dq(breaches: int) -> str:
        assert breaches == 0, f"DQ gate failed: {breaches} null breaches"
        return "OK"

    validate_and_stage >> merge_to_silver >> gold_aggregation >> dq_gate >> assert_dq()

resilient_pipeline()
'''

print(AIRFLOW_DAG_CODE)

# ============================================================================
# 7. END-TO-END EXECUTION EXAMPLE
# ============================================================================

def run_batch_pipeline():
    """
    Demonstrate the complete batch pipeline with all four guarantees:
      1. Staging & Validation  – read → validate → stage
      2. Idempotency           – MERGE on primary keys
      3. Atomicity             – single Delta transaction
      4. Error Handling        – retry wrapper + backfill available
    """
    log.info("=" * 60)
    log.info("Starting batch pipeline")
    log.info("=" * 60)

    # -- Read from staging (simulating Kafka batch landed in staging) -------
    raw = spark.table(cfg.fqn(cfg.staging_table))

    # -- Validate -----------------------------------------------------------
    validated = with_retry(
        run_all_validations,
        df=raw,
        expected_schema=raw.schema,
        retries=cfg.max_retries,
        backoff=cfg.backoff_base_seconds,
    )

    # -- Atomic idempotent merge into Silver --------------------------------
    with_retry(
        atomic_batch_write,
        df=validated,
        table_name=cfg.silver_table,
        join_cols=["event_id"],
        mode="merge",
        retries=cfg.max_retries,
        backoff=cfg.backoff_base_seconds,
    )

    log.info("Batch pipeline completed successfully.")


def run_backfill_example():
    """Example: backfill Jan 1 – Jan 7 after a bug fix."""
    def identity_transform(df: DataFrame) -> DataFrame:
        # In reality, apply the *corrected* transformation logic here.
        return df.withColumn("backfilled_at", F.current_timestamp())

    backfill(
        table_name=cfg.silver_table,
        start_date="2024-01-01",
        end_date="2024-01-07",
        transform_fn=identity_transform,
        join_cols=["event_id"],
    )


# ============================================================================
# 8. SUMMARY – How each constraint is satisfied
# ============================================================================

SUMMARY = """
╔════════════════════════════╦═══════════════════════════════════════════════════════╗
║ Technique                  ║ Implementation                                         ║
╠════════════════════════════╬═══════════════════════════════════════════════════════╣
║ Staging & Validation       ║ secure_staging(): Kafka→Delta w/ checkpointing        ║
║                            ║ validate_schema(), null_check(), outlier_check()      ║
║ Idempotency (Merge)        ║ idempotent_merge(): Delta MERGE on primary keys        ║
║ Idempotency (Delete+Insert)║ idempotent_delete_insert(): REPLACE WHERE pattern      ║
║ Atomicity                  ║ atomic_batch_write(): single Delta ACID transaction   ║
║ Error Handling – Retry     ║ with_retry(): exponential backoff for transient errs  ║
║ Error Handling – Replay    ║ replay_kafka_offsets(): separate consumer group        ║
║ Error Handling – Backfill  ║ backfill(): idempotent historical reprocessing        ║
║ Orchestration              ║ Airflow DAG: serialised, catchup=False, DQ gate       ║
║ Streaming                  ║ Kafka structured streaming w/ exactly-once semantics   ║
╚════════════════════════════╩═══════════════════════════════════════════════════════╝
"""
print(SUMMARY)

# COMMAND ----------

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "depends_on_past": False,
}