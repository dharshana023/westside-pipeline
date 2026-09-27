"""
Loading logic: idempotency, atomicity and the run ledger that makes backfills safe.

Written against Python's stdlib `sqlite3` so it's fully unit-testable with an
in-memory database and no external services. On Databricks, swap the `conn`
parameter for a helper that runs the equivalent `MERGE INTO` / transactional SQL
against a Delta table — the *logic* (what gets inserted/updated/skipped, and when
a batch is rolled back) is identical.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from .transform import row_hash


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS curated_products (
            product_key TEXT PRIMARY KEY, name TEXT, price REAL, category TEXT,
            subcategory TEXT, row_hash TEXT, batch_id TEXT, updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            execution_date TEXT PRIMARY KEY, status TEXT, rows_loaded INTEGER,
            message TEXT, run_at TEXT
        );
        """
    )


def idempotent_merge_upsert(conn: sqlite3.Connection, records: list[dict], batch_id: str) -> dict:
    """Insert new products, update ones whose content hash changed, skip unchanged
    ones. Safe to call repeatedly with the same records -- never duplicates rows."""
    cur = conn.cursor()
    counts = {"inserted": 0, "updated": 0, "unchanged": 0}
    for r in records:
        h = row_hash(r)
        existing = cur.execute(
            "SELECT row_hash FROM curated_products WHERE product_key = ?", (r["product_key"],)
        ).fetchone()
        if existing is None:
            cur.execute(
                "INSERT INTO curated_products "
                "(product_key,name,price,category,subcategory,row_hash,batch_id,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (r["product_key"], r["name"], r["price"], r["category"], r["subcategory"], h, batch_id, now()),
            )
            counts["inserted"] += 1
        elif existing[0] != h:
            cur.execute(
                "UPDATE curated_products SET name=?, price=?, category=?, subcategory=?, "
                "row_hash=?, batch_id=?, updated_at=? WHERE product_key=?",
                (r["name"], r["price"], r["category"], r["subcategory"], h, batch_id, now(), r["product_key"]),
            )
            counts["updated"] += 1
        else:
            counts["unchanged"] += 1
    return counts


def atomic_replace_batch(conn: sqlite3.Connection, records: list[dict], batch_id: str) -> None:
    """Delete + insert this batch's rows inside a single transaction: either the
    whole batch lands, or (on any exception) none of it does."""
    try:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM curated_products WHERE batch_id = ?", (batch_id,))
        for r in records:
            conn.execute(
                "INSERT INTO curated_products "
                "(product_key,name,price,category,subcategory,row_hash,batch_id,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (r["product_key"], r["name"], r["price"], r["category"], r["subcategory"],
                 row_hash(r), batch_id, now()),
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def already_succeeded(conn: sqlite3.Connection, execution_date: str) -> bool:
    """The idempotency guard a scheduler (Airflow) checks before doing any work."""
    row = conn.execute(
        "SELECT status FROM pipeline_runs WHERE execution_date = ?", (execution_date,)
    ).fetchone()
    return bool(row and row[0] == "SUCCESS")


def record_run(conn: sqlite3.Connection, execution_date: str, status: str, rows_loaded: int, message: str = "") -> None:
    conn.execute(
        "INSERT INTO pipeline_runs (execution_date, status, rows_loaded, message, run_at) VALUES (?,?,?,?,?) "
        "ON CONFLICT(execution_date) DO UPDATE SET status=excluded.status, rows_loaded=excluded.rows_loaded, "
        "message=excluded.message, run_at=excluded.run_at",
        (execution_date, status, rows_loaded, message, now()),
    )
