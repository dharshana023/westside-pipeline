import sqlite3

import pytest

from pipeline.load import (
    already_succeeded,
    atomic_replace_batch,
    idempotent_merge_upsert,
    init_schema,
    record_run,
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:", isolation_level=None)
    init_schema(c)
    yield c
    c.close()


def _rec(
    key="k1",
    name="A",
    price=10.0,
    category="men",
    subcategory="shirts",
):
    return {
        "product_key": key,
        "name": name,
        "price": price,
        "category": category,
        "subcategory": subcategory,
    }


class TestIdempotentMergeUpsert:
    def test_first_run_inserts(self, conn):
        result = idempotent_merge_upsert(conn, [_rec()], "batch1")
        assert result == {"inserted": 1, "updated": 0, "unchanged": 0}

    def test_identical_rerun_is_a_noop(self, conn):
        idempotent_merge_upsert(conn, [_rec()], "batch1")
        result = idempotent_merge_upsert(conn, [_rec()], "batch1")
        assert result == {"inserted": 0, "updated": 0, "unchanged": 1}
        count = conn.execute(
            "SELECT COUNT(*) FROM curated_products"
        ).fetchone()[0]
        assert count == 1

    def test_changed_value_updates_in_place_not_duplicates(self, conn):
        idempotent_merge_upsert(conn, [_rec(price=10.0)], "batch1")
        result = idempotent_merge_upsert(conn, [_rec(price=20.0)], "batch2")
        assert result == {"inserted": 0, "updated": 1, "unchanged": 0}
        rows = conn.execute(
            "SELECT price FROM curated_products WHERE product_key='k1'"
        ).fetchall()
        assert rows == [(20.0,)]

    def test_rerun_many_times_never_duplicates(self, conn):
        for _ in range(5):
            idempotent_merge_upsert(conn, [_rec()], "batch1")
        count = conn.execute(
            "SELECT COUNT(*) FROM curated_products"
        ).fetchone()[0]
        assert count == 1


class TestAtomicReplaceBatch:
    def test_successful_batch_lands_fully(self, conn):
        atomic_replace_batch(
            conn,
            [_rec("k1"), _rec("k2", name="B")],
            "batchA",
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM curated_products"
        ).fetchone()[0]
        assert count == 2

    def test_failure_partway_leaves_target_untouched(self, conn):
        atomic_replace_batch(conn, [_rec("k1")], "batchA")

        bad_records = [
            _rec("k2", name="B"),
            {"product_key": "k3"},
        ]
        with pytest.raises(KeyError):
            atomic_replace_batch(conn, bad_records, "batchB")

        # batchA's row must be untouched, and no partial batchB rows should exist
        remaining = conn.execute(
            "SELECT product_key, batch_id FROM curated_products"
        ).fetchall()
        assert remaining == [("k1", "batchA")]

    def test_retry_after_fixing_the_bad_record_succeeds_fully(self, conn):
        bad_records = [
            _rec("k2"),
            {"product_key": "k3"},
        ]

        with pytest.raises(KeyError):
            atomic_replace_batch(conn, bad_records, "batchB")

        good_records = [
            _rec("k2"),
            _rec("k3", name="C"),
        ]

        atomic_replace_batch(conn, good_records, "batchB")

        count = conn.execute(
            "SELECT COUNT(*) FROM curated_products WHERE batch_id='batchB'"
        ).fetchone()[0]

        assert count == 2

    def test_recorded_success_is_detected(self, conn):
        record_run(conn, "2026-01-01", "SUCCESS", rows_loaded=10)
        assert already_succeeded(conn, "2026-01-01") is True

    def test_failed_run_is_not_treated_as_succeeded(self, conn):
        record_run(
            conn,
            "2026-01-01",
            "FAILED",
            rows_loaded=0,
            message="boom",
        )
        assert already_succeeded(conn, "2026-01-01") is False

    def test_rerecording_same_date_overwrites_status(self, conn):
        record_run(conn, "2026-01-01", "FAILED", rows_loaded=0)
        record_run(conn, "2026-01-01", "SUCCESS", rows_loaded=5)
        assert already_succeeded(conn, "2026-01-01") is True

        row = conn.execute(
            "SELECT rows_loaded FROM pipeline_runs WHERE execution_date=?",
            ("2026-01-01",),
        ).fetchone()

        assert row[0] == 5