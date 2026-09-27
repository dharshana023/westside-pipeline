import pandas as pd
import pytest

from pipeline.quality import validate


def _df(rows):
    return pd.DataFrame(rows)


class TestValidate:
    def test_valid_rows_pass(self):
        df = _df(
            [
                {
                    "name": "A",
                    "price": 100.0,
                    "category": "men",
                    "product_key": "k1",
                },
                {
                    "name": "B",
                    "price": 200.0,
                    "category": "women",
                    "product_key": "k2",
                },
            ]
        )

        valid, rejects, report = validate(df)

        assert len(valid) == 2
        assert len(rejects) == 0
        assert report.rejected_rows == 0
        assert report.reject_rate == 0.0

    def test_null_required_field_is_quarantined_not_dropped_silently(self):
        df = _df(
            [
                {
                    "name": None,
                    "price": 100.0,
                    "category": "men",
                    "product_key": "k1",
                },
                {
                    "name": "B",
                    "price": 200.0,
                    "category": "women",
                    "product_key": "k2",
                },
            ]
        )

        valid, rejects, _report = validate(df)

        assert len(valid) == 1
        assert len(rejects) == 1
        assert rejects.iloc[0]["reject_reason"] == "null_required_field"

    def test_non_positive_price_is_rejected(self):
        df = _df(
            [
                {
                    "name": "A",
                    "price": -5.0,
                    "category": "men",
                    "product_key": "k1",
                }
            ]
        )

        valid, rejects, _report = validate(df)

        assert len(valid) == 0
        assert rejects.iloc[0]["reject_reason"] == "non_positive_price"

    def test_extreme_outlier_is_quarantined(self):
        rows = [
            {
                "name": f"P{i}",
                "price": 100.0 + i,
                "category": "men",
                "product_key": f"k{i}",
            }
            for i in range(20)
        ]

        rows.append(
            {
                "name": "Outlier",
                "price": 999999.0,
                "category": "men",
                "product_key": "k_out",
            }
        )

        valid, _rejects, report = validate(_df(rows))

        assert "price_outlier" in report.reject_reasons
        assert "Outlier" not in list(valid["name"])

    def test_duplicate_key_is_dropped_and_counted_separately_from_rejects(
        self,
    ):
        df = _df(
            [
                {
                    "name": "A",
                    "price": 100.0,
                    "category": "men",
                    "product_key": "k1",
                },
                {
                    "name": "A",
                    "price": 100.0,
                    "category": "men",
                    "product_key": "k1",
                },
            ]
        )

        valid, _rejects, report = validate(df)

        assert len(valid) == 1
        assert report.duplicate_rows_dropped == 1
        assert report.rejected_rows == 0

    def test_missing_schema_raises_immediately(self):
        df = _df(
            [
                {
                    "name": "A",
                    "category": "men",
                }
            ]
        )

        with pytest.raises(
            ValueError,
            match="Schema validation failed",
        ):
            validate(df)

    def test_report_counts_are_internally_consistent(self):
        df = _df(
            [
                {
                    "name": None,
                    "price": 100.0,
                    "category": "men",
                    "product_key": "k1",
                },
                {
                    "name": "B",
                    "price": -1.0,
                    "category": "men",
                    "product_key": "k2",
                },
                {
                    "name": "C",
                    "price": 50.0,
                    "category": "men",
                    "product_key": "k3",
                },
                {
                    "name": "C",
                    "price": 50.0,
                    "category": "men",
                    "product_key": "k3",
                },
            ]
        )

        valid, _rejects, report = validate(df)

        assert (
            report.input_rows
            == len(valid)
            + report.rejected_rows
            + report.duplicate_rows_dropped
        )

        assert report.valid_rows == len(valid)
