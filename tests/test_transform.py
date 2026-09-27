import pandas as pd
import pytest

from pipeline.transform import (
    clean_price,
    clean_price_series,
    extract_brand,
    flatten_rating,
    make_product_key,
    row_hash,
    standardize_category,
    transform_catalog,
)


class TestCleanPrice:
    def test_currency_symbol_and_commas(self):
        assert clean_price("₹ 1,699.00") == 1699.0

    def test_plain_string_number(self):
        assert clean_price("42.5") == 42.5

    def test_numeric_passthrough(self):
        assert clean_price(500) == 500.0
        assert clean_price(19.99) == 19.99

    def test_none_raises(self):
        with pytest.raises(ValueError):
            clean_price(None)

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            clean_price("₹ ")

    def test_series_version_matches_scalar(self):
        s = pd.Series(["₹ 100.00", "₹ 2,500.50"])
        result = clean_price_series(s)
        assert list(result) == [100.0, 2500.50]


class TestExtractBrand:
    def test_first_word(self):
        assert extract_brand("Ascot Slim Fit Shirt") == "Ascot"

    def test_strips_whitespace(self):
        assert extract_brand("   Nuon Tee  ") == "Nuon"

    @pytest.mark.parametrize("bad", [None, "", "   "])
    def test_empty_name_raises(self, bad):
        with pytest.raises(ValueError):
            extract_brand(bad)


class TestStandardizeCategory:
    def test_lowercases_and_trims(self):
        assert standardize_category("  Men  ") == "men"


class TestProductKey:
    def test_stable_across_category_case(self):
        assert make_product_key("Ascot Shirt", "Men") == make_product_key("Ascot Shirt", "men")

    def test_different_products_get_different_keys(self):
        assert make_product_key("A", "men") != make_product_key("B", "men")


class TestRowHash:
    def test_changes_when_price_changes(self):
        h1 = row_hash({"name": "a", "price": 1, "category": "x"})
        h2 = row_hash({"name": "a", "price": 2, "category": "x"})
        assert h1 != h2

    def test_stable_for_identical_input(self):
        rec = {"name": "a", "price": 1, "category": "x"}
        assert row_hash(rec) == row_hash(dict(rec))


class TestFlattenRating:
    def test_flattens_nested_rating(self):
        out = flatten_rating({"name": "a", "rating": {"rate": 4.5, "count": 10}})
        assert out["rating_rate"] == 4.5
        assert out["rating_count"] == 10
        assert "rating" not in out

    def test_missing_rating_yields_none(self):
        out = flatten_rating({"name": "a"})
        assert out["rating_rate"] is None
        assert out["rating_count"] is None


class TestTransformCatalog:
    def _sample_df(self):
        return pd.DataFrame({
            "name": ["Ascot Shirt", "Nuon Tee"],
            "price": ["₹ 1,699.00", "₹ 499.00"],
            "category": ["Men", "Women"],
            "subcategory": ["Shirts", "Tees"],
        })

    def test_cleans_price_and_derives_brand(self):
        out = transform_catalog(self._sample_df())
        assert list(out["price"]) == [1699.0, 499.0]
        assert list(out["brand"]) == ["Ascot", "Nuon"]
        assert list(out["category"]) == ["men", "women"]
        assert out["product_key"].is_unique

    def test_drops_exact_duplicate_rows(self):
        df = pd.concat([self._sample_df(), self._sample_df().iloc[[0]]], ignore_index=True)
        out = transform_catalog(df)
        assert len(out) == 2

    def test_missing_required_column_raises(self):
        df = self._sample_df().drop(columns=["price"])
        with pytest.raises(ValueError, match="missing required columns"):
            transform_catalog(df)
