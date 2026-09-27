"""
Transformation logic for the Westside product pipeline.

Kept dependency-free (pandas only) and side-effect-free so every function here is
trivially unit-testable without a database, a cluster, or a network connection.
"""
from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from datetime import datetime, timezone

import pandas as pd


def clean_price(raw_price) -> float:
    """Convert a noisy price string like '₹ 1,699.00' (or a plain number) to a float.

    Raises ValueError on anything that isn't a parseable price, so bad data fails
    loudly in a unit test rather than silently becoming NaN downstream.
    """
    if raw_price is None:
        raise ValueError("price is None")
    if isinstance(raw_price, (int, float)):
        return float(raw_price)
    text = str(raw_price).strip()
    text = re.sub(r"[₹$,\s]", "", text)
    if text == "":
        raise ValueError(f"empty price after cleaning: {raw_price!r}")
    return float(text)


def clean_price_series(series: pd.Series) -> pd.Series:
    """Vectorized version of clean_price for a whole DataFrame column."""
    return (
        series.astype(str)
        .str.replace("₹", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
        .astype(float)
    )


def extract_brand(product_name: str) -> str:
    """Brand is the first whitespace-delimited token of the product name."""
    if not product_name or not str(product_name).strip():
        raise ValueError("product_name is empty")
    return str(product_name).strip().split()[0]


def standardize_category(category: str) -> str:
    """Trim and lower-case a category/subcategory label for consistent grouping."""
    return str(category).strip().lower()


def make_product_key(name: str, category: str) -> str:
    """Stable business key for a product, used by idempotent upserts."""
    basis = f"{str(name).strip()}|{standardize_category(category)}"
    return hashlib.md5(basis.encode("utf-8")).hexdigest()[:16]


def row_hash(record: dict, fields: Iterable[str] = ("name", "price", "category")) -> str:
    """Content hash of the given fields — used to detect whether a row actually
    changed between runs, so idempotent loads can skip no-op updates."""
    basis = "|".join(str(record.get(f, "")) for f in fields)
    return hashlib.md5(basis.encode("utf-8")).hexdigest()


def flatten_rating(record: dict) -> dict:
    """Flatten a nested {'rating': {'rate': .., 'count': ..}} field, as returned by
    APIs like fakestoreapi.com, into top-level rating_rate / rating_count columns."""
    out = dict(record)
    rating = out.pop("rating", None) or {}
    out["rating_rate"] = rating.get("rate")
    out["rating_count"] = rating.get("count")
    return out


def enrich_with_ingestion_metadata(record: dict, source_system: str) -> dict:
    out = dict(record)
    out["ingested_at"] = datetime.now(timezone.utc).isoformat()
    out["source_system"] = source_system
    return out


def transform_catalog(df: pd.DataFrame) -> pd.DataFrame:
    """End-to-end transform for a raw product catalog DataFrame (as loaded from
    westside_combined.csv): clean price, standardize categories, derive brand and
    a stable product_key. Raises if required columns are missing."""
    required = {"name", "price", "category", "subcategory"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"transform_catalog: missing required columns: {sorted(missing)}")

    out = df.drop_duplicates().copy()
    out["price"] = clean_price_series(out["price"])
    out["category"] = out["category"].map(standardize_category)
    out["subcategory"] = out["subcategory"].map(standardize_category)
    out["brand"] = out["name"].map(extract_brand)
    out["product_key"] = [
        make_product_key(n, c) for n, c in zip(out["name"], out["category"])
    ]
    return out
