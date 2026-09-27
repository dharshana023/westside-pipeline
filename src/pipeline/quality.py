"""
Data-quality / staging validation logic.

`validate()` never raises on bad *rows* — it quarantines them — so one malformed
record can't take down an entire batch. It only raises on a genuine schema problem
(missing required columns), which should fail the pipeline loudly.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

REQUIRED_COLUMNS = ("name", "price", "category")


@dataclass
class ValidationReport:
    input_rows: int
    valid_rows: int
    rejected_rows: int
    duplicate_rows_dropped: int
    reject_reasons: dict = field(default_factory=dict)

    @property
    def reject_rate(self) -> float:
        return 0.0 if self.input_rows == 0 else self.rejected_rows / self.input_rows


def check_schema(df: pd.DataFrame, required: tuple = REQUIRED_COLUMNS) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Schema validation failed, missing columns: {missing}")


def detect_price_outliers(prices: pd.Series, iqr_multiplier: float = 3.0) -> pd.Series:
    """Returns a boolean mask of rows whose price is an extreme (Tukey IQR) outlier."""
    if len(prices) == 0:
        return pd.Series([], dtype=bool)
    q1, q3 = prices.quantile([0.25, 0.75])
    iqr = q3 - q1
    high = q3 + iqr_multiplier * iqr
    return prices > high


def validate(df: pd.DataFrame, key_col: str = "product_key") -> tuple[pd.DataFrame, pd.DataFrame, ValidationReport]:
    """Run schema, null, invalid-value, outlier and duplicate checks.

    Returns (valid_rows_df, rejected_rows_df, report). Rejected rows carry a
    'reject_reason' column so they can be inspected and replayed later.
    """
    check_schema(df)
    df = df.copy()
    rejects = []

    null_mask = df[list(REQUIRED_COLUMNS)].isna().any(axis=1)
    for _, r in df[null_mask].iterrows():
        rejects.append({**r.to_dict(), "reject_reason": "null_required_field"})
    df = df[~null_mask]

    invalid_mask = df["price"] <= 0
    for _, r in df[invalid_mask].iterrows():
        rejects.append({**r.to_dict(), "reject_reason": "non_positive_price"})
    df = df[~invalid_mask]

    outlier_mask = detect_price_outliers(df["price"])
    for _, r in df[outlier_mask].iterrows():
        rejects.append({**r.to_dict(), "reject_reason": "price_outlier"})
    df = df[~outlier_mask]

    n_dupes = 0
    if key_col in df.columns:
        dup_mask = df.duplicated(subset=[key_col])
        n_dupes = int(dup_mask.sum())
        df = df[~dup_mask]

    rejects_df = pd.DataFrame(rejects)
    reasons = rejects_df["reject_reason"].value_counts().to_dict() if len(rejects_df) else {}

    report = ValidationReport(
        input_rows=len(df) + len(rejects) + n_dupes,
        valid_rows=len(df),
        rejected_rows=len(rejects),
        duplicate_rows_dropped=n_dupes,
        reject_reasons=reasons,
    )
    return df, rejects_df, report
