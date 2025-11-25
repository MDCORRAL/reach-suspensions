"""Shared data quality checks for graph generation scripts.

These helpers enforce the CLAUDE.md guidance around dropping
impossible suspension counts, keeping rates within [0, 1], and
recording anomalies to ``outputs/data_audit`` for traceability.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


def ensure_audit_dir(root_dir: Path) -> Path:
    audit_dir = root_dir / "outputs" / "data_audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    return audit_dir


def _as_numeric(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    result = frame.copy()
    for col in columns:
        result[col] = pd.to_numeric(result[col], errors="coerce")
    return result


def audit_counts_against_enrollment(
    frame: pd.DataFrame,
    *,
    count_columns: Sequence[str],
    enrollment_column: str,
    context: str,
    audit_dir: Path,
) -> pd.DataFrame:
    """Drop rows with impossible suspension volumes and log them.

    Any rows with negative counts, negative enrollment, or counts exceeding
    enrollment are removed. Offending rows are appended to
    ``outputs/data_audit/graph_input_anomalies.csv`` so downstream analysts
    can review dropped cases.
    """

    working = _as_numeric(frame, [*count_columns, enrollment_column]).copy()
    working["_row_id"] = np.arange(len(working))

    negative_counts = (working[list(count_columns)] < 0).any(axis=1)
    over_enrollment = (working[list(count_columns)] > working[enrollment_column]).any(axis=1)
    invalid_enrollment = working[enrollment_column] < 0

    invalid_mask = negative_counts | over_enrollment | invalid_enrollment
    invalid_rows = working.loc[invalid_mask].copy()

    if not invalid_rows.empty:
        audit_path = audit_dir / "graph_input_anomalies.csv"
        candidate_cols = [
            "academic_year",
            "school_level",
            "subgroup",
            "locale_simple",
            "school_code",
            *count_columns,
            enrollment_column,
        ]
        export_cols = [col for col in candidate_cols if col in invalid_rows.columns]
        audit_payload = invalid_rows.loc[:, export_cols].copy()
        audit_payload.insert(0, "context", context)
        audit_payload.to_csv(
            audit_path,
            mode="a",
            index=False,
            header=not audit_path.exists(),
        )
        print(
            f"[{context}] Dropped {len(invalid_rows)} rows with impossible"
            f" counts; details written to {audit_path}."
        )

    cleaned = working.loc[~invalid_mask].copy()
    cleaned = cleaned.drop(columns=["_row_id"])
    return cleaned


def sanitize_rate_column(
    frame: pd.DataFrame,
    *,
    rate_column: str,
    context: str,
    audit_dir: Path,
) -> pd.DataFrame:
    """Ensure computed rates live within [0, 1] and log anomalies."""

    working = frame.copy()
    working[rate_column] = pd.to_numeric(working[rate_column], errors="coerce")
    invalid_mask = (working[rate_column] < 0) | (working[rate_column] > 1)
    invalid_rows = working.loc[invalid_mask].copy()

    if not invalid_rows.empty:
        audit_path = audit_dir / "graph_input_anomalies.csv"
        candidate_cols = [
            "academic_year",
            "school_level",
            "subgroup",
            "locale_simple",
            "school_code",
            rate_column,
        ]
        export_cols = [col for col in candidate_cols if col in invalid_rows.columns]
        audit_payload = invalid_rows.loc[:, export_cols].copy()
        audit_payload.insert(0, "context", f"{context} (rate)")
        audit_payload.to_csv(
            audit_path,
            mode="a",
            index=False,
            header=not audit_path.exists(),
        )
        print(
            f"[{context}] Replaced {len(invalid_rows)} out-of-range rates with NA;"
            f" details written to {audit_path}."
        )

    working.loc[invalid_mask, rate_column] = np.nan
    return working
