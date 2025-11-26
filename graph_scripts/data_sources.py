"""Shared data loading utilities for graph scripts.

This module provides reusable functions for loading and filtering
suspension data from parquet files.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Set

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from data_validations import audit_counts_against_enrollment, ensure_audit_dir


# Project root detection
def get_project_root() -> Path:
    """Detect the project root directory.

    Returns:
        Path to project root (contains data-stage/ directory)
    """
    env_root = os.environ.get("REACH_SUSPENSIONS_ROOT")
    if env_root:
        candidate = Path(env_root).expanduser()
        if (candidate / "data-stage").exists():
            return candidate.resolve()

    # Try from current file location
    try:
        start = Path(__file__).resolve()
    except NameError:
        start = Path.cwd().resolve()

    for candidate in [start, *start.parents]:
        if (candidate / "data-stage").exists() and (candidate / "graph_scripts").exists():
            return candidate

    raise RuntimeError(
        "Unable to locate project root. Set REACH_SUSPENSIONS_ROOT or run from repository."
    )


PROJECT_ROOT = get_project_root()
DATA_STAGE = PROJECT_ROOT / "data-stage"
AUDIT_DIR = ensure_audit_dir(PROJECT_ROOT)

# Constants
SPECIAL_SCHOOL_CODES: Set[str] = {"0000000", "0000001"}
DEFAULT_IS_TRADITIONAL = True


def load_susp_v6_long(
    columns: list[str] | None = None,
    *,
    filter_campus: bool = True,
    filter_special_codes: bool = True,
) -> pd.DataFrame:
    """Load long-format suspension data (susp_v6_long.parquet).

    Args:
        columns: Specific columns to load (None = all columns)
        filter_campus: Filter to campus-level records only (default: True)
        filter_special_codes: Exclude special school codes (default: True)

    Returns:
        DataFrame with suspension data
    """
    parquet_path = DATA_STAGE / "susp_v6_long.parquet"

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Required file not found: {parquet_path}\n"
            "Run the data pipeline first: source('run_all.R')"
        )

    df = pq.read_table(parquet_path, columns=columns).to_pandas()

    # Filter to campus-level records
    if filter_campus and "aggregate_level" in df.columns:
        df["aggregate_level"] = df["aggregate_level"].astype("string")
        df = df[df["aggregate_level"].str.lower().isin({"s", "school"})]
        df = df.drop(columns=["aggregate_level"])

    # Exclude special school codes
    if filter_special_codes and "school_code" in df.columns:
        df["school_code"] = df["school_code"].astype(str).str.strip().str.zfill(7)
        df = df[~df["school_code"].isin(SPECIAL_SCHOOL_CODES)]

    return df


def load_susp_v6_features(columns: list[str] | None = None) -> pd.DataFrame:
    """Load wide-format suspension features (susp_v6_features.parquet).

    Args:
        columns: Specific columns to load (None = all columns)

    Returns:
        DataFrame with suspension features
    """
    parquet_path = DATA_STAGE / "susp_v6_features.parquet"

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Required file not found: {parquet_path}\n"
            "Run the data pipeline first: source('run_all.R')"
        )

    df = pq.read_table(parquet_path, columns=columns).to_pandas()

    # Standardize school_code
    if "school_code" in df.columns:
        df["school_code"] = df["school_code"].astype(str).str.strip().str.zfill(7)

    return df


def filter_traditional_schools(
    df: pd.DataFrame,
    *,
    charter_column: str = "charter_yn_std",
    is_traditional_column: str = "is_traditional",
) -> pd.DataFrame:
    """Filter dataset to traditional (non-charter) schools.

    Args:
        df: Input DataFrame
        charter_column: Name of charter flag column (default: "charter_yn_std")
        is_traditional_column: Name of traditional flag column (default: "is_traditional")

    Returns:
        Filtered DataFrame with only traditional schools
    """
    if charter_column in df.columns:
        # Use charter_yn_std column
        return df[df[charter_column] == "No"].copy()
    elif is_traditional_column in df.columns:
        # Use is_traditional column
        return df[df[is_traditional_column] == True].copy()
    else:
        # No filtering available, return all
        return df.copy()


def prepare_reason_data(
    df: pd.DataFrame,
    *,
    reason_columns: dict[str, str],
    school_levels: list[str] | None = None,
    locales: list[str] | None = None,
    subgroup: str = "All Students",
) -> pd.DataFrame:
    """Prepare suspension reason data for plotting.

    Args:
        df: Input DataFrame from load_susp_v6_long()
        reason_columns: Mapping of column names to display labels
        school_levels: Filter to specific school levels (None = all)
        locales: Filter to specific locales (None = all)
        subgroup: Student subgroup to analyze (default: "All Students")

    Returns:
        Melted DataFrame ready for plotting
    """
    # Filter to relevant data
    filtered = df[
        (df["category_type"] == "Race/Ethnicity")
        & (df["subgroup"] == subgroup)
    ].copy()

    # Filter by school level
    if school_levels is not None:
        filtered = filtered[filtered["school_level"].isin(school_levels)]

    # Filter by locale
    if locales is not None and "locale_simple" in filtered.columns:
        filtered = filtered[filtered["locale_simple"].isin(locales)]

    # Aggregate by year
    agg_dict = {col: "sum" for col in reason_columns.keys()}
    agg_dict["cumulative_enrollment"] = "sum"

    group_cols = ["academic_year"]
    aggregated = (
        filtered
        .groupby(group_cols, observed=True, dropna=False)
        .agg(agg_dict)
        .reset_index()
    )

    # Validate data quality
    aggregated = audit_counts_against_enrollment(
        aggregated,
        count_columns=list(reason_columns.keys()),
        enrollment_column="cumulative_enrollment",
        context="prepare_reason_data",
        audit_dir=AUDIT_DIR,
    )

    # Melt to long format
    melted = aggregated.melt(
        id_vars=["academic_year", "cumulative_enrollment"],
        value_vars=list(reason_columns.keys()),
        var_name="reason",
        value_name="count",
    )

    # Add labels and calculate rates
    melted["reason_label"] = melted["reason"].map(reason_columns)
    melted["rate"] = np.where(
        melted["cumulative_enrollment"] > 0,
        melted["count"] / melted["cumulative_enrollment"],
        np.nan,
    )

    melted = melted.dropna(subset=["reason_label", "rate"])

    return melted
