"""Shared helpers for loading suspension data used by the dashboard builders.

The repository's R analysis pipeline (see
``Analysis/17_tail_by_grade-school_concentration_analysis.R``) defines the
canonical cleaning and grouping logic for the school-level suspension data.
The Python data-build scripts mirror that behaviour to guarantee that every
HTML dashboard is fuelled by the same numbers.  Keeping the normalisation logic
in this module prevents the builders from drifting out of sync with the
pipeline when fields or encodings change.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_STAGE = PROJECT_ROOT / "data-stage"
LONG_PATH = DATA_STAGE / "susp_v6_long.parquet"
FEATURES_PATH = DATA_STAGE / "susp_v6_features.parquet"

YEAR_PATTERN = re.compile(r"^(\d{4})")
SETTING_ALT_PATTERN = re.compile(
    r"alternative|charter|continuation|community", re.IGNORECASE
)
GRADE_ELEMENTARY = re.compile(r"elementary|elem|primary|k.*5|k.*6", re.IGNORECASE)
GRADE_MIDDLE = re.compile(r"middle|junior|intermediate|6.*8|7.*8", re.IGNORECASE)
GRADE_HIGH = re.compile(r"high|secondary|9.*12|senior", re.IGNORECASE)
GRADE_ALT = re.compile(r"adult|continuation", re.IGNORECASE)


@dataclass(frozen=True)
class SuspensionRecord:
    """Normalised suspension row used by the dashboard builders."""

    school_id: str
    year_num: int
    year_label: str
    level: str
    setting: str
    enrollment: float
    total_susp: float
    undup_susp: float

    @property
    def measure(self) -> float:
        """Return the default measure (total suspensions) used in reports."""

        return self.total_susp


def extract_year(value: object) -> int | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    match = YEAR_PATTERN.search(str(value))
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def map_grade_level(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "Unknown"
    text = str(value).strip()
    if not text:
        return "Unknown"
    if GRADE_ELEMENTARY.search(text):
        return "Elementary"
    if GRADE_MIDDLE.search(text):
        return "Middle"
    if GRADE_HIGH.search(text):
        return "High"
    if GRADE_ALT.search(text):
        return "Alternative"
    return "Other"


def map_setting(raw: object, is_traditional: object) -> str:
    if not pd.isna(is_traditional):
        return "Traditional" if bool(is_traditional) else "Non-traditional"
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        # The graph scripts assume schools with missing flags are traditional
        # unless explicitly labelled otherwise. Mirror that default so the
        # dashboard shares the same totals.
        return "Traditional"
    text = str(raw).strip()
    if not text:
        return "Traditional"
    lowered = text.lower()
    if "non" in lowered and "traditional" in lowered:
        return "Non-traditional"
    if SETTING_ALT_PATTERN.search(lowered):
        return "Non-traditional"
    if "traditional" in lowered:
        return "Traditional"
    # Fall back to the canonical behaviour of treating unspecified values as
    # traditional so downstream percentages stay aligned with the R outputs.
    return "Traditional"


def safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    """Return ``numer / denom`` while protecting against zero denominators."""

    denom = denom.replace({0: np.nan})
    return numer / denom


def load_feature_flags() -> pd.DataFrame:
    """Return school-level feature flags for traditional status."""

    if not FEATURES_PATH.exists():
        return pd.DataFrame(
            columns=["school_code", "academic_year", "is_traditional", "school_type"]
        )
    feats = pq.read_table(
        FEATURES_PATH,
        columns=["school_code", "academic_year", "is_traditional", "school_type"],
    ).to_pandas()
    feats["school_code"] = feats["school_code"].astype("string")
    feats["academic_year"] = feats["academic_year"].astype("string")
    return feats.drop_duplicates()


def load_suspension_long(columns: Sequence[str] | None = None) -> pd.DataFrame:
    """Load the long-form suspension file filtered to Total/All Students rows."""

    base_cols = [
        "school_code",
        "school_name",
        "academic_year",
        "school_type",
        "school_level",
        "cumulative_enrollment",
        "total_suspensions",
        "unduplicated_count_of_students_suspended_total",
        "subgroup",
    ]
    requested = list(dict.fromkeys(list(columns or []) + base_cols))
    table = pq.read_table(LONG_PATH, columns=requested)
    df = table.to_pandas()
    mask = df["subgroup"].astype("string").str.lower().isin({"total", "all students", "ta"})
    df = df.loc[mask].copy()
    df["school_id"] = df["school_code"].astype("string")
    df["year_label"] = df["academic_year"].astype("string")
    df["year_num"] = df["year_label"].map(extract_year)
    df["level_raw"] = df["school_level"].astype("string")
    df["setting_raw"] = df["school_type"].astype("string")
    df["enrollment"] = pd.to_numeric(df["cumulative_enrollment"], errors="coerce")
    df["total_susp"] = pd.to_numeric(df["total_suspensions"], errors="coerce")
    df["undup_susp"] = pd.to_numeric(
        df["unduplicated_count_of_students_suspended_total"], errors="coerce"
    )
    df = df.drop(columns=[c for c in requested if c not in base_cols], errors="ignore")
    return df


def prepare_analysis_frame() -> pd.DataFrame:
    """Return cleaned suspension data with grade level and setting labels."""

    susp = load_suspension_long()
    feats = load_feature_flags()
    merged = susp.merge(
        feats,
        left_on=["school_id", "year_label"],
        right_on=["school_code", "academic_year"],
        how="left",
        suffixes=("", "_feat"),
    )
    merged["setting"] = [
        map_setting(fallback if pd.notna(fallback) else raw, is_trad)
        for raw, fallback, is_trad in zip(
            merged["setting_raw"], merged["school_type_feat"], merged["is_traditional"]
        )
    ]
    merged["level"] = [map_grade_level(value) for value in merged["level_raw"]]
    cleaned = merged[
        [
            "school_id",
            "year_num",
            "year_label",
            "level",
            "setting",
            "enrollment",
            "total_susp",
            "undup_susp",
        ]
    ].copy()
    cleaned = cleaned.replace({np.nan: None})
    cleaned = cleaned.loc[
        cleaned["year_num"].notna()
        & cleaned["enrollment"].notna()
        & cleaned["total_susp"].notna()
        & (cleaned["enrollment"] > 0)
        & (cleaned["total_susp"] >= 0)
    ]
    cleaned["school_id"] = cleaned["school_id"].astype("string")
    cleaned["year_num"] = cleaned["year_num"].astype(int)
    cleaned["year_label"] = cleaned["year_label"].astype("string")
    cleaned["enrollment"] = cleaned["enrollment"].astype(float)
    cleaned["total_susp"] = cleaned["total_susp"].astype(float)
    cleaned["undup_susp"] = cleaned["undup_susp"].astype(float)
    cleaned["level"] = cleaned["level"].astype("string")
    cleaned["setting"] = cleaned["setting"].astype("string")
    cleaned = cleaned.drop_duplicates(
        subset=["school_id", "year_label", "level", "setting"], keep="first"
    )
    return cleaned.reset_index(drop=True)


__all__ = [
    "PROJECT_ROOT",
    "prepare_analysis_frame",
    "safe_div",
    "map_grade_level",
    "map_setting",
]
