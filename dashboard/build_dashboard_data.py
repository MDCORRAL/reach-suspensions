"""Build the JSON payload consumed by the dashboard.

The previous iteration of this script accumulated numerous quick patches that
made it difficult to reason about how the final metrics were produced.  This
rewrite keeps the same external behaviour – including the structure of the
JSON payload – while structuring the computation as small, well named
functions.  The goal is to make the aggregation steps explicit and
repeatable so downstream visualisations receive consistent inputs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data-stage" / "susp_v6_long.parquet"
OUTPUT_PATH = PROJECT_ROOT / "dashboard" / "data" / "dashboard_data.json"

DIMENSION_COLUMNS: list[str] = [
    "academic_year",
    "school_level_final",
    "reporting_category_description",
    "black_prop_q_label",
]
SCHOOL_IDENTIFIER = "school_code"
REASON_COLUMN = "reason_lab"

NUMERIC_COLUMNS: list[str] = [
    "total_suspensions",
    "unduplicated_count_of_students_suspended_total",
    "cumulative_enrollment",
]
STRING_COLUMNS: list[str] = DIMENSION_COLUMNS + [SCHOOL_IDENTIFIER, REASON_COLUMN]


@dataclass(frozen=True)
class AggregatedData:
    """Container that holds all pieces of the dashboard payload."""

    meta: dict[str, list[str]]
    overall: list[dict[str, object]]
    reasons: list[dict[str, object]]


# ---------------------------------------------------------------------------
# Data preparation helpers
# ---------------------------------------------------------------------------

def read_source(path: Path, columns: Sequence[str]) -> pd.DataFrame:
    """Load the parquet file with only the columns we need."""

    return pd.read_parquet(path, columns=list(columns))


def normalise_strings(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Cast selected columns to pandas' string dtype with sensible defaults."""

    output = frame.copy()
    for column in columns:
        output[column] = output[column].astype("string")
        if column != SCHOOL_IDENTIFIER:
            # "Unknown" is preferable to NaN for front-end filters.
            output[column] = output[column].fillna("Unknown")
    return output


def fill_numeric_na(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Replace missing numeric counts with zeros so they do not poison sums."""

    output = frame.copy()
    for column in columns:
        output[column] = output[column].fillna(0)
    return output


# ---------------------------------------------------------------------------
# Metric calculations
# ---------------------------------------------------------------------------

def safe_rate(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Return a rate per 100 observations, protecting against zero denominators."""

    return (numerator / denominator.replace({0: np.nan}) * 100).round(3)


def aggregate_per_school(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return per-school overall and reason-level totals.

    Grouping at the school level prevents reason-level rows from double
    counting the same school's enrolment or student counts when we later sum
    across schools.
    """

    per_school_overall = (
        df.groupby(DIMENSION_COLUMNS + [SCHOOL_IDENTIFIER], dropna=False)
        .agg(
            total_suspensions=("total_suspensions", "sum"),
            students_suspended=(
                "unduplicated_count_of_students_suspended_total",
                "max",
            ),
            enrollment=("cumulative_enrollment", "max"),
        )
        .reset_index()
    )

    per_school_reason = (
        df.groupby(DIMENSION_COLUMNS + [SCHOOL_IDENTIFIER, REASON_COLUMN], dropna=False)
        .agg(total_suspensions=("total_suspensions", "sum"))
        .reset_index()
    )

    return per_school_overall, per_school_reason


def build_overall_summary(per_school_overall: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-school totals into the overall dashboard summary."""

    overall = (
        per_school_overall.groupby(DIMENSION_COLUMNS, dropna=False)
        .agg(
            total_suspensions=("total_suspensions", "sum"),
            students_suspended=("students_suspended", "sum"),
            enrollment=("enrollment", "sum"),
        )
        .reset_index()
    )

    overall["suspension_rate"] = safe_rate(
        overall["total_suspensions"], overall["enrollment"]
    )
    overall["student_rate"] = safe_rate(
        overall["students_suspended"], overall["enrollment"]
    )

    white_baseline = (
        overall[overall["reporting_category_description"] == "White"]
        .set_index([c for c in DIMENSION_COLUMNS if c != "reporting_category_description"])
        .loc[:, ["suspension_rate"]]
        .rename(columns={"suspension_rate": "white_rate"})
    )

    total_baseline = (
        overall[overall["reporting_category_description"] == "Total"]
        .set_index([c for c in DIMENSION_COLUMNS if c != "reporting_category_description"])
        .loc[:, ["suspension_rate"]]
        .rename(columns={"suspension_rate": "overall_rate"})
    )

    key_index = [c for c in DIMENSION_COLUMNS if c != "reporting_category_description"]
    disparity = (
        overall.set_index(DIMENSION_COLUMNS)
        .join(white_baseline, on=key_index)
        .join(total_baseline, on=key_index)
        .reset_index()
    )

    disparity["gap_vs_white"] = (
        disparity["suspension_rate"] - disparity["white_rate"]
    ).round(3)
    disparity["gap_vs_overall"] = (
        disparity["suspension_rate"] - disparity["overall_rate"]
    ).round(3)

    return disparity


def build_reason_summary(
    per_school_reason: pd.DataFrame, overall_summary: pd.DataFrame
) -> pd.DataFrame:
    """Aggregate suspensions by reason using overall denominators for rates."""

    reason_totals = (
        per_school_reason.groupby(DIMENSION_COLUMNS + [REASON_COLUMN], dropna=False)
        .agg(total_suspensions=("total_suspensions", "sum"))
        .reset_index()
    )

    reason_totals = reason_totals.merge(
        overall_summary[
            DIMENSION_COLUMNS
            + ["total_suspensions", "students_suspended", "enrollment"]
        ],
        on=DIMENSION_COLUMNS,
        how="left",
        suffixes=("", "_overall"),
    )

    reason_totals = reason_totals.rename(
        columns={
            "total_suspensions_overall": "overall_total_suspensions",
            "students_suspended": "overall_students_suspended",
            "enrollment": "overall_enrollment",
        }
    )

    reason_totals["share_of_total"] = (
        reason_totals["total_suspensions"]
        / reason_totals["overall_total_suspensions"].replace({0: np.nan})
    ).round(4)

    reason_totals["suspension_rate"] = safe_rate(
        reason_totals["total_suspensions"], reason_totals["overall_enrollment"]
    )

    # We do not have unique student counts per reason.  Approximating the
    # student rate using the share of total suspensions keeps the field in the
    # payload for backwards compatibility while making the calculation explicit.
    reason_totals["student_rate"] = safe_rate(
        reason_totals["share_of_total"] * reason_totals["overall_students_suspended"],
        reason_totals["overall_enrollment"],
    )

    ordered_columns = (
        DIMENSION_COLUMNS
        + [
            REASON_COLUMN,
            "total_suspensions",
            "suspension_rate",
            "student_rate",
            "share_of_total",
        ]
    )
    reason_totals = reason_totals[ordered_columns]

    return reason_totals


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def round_numeric_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Round all numeric columns to three decimals for compact JSON."""

    rounded = frame.copy()
    numeric_columns = rounded.select_dtypes(include=["float", "int"]).columns
    rounded[numeric_columns] = rounded[numeric_columns].round(3)
    return rounded


def build_meta(df: pd.DataFrame) -> dict[str, list[str]]:
    """Extract the lists that populate the dashboard filters."""

    return {
        "academic_years": sorted(df["academic_year"].dropna().unique().tolist()),
        "school_levels": sorted(
            df["school_level_final"].dropna().unique().tolist()
        ),
        "student_groups": sorted(
            df["reporting_category_description"].dropna().unique().tolist()
        ),
        "black_quartiles": sorted(df["black_prop_q_label"].dropna().unique().tolist()),
        "reason_labels": sorted(df[REASON_COLUMN].dropna().unique().tolist()),
    }


def sanitise_for_json(value):
    """Recursively convert NaN-like values into None so JSON is valid."""

    if isinstance(value, dict):
        return {key: sanitise_for_json(item) for key, item in value.items()}

    if isinstance(value, list):
        return [sanitise_for_json(item) for item in value]

    if value is pd.NA or (isinstance(value, (float, np.floating)) and np.isnan(value)):
        return None

    return value


def build_payload(df: pd.DataFrame) -> AggregatedData:
    """Run the full aggregation pipeline and package the outputs."""

    per_school_overall, per_school_reason = aggregate_per_school(df)
    overall_summary = build_overall_summary(per_school_overall)
    reason_summary = build_reason_summary(per_school_reason, overall_summary)

    overall_summary = round_numeric_columns(
        overall_summary.sort_values(DIMENSION_COLUMNS).reset_index(drop=True)
    )
    reason_summary = round_numeric_columns(
        reason_summary.sort_values(DIMENSION_COLUMNS + [REASON_COLUMN]).reset_index(
            drop=True
        )
    )

    return AggregatedData(
        meta=build_meta(df),
        overall=overall_summary.to_dict(orient="records"),
        reasons=reason_summary.to_dict(orient="records"),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    columns_to_load = set(DIMENSION_COLUMNS + [SCHOOL_IDENTIFIER, REASON_COLUMN])
    columns_to_load.update(NUMERIC_COLUMNS)

    df = read_source(DATA_PATH, sorted(columns_to_load))
    df = normalise_strings(df, STRING_COLUMNS)
    df = fill_numeric_na(df, NUMERIC_COLUMNS)

    payload = build_payload(df)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(sanitise_for_json(payload.__dict__), indent=2, allow_nan=False)
    )
    print(f"Wrote {OUTPUT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
