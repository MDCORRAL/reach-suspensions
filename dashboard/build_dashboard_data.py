import json
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = PROJECT_ROOT / "data-stage" / "susp_v6_long.parquet"

OUTPUT_PATH = PROJECT_ROOT / "dashboard" / "data" / "dashboard_data.json"



def sanitize_for_json(value):
    """Recursively replace NaN-like values with ``None`` so JSON is valid."""

    if isinstance(value, dict):
        return {key: sanitize_for_json(item) for key, item in value.items()}

    if isinstance(value, list):
        return [sanitize_for_json(item) for item in value]

    if value is pd.NA:
        return None

    if isinstance(value, (float, np.floating)) and np.isnan(value):
        return None

    return value


def safe_rate(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    rate = numerator / denominator.replace({0: np.nan})
    return (rate * 100).round(3)


def share_within_group(series: pd.Series) -> pd.Series:
    total = series.sum()
    if total == 0:
        return pd.Series([np.nan] * len(series), index=series.index)
    return (series / total).round(4)


def summarize() -> dict:
    df = pd.read_parquet(DATA_PATH)

    # Normalize labels for filtering
    for col in [
        "black_prop_q_label",
        "school_level_final",
        "reporting_category_description",
    ]:
        df[col] = df[col].astype("string").fillna("Unknown")

    df["reason_lab"] = df["reason_lab"].astype("string")
    df["academic_year"] = df["academic_year"].astype("string")
    df["school_code"] = df["school_code"].astype("string")

    # Replace missing counts with zeros to avoid propagation of NaNs
    count_cols = [
        "total_suspensions",
        "unduplicated_count_of_students_suspended_total",
        "cumulative_enrollment",
    ]
    df[count_cols] = df[count_cols].fillna(0)

    filter_cols = [
        "academic_year",
        "school_level_final",
        "reporting_category_description",
        "black_prop_q_label",
    ]

    per_school_cols = filter_cols + ["school_code"]

    # Collapse to one record per school to avoid reason-level duplication
    per_school_reason = (
        df.groupby(per_school_cols + ["reason_lab"], dropna=False)
        .agg(
            total_suspensions=("total_suspensions", "sum"),
            students_suspended=(
                "unduplicated_count_of_students_suspended_total", "max"
            ),
            enrollment=("cumulative_enrollment", "max"),
        )
        .reset_index()
    )

    per_school_overall = (
        df.groupby(per_school_cols, dropna=False)
        .agg(
            total_suspensions=("total_suspensions", "sum"),
            students_suspended=(
                "unduplicated_count_of_students_suspended_total", "max"
            ),
            enrollment=("cumulative_enrollment", "max"),
        )
        .reset_index()
    )

    # Reason level summaries aggregated across schools
    reason_summary = (
        per_school_reason.groupby(filter_cols + ["reason_lab"], dropna=False)
        .agg(
            total_suspensions=("total_suspensions", "sum"),
            students_suspended=("students_suspended", "sum"),
            enrollment=("enrollment", "sum"),
        )
        .reset_index()
    )

    reason_summary["suspension_rate"] = safe_rate(
        reason_summary["total_suspensions"], reason_summary["enrollment"]
    )
    reason_summary["student_rate"] = safe_rate(
        reason_summary["students_suspended"], reason_summary["enrollment"]
    )
    reason_summary["share_of_total"] = reason_summary.groupby(filter_cols)[
        "total_suspensions"
    ].transform(share_within_group)

    reason_summary = reason_summary[
        filter_cols
        + [
            "reason_lab",
            "total_suspensions",
            "suspension_rate",
            "student_rate",
            "share_of_total",
        ]
    ]

    # Aggregate across reasons for overall totals
    overall = (
        per_school_overall.groupby(filter_cols, dropna=False)
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

    # Disparities relative to White students and all students (Total)
    white_baseline = (
        overall[overall["reporting_category_description"] == "White"]
        .set_index([c for c in filter_cols if c != "reporting_category_description"])
        .loc[:, ["suspension_rate"]]
        .rename(columns={"suspension_rate": "white_rate"})
    )

    total_baseline = (
        overall[overall["reporting_category_description"] == "Total"]
        .set_index([c for c in filter_cols if c != "reporting_category_description"])
        .loc[:, ["suspension_rate"]]
        .rename(columns={"suspension_rate": "overall_rate"})
    )

    key_index = [c for c in filter_cols if c != "reporting_category_description"]
    disparity = (
        overall.set_index(filter_cols)
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

    # Round numeric columns for compact JSON
    for frame in (reason_summary, disparity):
        numeric_cols = frame.select_dtypes(include=["float", "int"]).columns
        frame[numeric_cols] = frame[numeric_cols].round(3)

    return {
        "meta": {
            "academic_years": sorted(df["academic_year"].unique()),
            "school_levels": sorted(df["school_level_final"].dropna().unique()),
            "student_groups": sorted(
                df["reporting_category_description"].dropna().unique()
            ),
            "black_quartiles": sorted(
                df["black_prop_q_label"].dropna().unique().tolist()
            ),
            "reason_labels": sorted(df["reason_lab"].dropna().unique()),
        },
        "overall": disparity.to_dict(orient="records"),
        "reasons": reason_summary.to_dict(orient="records"),
    }


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = sanitize_for_json(summarize())
    OUTPUT_PATH.write_text(json.dumps(data, indent=2, allow_nan=False))
    print(f"Wrote {OUTPUT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
