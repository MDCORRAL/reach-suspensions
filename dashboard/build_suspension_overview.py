"""Build the suspension overview payload consumed by ``suspension_dashboard.html``.

The previous dashboard hard-coded summary numbers directly in the HTML, which
made it easy for the figures to fall out of sync with the analysis pipeline.
This script regenerates the full payload from the canonical school-level
suspension dataset so that the overview stays consistent with the R analysis
outputs.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = THIS_DIR.parent
if str(THIS_DIR) not in sys.path:
    sys.path.append(str(THIS_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from data_sources import prepare_analysis_frame as load_base_frame, safe_div

OUTPUT_DIR = PROJECT_ROOT / "dashboard" / "data"
JSON_PATH = OUTPUT_DIR / "suspension_dashboard.json"
JS_PATH = OUTPUT_DIR / "suspension_dashboard.js"
GRADE_LEVELS = ("Elementary", "Middle", "High")
SETTINGS = ("Traditional", "Non-traditional")


def format_percent(value: float, decimals: int = 1) -> str:
    if pd.isna(value):
        return "0.0%"
    return f"{value * 100:.{decimals}f}%"


def load_overview_frame() -> pd.DataFrame:
    frame = load_base_frame()
    frame = frame.rename(columns={"year_label": "year"})
    frame = frame.loc[frame["year"].notna()].copy()
    frame["year_num"] = frame["year_num"].astype(int)
    frame["enrollment"] = frame["enrollment"].astype(float)
    frame["total_susp"] = frame["total_susp"].astype(float)
    aggregated = (
        frame.groupby(["year", "year_num", "school_id", "level", "setting"], as_index=False)
        .agg(
            enrollment=("enrollment", "max"),
            total_suspensions=("total_susp", "sum"),
        )
    )
    aggregated["rate"] = safe_div(aggregated["total_suspensions"], aggregated["enrollment"])
    return aggregated


def build_summary(per_school: pd.DataFrame) -> list[dict[str, object]]:
    grouped = (
        per_school.groupby(["year", "year_num"], as_index=False)
        .agg(
            n_schools=("school_id", pd.Series.nunique),
            total_enrollment=("enrollment", "sum"),
            total_suspensions=("total_suspensions", "sum"),
            mean_rate=("rate", "mean"),
            median_rate=("rate", "median"),
        )
        .sort_values("year_num")
    )
    grouped["overall_rate"] = safe_div(grouped["total_suspensions"], grouped["total_enrollment"])
    summary: list[dict[str, object]] = []
    for row in grouped.itertuples(index=False):
        summary.append(
            {
                "year_num": int(row.year_num),
                "n_schools": int(row.n_schools),
                "total_enrollment": int(round(row.total_enrollment)),
                "total_suspensions": int(round(row.total_suspensions)),
                "mean_rate": float(row.mean_rate),
                "median_rate": float(row.median_rate),
                "overall_rate": float(row.overall_rate),
                "mean_rate_pct": format_percent(row.mean_rate),
                "median_rate_pct": format_percent(row.median_rate),
                "overall_rate_pct": format_percent(row.overall_rate),
            }
        )
    return summary


def build_grade_setting(per_school: pd.DataFrame) -> list[dict[str, object]]:
    filtered = per_school[
        per_school["level"].isin(GRADE_LEVELS) & per_school["setting"].isin(SETTINGS)
    ].copy()
    grouped = (
        filtered.groupby(["year_num", "level", "setting"], as_index=False)
        .agg(
            n_schools=("school_id", pd.Series.nunique),
            total_enrollment=("enrollment", "sum"),
            total_suspensions=("total_suspensions", "sum"),
        )
        .sort_values(["year_num", "level", "setting"])
    )
    grouped["overall_rate"] = safe_div(grouped["total_suspensions"], grouped["total_enrollment"])
    output: list[dict[str, object]] = []
    for row in grouped.itertuples(index=False):
        output.append(
            {
                "year_num": int(row.year_num),
                "level": row.level,
                "setting": row.setting,
                "n_schools": int(row.n_schools),
                "total_enrollment": int(round(row.total_enrollment)),
                "total_suspensions": int(round(row.total_suspensions)),
                "overall_rate": float(row.overall_rate),
            }
        )
    return output


def build_comparison(per_school: pd.DataFrame) -> list[dict[str, object]]:
    filtered = per_school[
        per_school["level"].isin(GRADE_LEVELS) & per_school["setting"].isin(SETTINGS)
    ].copy()
    grouped = (
        filtered.groupby(["year_num", "level", "setting"], as_index=False)
        .agg(
            n_schools=("school_id", pd.Series.nunique),
            overall_rate=("total_suspensions", "sum"),
            total_enrollment=("enrollment", "sum"),
            mean_rate=("rate", "mean"),
            median_rate=("rate", "median"),
        )
    )
    grouped["overall_rate"] = safe_div(grouped["overall_rate"], grouped["total_enrollment"])
    rows: list[dict[str, object]] = []
    for (year_num, level), chunk in grouped.groupby(["year_num", "level"], sort=True):
        record: dict[str, object] = {"year_num": int(year_num), "level": level}
        for row in chunk.itertuples(index=False):
            suffix = row.setting.replace(" ", "-")
            record[f"n_schools_{suffix}"] = int(row.n_schools)
            record[f"overall_rate_pct_{suffix}"] = format_percent(row.overall_rate)
            record[f"mean_rate_pct_{suffix}"] = format_percent(row.mean_rate)
            record[f"median_rate_pct_{suffix}"] = format_percent(row.median_rate)
        rows.append(record)
    return rows


def build_pareto(per_school: pd.DataFrame) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for year_num, group in per_school.groupby("year_num", sort=True):
        totals = group.groupby("school_id", as_index=False)["total_suspensions"].sum()
        totals = totals.sort_values("total_suspensions", ascending=False).reset_index(drop=True)
        n_schools = int(len(totals))
        if n_schools == 0:
            continue
        total_measure = float(totals["total_suspensions"].sum())
        if total_measure <= 0:
            continue
        for pct in (0.05, 0.10, 0.20):
            cutoff = max(1, int(np.floor(pct * n_schools)))
            top_sum = float(totals.loc[: cutoff - 1, "total_suspensions"].sum())
            share = top_sum / total_measure
            records.append(
                {
                    "year_num": int(year_num),
                    "top_label": f"Top {int(pct * 100)}%",
                    "share_pct": format_percent(share, decimals=1),
                    "top_schools": int(cutoff),
                    "total_schools": int(n_schools),
                    "measure_type": "total_susp",
                }
            )
    return records


def build_payload() -> dict[str, object]:
    per_school = load_overview_frame()
    summary = build_summary(per_school)
    grade_setting = build_grade_setting(per_school)
    comparison = build_comparison(per_school)
    pareto = build_pareto(per_school)
    return {
        "summary": summary,
        "gradeSetting": grade_setting,
        "comparison": comparison,
        "pareto": pareto,
    }


def write_outputs(payload: dict[str, object]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    banner = "// Auto-generated by build_suspension_overview.py. Do not edit manually.\n"
    assignment = "window.SUSPENSION_DASHBOARD_DATA = " + json.dumps(payload, ensure_ascii=False) + ";\n"
    with JS_PATH.open("w", encoding="utf-8") as f:
        f.write(banner)
        f.write(assignment)


def main() -> None:
    payload = build_payload()
    write_outputs(payload)
    print(f"Updated {JSON_PATH.relative_to(PROJECT_ROOT)} and {JS_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
