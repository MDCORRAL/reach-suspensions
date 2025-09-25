"""Generate Pareto grade-setting payload for the tail concentration dashboard.

This helper mirrors the slide-ready export produced by
`Analysis/17_tail_by_grade-school_concentration_analysis.R` but is implemented
in Python so it can run in environments without an R toolchain.  The script
loads the long-form suspension data and school feature table, replicates the
same grade-level and school-type mappings, computes top-share statistics for
the top 5/10/20 percent of schools, and emits both the CSV export and the JSON
payload consumed by the HTML dashboard.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
LONG_PATH = PROJECT_ROOT / "data-stage" / "susp_v6_long.parquet"
FEATURES_PATH = PROJECT_ROOT / "data-stage" / "susp_v6_features.parquet"
JSON_PATH = PROJECT_ROOT / "dashboard" / "pareto_grade_setting_payload.json"
TARGET_CSV_NAME = "pareto_grade_setting_slide_ready.csv"
TOP_PCTS: tuple[float, ...] = (0.05, 0.10, 0.20)

YEAR_PATTERN = re.compile(r"^(\d{4})")
SETTING_ALT_PATTERN = re.compile(r"alternative|charter|continuation|community", re.IGNORECASE)
GRADE_ELEMENTARY = re.compile(r"elementary|elem|primary|k.*5|k.*6", re.IGNORECASE)
GRADE_MIDDLE = re.compile(r"middle|junior|intermediate|6.*8|7.*8", re.IGNORECASE)
GRADE_HIGH = re.compile(r"high|secondary|9.*12|senior", re.IGNORECASE)
GRADE_ALT = re.compile(r"adult|continuation", re.IGNORECASE)


@dataclass(frozen=True)
class SlideRow:
    year_num: int
    level: str
    setting: str
    top_pct: float
    top_label: str
    top_schools: int
    total_schools: int
    top_share: float
    share_pct: str
    slide_text: str

    def to_dict(self) -> dict[str, object]:
        return {
            "year_num": self.year_num,
            "level": self.level,
            "setting": self.setting,
            "top_pct": self.top_pct,
            "top_label": self.top_label,
            "top_schools": self.top_schools,
            "total_schools": self.total_schools,
            "top_share": self.top_share,
            "share_pct": self.share_pct,
            "slide_text": self.slide_text,
        }


def extract_year(text: object) -> int | None:
    if text is None or (isinstance(text, float) and math.isnan(text)):
        return None
    match = YEAR_PATTERN.search(str(text))
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
        return "Unknown"
    text = str(raw).strip()
    if not text:
        return "Unknown"
    lowered = text.lower()
    if "traditional" in lowered:
        return "Traditional"
    if SETTING_ALT_PATTERN.search(lowered):
        return "Non-traditional"
    return "Other"


def load_suspension_data() -> pd.DataFrame:
    long_cols = [
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
    table = pq.read_table(LONG_PATH, columns=long_cols)
    df = table.to_pandas()
    mask = df["subgroup"].str.lower().isin({"total", "all students", "ta"})
    df = df.loc[mask].copy()
    df["school_id"] = df["school_code"].astype("string")
    df["school_name"] = df["school_name"].astype("string")
    df["year"] = df["academic_year"].astype("string")
    df["year_num"] = df["year"].map(extract_year)
    df["level_raw"] = df["school_level"].astype("string")
    df["setting_raw"] = df["school_type"].astype("string")
    df["enrollment"] = pd.to_numeric(df["cumulative_enrollment"], errors="coerce")
    df["measure"] = pd.to_numeric(df["total_suspensions"], errors="coerce")
    df = df.dropna(subset=["school_id", "year_num", "enrollment", "measure"])
    df = df.loc[(df["enrollment"] > 0) & (df["measure"] >= 0)]
    return df


def load_feature_flags() -> pd.DataFrame:
    feat_cols = ["school_code", "academic_year", "is_traditional", "school_type"]
    table = pq.read_table(FEATURES_PATH, columns=feat_cols)
    df = table.to_pandas()
    df["school_code"] = df["school_code"].astype("string")
    df["academic_year"] = df["academic_year"].astype("string")
    return df


def prepare_analysis_frame() -> pd.DataFrame:
    susp = load_suspension_data()
    feats = load_feature_flags()
    merged = susp.merge(
        feats,
        left_on=["school_id", "year"],
        right_on=["school_code", "academic_year"],
        how="left",
        suffixes=("", "_feat"),
    )
    merged["setting"] = [
        map_setting(raw, is_trad)
        for raw, is_trad in zip(merged["school_type_feat"].fillna(merged["setting_raw"]), merged["is_traditional"])
    ]
    merged["level"] = [map_grade_level(value) for value in merged["level_raw"]]
    analysis = merged[["school_id", "year_num", "level", "setting", "measure"]].copy()
    analysis = analysis.replace({np.nan: None})
    analysis = analysis.loc[(analysis["level"] != "Unknown") & (analysis["setting"] != "Unknown")]
    return analysis


def compute_slide_rows(frame: pd.DataFrame) -> list[SlideRow]:
    rows: list[SlideRow] = []
    grouped = frame.groupby(["year_num", "level", "setting"], sort=True)
    for (year_num, level, setting), group in grouped:
        school_totals = group.groupby("school_id", as_index=False)["measure"].sum()
        n_schools = int(len(school_totals))
        if n_schools < 5:
            continue
        school_totals = school_totals.sort_values("measure", ascending=False).reset_index(drop=True)
        total_measure = float(school_totals["measure"].sum())
        if total_measure <= 0:
            continue
        for pct in TOP_PCTS:
            cutoff = max(1, int(math.floor(pct * n_schools)))
            top_sum = float(school_totals.loc[: cutoff - 1, "measure"].sum())
            share = top_sum / total_measure
            share_pct = f"{share * 100:.1f}%"
            top_label = f"Top {int(pct * 100)}%"
            slide_text = (
                f"In {year_num} ({level} {setting} schools), {top_label} "
                f"accounted for {share_pct} of suspension events."
            )
            rows.append(
                SlideRow(
                    year_num=int(year_num),
                    level=level,
                    setting=setting,
                    top_pct=pct,
                    top_label=top_label,
                    top_schools=cutoff,
                    total_schools=n_schools,
                    top_share=share,
                    share_pct=share_pct,
                    slide_text=slide_text,
                )
            )
    return rows


def write_outputs(rows: Iterable[SlideRow]) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = OUTPUTS_DIR / f"tail_by_grade_school_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / TARGET_CSV_NAME
    df = pd.DataFrame([row.to_dict() for row in rows])
    df.sort_values(["year_num", "level", "setting", "top_pct"]).to_csv(csv_path, index=False)
    payload = {
        "gradeSetting": [row.to_dict() for row in rows],
        "source": {
            "csv_path": str(csv_path.relative_to(PROJECT_ROOT)),
            "generated_at": datetime.now().isoformat(),
        },
    }
    JSON_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return csv_path


def main() -> None:
    OUTPUTS_DIR.mkdir(exist_ok=True)
    frame = prepare_analysis_frame()
    rows = compute_slide_rows(frame)
    csv_path = write_outputs(rows)
    print(f"Wrote {len(rows)} slide-ready rows to {csv_path}")
    print(f"Updated {JSON_PATH}")


if __name__ == "__main__":
    main()
