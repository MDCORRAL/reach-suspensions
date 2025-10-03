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
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = THIS_DIR.parent
if str(THIS_DIR) not in sys.path:
    sys.path.append(str(THIS_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from data_sources import prepare_analysis_frame as load_base_frame

OUTPUTS_DIR = PROJECT_ROOT / "outputs"
JSON_PATH = PROJECT_ROOT / "dashboard" / "pareto_grade_setting_payload.json"
TARGET_CSV_NAME = "pareto_grade_setting_slide_ready.csv"
TOP_PCTS: tuple[float, ...] = (0.05, 0.10, 0.20)


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


def prepare_grade_setting_frame() -> pd.DataFrame:
    frame = load_base_frame()
    frame = frame.loc[(frame["level"] != "Unknown") & (frame["setting"] != "Unknown")].copy()
    frame = frame.rename(columns={"year_label": "year"})
    aggregated = (
        frame.groupby(["school_id", "year", "year_num", "level", "setting"], as_index=False)
        .agg(
            enrollment=("enrollment", "max"),
            measure=("total_susp", "sum"),
        )
    )
    aggregated["enrollment"] = aggregated["enrollment"].astype(float)
    aggregated["measure"] = aggregated["measure"].astype(float)
    return aggregated


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
    frame = prepare_grade_setting_frame()
    rows = compute_slide_rows(frame)
    csv_path = write_outputs(rows)
    print(f"Wrote {len(rows)} slide-ready rows to {csv_path}")
    print(f"Updated {JSON_PATH}")


if __name__ == "__main__":
    main()
