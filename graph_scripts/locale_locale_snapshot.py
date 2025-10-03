#!/usr/bin/env python3
"""Generate a 2023-24 locale snapshot chart using statewide trend utilities."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq


def load_statewide_module() -> Any:
    script_path = Path(__file__).resolve().with_name("06_statewide_trends.py")
    spec = importlib.util.spec_from_file_location("statewide_trends", script_path)
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:  # pragma: no cover - defensive guard
        raise RuntimeError("Unable to load statewide trends module")
    spec.loader.exec_module(module)
    return module


def load_minimal_joined(module: Any) -> pd.DataFrame:
    columns = [
        "school_code",
        "academic_year",
        "subgroup",
        "cumulative_enrollment",
        "total_suspensions",
        "locale_simple",
        "aggregate_level",
    ]
    susp = pq.read_table(module.LONG_PATH, columns=columns).to_pandas()
    susp.columns = module.clean_columns(susp.columns)
    susp["aggregate_level"] = susp["aggregate_level"].astype("string")
    susp = susp[susp["aggregate_level"].str.lower().isin({"s", "school"})]
    susp = susp.drop(columns=["aggregate_level"])
    susp["school_code"] = susp["school_code"].astype(str).str.strip().str.zfill(7)
    susp = susp[~susp["school_code"].isin(module.SPECIAL_SCHOOL_CODES)]
    susp["academic_year"] = susp["academic_year"].astype(str).str.strip()
    susp["subgroup"] = susp["subgroup"].astype("string").str.strip()
    susp["locale_simple"] = susp["locale_simple"].astype("string").str.strip()
    susp["cumulative_enrollment"] = pd.to_numeric(
        susp["cumulative_enrollment"], errors="coerce"
    )
    susp["total_suspensions"] = pd.to_numeric(
        susp["total_suspensions"], errors="coerce"
    )

    feat = pq.read_table(
        module.FEAT_PATH, columns=["school_code", "academic_year", "is_traditional"]
    ).to_pandas()
    feat.columns = module.clean_columns(feat.columns)
    feat["school_code"] = feat["school_code"].astype(str).str.strip().str.zfill(7)
    feat["academic_year"] = feat["academic_year"].astype(str).str.strip()
    feat["is_traditional"] = feat["is_traditional"].astype("boolean")

    joined = susp.merge(feat, on=["school_code", "academic_year"], how="left")
    joined["is_traditional"] = (
        joined["is_traditional"].astype("boolean").fillna(module.DEFAULT_IS_TRADITIONAL)
    )
    joined["is_traditional"] = joined["is_traditional"].astype(bool)
    joined["setting"] = joined["is_traditional"].map(
        {True: "Traditional", False: "Non-traditional"}
    )
    joined["setting"] = joined["setting"].astype("string")

    return joined


def main() -> None:
    module = load_statewide_module()
    joined = load_minimal_joined(module)
    base = module.prepare_base_data(joined)
    _, latest_year, path = module.build_locale_snapshot_figure(base, render=True)
    if path is None:
        raise RuntimeError("Locale snapshot figure was not generated.")
    print(f"Generated locale snapshot for {latest_year}: {path}")


if __name__ == "__main__":
    main()
