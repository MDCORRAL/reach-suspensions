# Build aggregated suspension metrics by race and academic year.
"""Export the ``rates_by_race_year`` summary used by the dashboards.

The implementation mirrors the logic in
``Analysis/18_comprehensive_suspension_rates_analysis.R`` for the
``rates_by_race_year`` worksheet.  It harmonises race labels, filters to
campus-level observations, attaches traditional/non-traditional flags, and
aggregates suspension metrics by academic year and race/ethnicity.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_STAGE = PROJECT_ROOT / "data-stage"
LONG_PATH = DATA_STAGE / "susp_v6_long.parquet"
FEATURES_PATH = DATA_STAGE / "susp_v6_features.parquet"
OUTPUT_PATH = PROJECT_ROOT / "dashboard" / "data" / "rates_by_race_year.json"

SPECIAL_SCHOOL_CODES = {"0000000", "0000001"}


@dataclass
class RateRecord:
    year: str
    race_ethnicity: str

    school_level: str
    setting: str
    n_schools: int
    n_records: int
    total_enrollment: float
    total_suspensions: float
    mean_rate: float
    median_rate: float
    pooled_rate: float

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        # Rounded values help keep the JSON compact while matching R output
        for field in ("mean_rate", "median_rate", "pooled_rate"):
            data[field] = round(float(data[field]), 10)
        data["total_enrollment"] = float(data["total_enrollment"])
        data["total_suspensions"] = float(data["total_suspensions"])
        return data


RACE_MAP = {
    "ta": "All Students",
    "total": "All Students",
    "all students": "All Students",
    "all_students": "All Students",
    "ra": "Asian",
    "asian": "Asian",
    "rb": "Black/African American",
    "black": "Black/African American",
    "african american": "Black/African American",
    "black/african american": "Black/African American",
    "african_american": "Black/African American",
    "rf": "Filipino",
    "filipino": "Filipino",
    "rh": "Hispanic/Latino",
    "rl": "Hispanic/Latino",
    "hispanic": "Hispanic/Latino",
    "latino": "Hispanic/Latino",
    "hispanic/latino": "Hispanic/Latino",
    "hispanic_latino": "Hispanic/Latino",
    "ri": "American Indian/Alaska Native",
    "american indian": "American Indian/Alaska Native",
    "alaska native": "American Indian/Alaska Native",
    "american indian/alaska native": "American Indian/Alaska Native",
    "native american": "American Indian/Alaska Native",
    "rp": "Native Hawaiian/Pacific Islander",
    "pacific islander": "Native Hawaiian/Pacific Islander",
    "native hawaiian": "Native Hawaiian/Pacific Islander",
    "rt": "Two or More Races",
    "two or more": "Two or More Races",
    "two or more races": "Two or More Races",
    "multirace": "Two or More Races",
    "multiple": "Two or More Races",
    "rw": "White",
    "white": "White",
    "rd": "Not Reported",
    "not reported": "Not Reported",
    "not_reported": "Not Reported",
    "notreported": "Not Reported",
}


def canon_race_label(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string").str.strip().str.lower()
    mapped = cleaned.map(RACE_MAP)
    is_sex = cleaned.str.contains("gender|male|female", na=False)
    mapped = mapped.mask(is_sex, "Sex")
    mapped = mapped.where(~cleaned.isna(), other=np.nan)
    return mapped


def norm_quartile(series: pd.Series) -> pd.Series:
    mapping = {
        "Q1": "Q1",
        "1": "Q1",
        "Q2": "Q2",
        "2": "Q2",
        "Q3": "Q3",
        "3": "Q3",
        "Q4": "Q4",
        "4": "Q4",
        "Q1 (Lowest % Black)": "Q1",
        "Q1 (Lowest % White)": "Q1",
        "Q1 (Lowest % Hispanic/Latino)": "Q1",
        "Q4 (Highest % Black)": "Q4",
        "Q4 (Highest % White)": "Q4",
        "Q4 (Highest % Hispanic/Latino)": "Q4",
    }
    values = series.astype("string")
    # Preserve missing values instead of converting to the string "<NA>"
    values = values.where(~values.isna(), other=np.nan)
    return values.map(mapping)


def safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    denom = denom.replace({0: np.nan})
    return numer / denom


def load_long_form() -> pd.DataFrame:
    cols = [
        "academic_year",
        "subgroup",
        "cumulative_enrollment",
        "total_suspensions",
        "unduplicated_count_of_students_suspended_total",
        "white_prop_q_label",
        "black_prop_q_label",
        "hispanic_prop_q_label",
        "aggregate_level",
        "school_code",
        "county_code",
        "district_code",
        "school_level",
    ]
    df = pd.read_parquet(LONG_PATH, columns=cols)
    for code_col, width in ("county_code", 2), ("district_code", 5), ("school_code", 7):
        df[code_col] = df[code_col].astype("string").str.zfill(width)
    df["aggregate_level"] = df["aggregate_level"].astype("string")
    df = df[df["aggregate_level"].str.lower().isin({"s", "school"})]
    df = df[~df["school_code"].isin(SPECIAL_SCHOOL_CODES)]
    df["race_ethnicity"] = canon_race_label(df["subgroup"])
    df["white_q"] = norm_quartile(df["white_prop_q_label"])
    df["black_q"] = norm_quartile(df["black_prop_q_label"])
    df["hispanic_q"] = norm_quartile(df["hispanic_prop_q_label"])
    df["enrollment"] = pd.to_numeric(df["cumulative_enrollment"], errors="coerce")
    df["total_suspensions"] = pd.to_numeric(df["total_suspensions"], errors="coerce")
    df["undup_suspensions"] = pd.to_numeric(
        df["unduplicated_count_of_students_suspended_total"], errors="coerce"
    )
    df = df[
        df["race_ethnicity"].notna()
        & df["enrollment"].notna()
        & df["total_suspensions"].notna()
        & (df["enrollment"] > 0)
        & (df["total_suspensions"] >= 0)
        & df["white_q"].notna()
        & df["black_q"].notna()
        & df["hispanic_q"].notna()
    ]
    df["year"] = df["academic_year"].astype("string")
    df["school_level"] = df["school_level"].astype("string")
    df["suspension_rate"] = safe_div(df["total_suspensions"], df["enrollment"])
    df["undup_rate"] = safe_div(df["undup_suspensions"], df["enrollment"])
    return df


def load_traditional_flags() -> pd.DataFrame:
    if not FEATURES_PATH.exists():
        return pd.DataFrame(columns=["school_code", "year", "is_traditional"])
    feats = pd.read_parquet(
        FEATURES_PATH, columns=["school_code", "academic_year", "is_traditional"]
    )
    feats["school_code"] = feats["school_code"].astype("string").str.zfill(7)
    feats["year"] = feats["academic_year"].astype("string")
    feats["is_traditional"] = feats["is_traditional"].fillna(False)
    feats = feats.drop(columns=["academic_year"])
    return feats.drop_duplicates()


def attach_traditional(df: pd.DataFrame) -> pd.DataFrame:
    flags = load_traditional_flags()
    merged = df.merge(flags, on=["school_code", "year"], how="left")
    merged["is_traditional"] = merged["is_traditional"].fillna(True)
    merged["setting"] = np.where(merged["is_traditional"], "Traditional", "Non-traditional")
    return merged


def summarise_rates(df: pd.DataFrame) -> list[RateRecord]:

    df = df.copy()
    df["school_level"] = df["school_level"].fillna("Unknown")
    df["setting"] = df["setting"].fillna("Traditional")
    grouped = (
        df.groupby(["year", "race_ethnicity", "school_level", "setting"], dropna=False)
        .agg(
            n_schools=("school_code", pd.Series.nunique),
            n_records=("school_code", "size"),
            total_enrollment=("enrollment", "sum"),
            total_suspensions=("total_suspensions", "sum"),
            mean_rate=("suspension_rate", "mean"),
            median_rate=("suspension_rate", "median"),
        )
        .reset_index()
        .sort_values(["year", "race_ethnicity", "school_level", "setting"])
    )
    grouped["pooled_rate"] = safe_div(grouped["total_suspensions"], grouped["total_enrollment"])
    records: list[RateRecord] = []
    for row in grouped.itertuples(index=False):
        records.append(
            RateRecord(
                year=row.year,
                race_ethnicity=row.race_ethnicity,
                school_level=row.school_level,
                setting=row.setting,
                n_schools=int(row.n_schools),
                n_records=int(row.n_records),
                total_enrollment=row.total_enrollment,
                total_suspensions=row.total_suspensions,
                mean_rate=row.mean_rate,
                median_rate=row.median_rate,
                pooled_rate=row.pooled_rate,
            )
        )
    return records


def build_payload() -> dict[str, object]:
    long_df = load_long_form()
    enriched = attach_traditional(long_df)
    records = summarise_rates(enriched)
    return {"rates_by_race_year": [rec.to_dict() for rec in records]}


def main() -> None:
    payload = build_payload()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
