"""
This module builds a JSON payload for the REACH dashboard.  It is based on
``dashboard/build_dashboard_data.py`` from the user's repository but has been
extended to support additional demographic quartile dimensions, locale
breakdowns and tail‐concentration metrics.  The goal of this version is to
retain backwards compatibility with the original payload while providing
additional fields requested by the user.

Changes from the original script include:

* **Additional dimension columns** – The original build script only
  considered the Black enrollment quartile when grouping and aggregating
  suspension counts.  Older analyses demonstrated that quartile rates were
  available for White and Hispanic students as well, and the v6 features
  dataset stores these fields under ``white_prop_q_label`` and
  ``hispanic_prop_q_label``【555959540918920†L107-L129】.  To align the
  dashboard data with prior analyses, this script now includes all three
  quartile labels in the ``DIMENSION_COLUMNS`` list.  It also attempts to
  include a simple locale indicator (``locale_simple``) whenever present in
  the source data; this allows the front end to explore suspension
  disparities across city, suburban, town and rural contexts【555959540918920†L84-L89】.  If the
  column is missing, the script falls back to ``Unknown``.

* **Enhanced meta section** – The filter metadata now exposes lists of
  available quartiles for Black, White and Hispanic student enrollment as
  ``black_quartiles``, ``white_quartiles`` and ``hispanic_quartiles``.
  Additionally, a ``locales`` entry provides the available ``locale_simple``
  values (e.g. City, Suburb, Town, Rural).  These lists populate filter
  controls in the HTML dashboard.

* **Tail concentration metrics** – To support the requested tail
  concentration (Pareto) analysis, a new helper computes the share of total
  suspensions attributable to the top 5%, 10% and 20% of schools by
  suspension count.  This metric is calculated per academic year and
  student group using the aggregated per–school totals.  The results are
  included in the payload as a top–level ``tail_concentrations`` list
  containing dictionaries with ``academic_year``, ``student_group``,
  ``threshold`` (0.05, 0.1 or 0.2) and ``share`` fields.

Usage: run this module directly with ``python modified_build_dashboard_data.py``.
It will read the long–form suspension parquet file (``data-stage/susp_v6_long.parquet``)
and, where available, join the corresponding features file
(``data-stage/susp_v6_features.parquet``) to pull in locale information.  The
output JSON file is written to ``dashboard/data/dashboard_data.json``.  If
additional columns are missing, they will be filled with ``"Unknown"`` to
prevent front-end errors.
"""

from __future__ import annotations

import os
from pathlib import Path

# -------- Robust project root resolution --------------------------------------
def resolve_project_root() -> Path:
    """
    Order of precedence:
      1) REACH_PROJECT_ROOT environment variable (if set)
      2) Directory of this file (when __file__ exists)
      3) Current working directory (interactive shells / reticulate)
    """
    env_root = os.getenv("REACH_PROJECT_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()
    try:
        return Path(__file__).resolve().parent
    except NameError:
        # __file__ is undefined in REPL / notebooks / some reticulate calls
        return Path.cwd().resolve()

PROJECT_ROOT = resolve_project_root()

# -------- Standardized paths ---------------------------------------------------
DATA_STAGE   = PROJECT_ROOT / "data-stage"
LONG_PATH    = DATA_STAGE / "susp_v6_long.parquet"
FEATURES_PATH= DATA_STAGE / "susp_v6_features.parquet"
OUTPUT_PATH  = PROJECT_ROOT / "dashboard" / "data" / "dashboard_data.json"


# Dimension columns now include additional quartile labels and locale.
# The front end uses these to generate filter controls.
DIMENSION_COLUMNS: list[str] = [
    "academic_year",
    "school_level_final",
    "reporting_category_description",
    "black_prop_q_label",
    "white_prop_q_label",
    "hispanic_prop_q_label",
    "locale_simple",  # may be missing; filled later
]

# Identifiers and reason column remain unchanged.
SCHOOL_IDENTIFIER = "school_code"
REASON_COLUMN = "reason_lab"

# Numeric columns aggregated in the dashboard.
NUMERIC_COLUMNS: list[str] = [
    "total_suspensions",
    "unduplicated_count_of_students_suspended_total",
    "cumulative_enrollment",
]

# All string columns (dimensions plus identifiers and reason).
STRING_COLUMNS: list[str] = DIMENSION_COLUMNS + [SCHOOL_IDENTIFIER, REASON_COLUMN]


@dataclass(frozen=True)
class AggregatedData:
    """Container that holds all pieces of the dashboard payload."""

    meta: dict[str, list[str]]
    overall: list[dict[str, object]]
    reasons: list[dict[str, object]]
    tail_concentrations: list[dict[str, object]]


# ---------------------------------------------------------------------------
# Data preparation helpers
# ---------------------------------------------------------------------------

def read_source(long_path: Path, features_path: Path, columns: Sequence[str]) -> pd.DataFrame:
    """Load the long-form parquet file and join features for locale and quartiles.

    This function reads only the requested columns from the long-form file and
    attempts to join locale information from the features file when possible.
    If the features file cannot be read or lacks the expected columns,
    locale values default to "Unknown".  Quartile labels are assumed to be
    present in the long-form file; if not, they will also be filled with
    "Unknown" later.
    """

    # Load the long-form suspension data
    long_df = pd.read_parquet(long_path, columns=list(columns))

    # Attempt to join locale information
    if features_path.exists():
        try:
            feats = pd.read_parquet(
                features_path,
                columns=[SCHOOL_IDENTIFIER, "academic_year", "locale_simple"],
            )
            feats["academic_year"] = feats["academic_year"].astype("string")
            feats[SCHOOL_IDENTIFIER] = feats[SCHOOL_IDENTIFIER].astype("string")
            long_df = long_df.merge(
                feats, on=[SCHOOL_IDENTIFIER, "academic_year"], how="left"
            )
        except Exception:
            # If reading or joining fails, fill locale later
            long_df["locale_simple"] = np.nan
    else:
        long_df["locale_simple"] = np.nan
    return long_df


def normalise_strings(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Cast selected columns to pandas' string dtype with sensible defaults."""

    output = frame.copy()
    for column in columns:
        output[column] = output[column].astype("string")
        if column != SCHOOL_IDENTIFIER:
            # Unknown values are preferable to NaN for front-end filters.
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

    # Baseline rates for gap calculations (using White and Total groups)
    key_cols = [c for c in DIMENSION_COLUMNS if c != "reporting_category_description"]
    white_baseline = (
        overall[overall["reporting_category_description"] == "White"]
        .set_index(key_cols)
        .loc[:, ["suspension_rate"]]
        .rename(columns={"suspension_rate": "white_rate"})
    )
    total_baseline = (
        overall[overall["reporting_category_description"] == "Total"]
        .set_index(key_cols)
        .loc[:, ["suspension_rate"]]
        .rename(columns={"suspension_rate": "overall_rate"})
    )
    disparity = (
        overall.set_index(DIMENSION_COLUMNS)
        .join(white_baseline, on=key_cols)
        .join(total_baseline, on=key_cols)
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
    # Approximate student rate using share of total suspensions
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
    return reason_totals[ordered_columns]


def compute_tail_concentration(per_school_overall: pd.DataFrame) -> list[dict[str, object]]:
    """Compute tail concentration metrics (Pareto shares).

    For each academic year and student group, this function calculates the
    proportion of total suspensions accounted for by the top 5%, 10% and 20%
    of schools, ranked by total suspensions.  The input must be the
    per-school overall DataFrame produced by ``aggregate_per_school`` and
    should contain the columns listed in ``DIMENSION_COLUMNS`` and
    ``total_suspensions``.
    """
    results: list[dict[str, object]] = []
    thresholds = [0.05, 0.10, 0.20]
    # Iterate over each academic year and student group
    for (year, group) in per_school_overall[["academic_year", "reporting_category_description"]].drop_duplicates().itertuples(index=False, name=None):
        subset = per_school_overall[
            (per_school_overall["academic_year"] == year)
            & (per_school_overall["reporting_category_description"] == group)
        ]
        if subset.empty:
            continue
        # Sort by total suspensions descending
        subset_sorted = subset.sort_values("total_suspensions", ascending=False)
        total_susp = subset_sorted["total_suspensions"].sum()
        n_schools = len(subset_sorted)
        for thr in thresholds:
            k = max(int(np.floor(n_schools * thr)), 1)
            top_total = subset_sorted.iloc[:k]["total_suspensions"].sum()
            share = (top_total / total_susp) if total_susp > 0 else np.nan
            results.append(
                {
                    "academic_year": year,
                    "student_group": group,
                    "threshold": thr,
                    "share": round(share, 4) if share == share else None,
                }
            )
    return results


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
        "school_levels": sorted(df["school_level_final"].dropna().unique().tolist()),
        "student_groups": sorted(df["reporting_category_description"].dropna().unique().tolist()),
        "black_quartiles": sorted(df["black_prop_q_label"].dropna().unique().tolist()),
        "white_quartiles": sorted(df["white_prop_q_label"].dropna().unique().tolist()),
        "hispanic_quartiles": sorted(df["hispanic_prop_q_label"].dropna().unique().tolist()),
        "locales": sorted(df["locale_simple"].dropna().unique().tolist()),
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
    # Sort and round numeric columns for compact JSON
    overall_summary = round_numeric_columns(
        overall_summary.sort_values(DIMENSION_COLUMNS).reset_index(drop=True)
    )
    reason_summary = round_numeric_columns(
        reason_summary.sort_values(DIMENSION_COLUMNS + [REASON_COLUMN]).reset_index(
            drop=True
        )
    )
    tail_conc = compute_tail_concentration(per_school_overall)
    return AggregatedData(
        meta=build_meta(df),
        overall=overall_summary.to_dict(orient="records"),
        reasons=reason_summary.to_dict(orient="records"),
        tail_concentrations=tail_conc,
    )


def main() -> None:
    """Entry point for command-line execution."""

    columns_to_load = set(DIMENSION_COLUMNS + [SCHOOL_IDENTIFIER, REASON_COLUMN])
    columns_to_load.update(NUMERIC_COLUMNS)
    # Read data and join features
    df = read_source(LONG_PATH, FEATURES_PATH, sorted(columns_to_load))
    df = normalise_strings(df, STRING_COLUMNS)
    df = fill_numeric_na(df, NUMERIC_COLUMNS)
    # Build payload and write to JSON
    payload = build_payload(df)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(sanitise_for_json(payload.__dict__), indent=2, allow_nan=False)
    )
    print(f"Wrote {OUTPUT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
