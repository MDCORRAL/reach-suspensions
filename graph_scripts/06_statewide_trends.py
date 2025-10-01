#!/usr/bin/env python3
##06_statewide_trends.py
"""Generate statewide suspension trends by race across levels, locales, and quartiles."""

from __future__ import annotations

import argparse
import math
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import sys

try:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    from matplotlib.text import Annotation
except ImportError:
    print(
        "Required package 'matplotlib' is not installed. Install it with `pip install matplotlib`.",
        file=sys.stderr,
    )
    sys.exit(1)

try:
    from adjustText import adjust_text
except ImportError:
    print(
        "Required package 'adjustText' is not installed. Install it with `pip install adjustText`.",
        file=sys.stderr,
    )
    sys.exit(1)

try:
    import numpy as np
except ImportError:
    print(
        "Required package 'numpy' is not installed. Install it with `pip install numpy`.",
        file=sys.stderr,
    )
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print(
        "Required package 'pandas' is not installed. Install it with `pip install pandas`.",
        file=sys.stderr,
    )
    sys.exit(1)

try:
    import pyarrow.parquet as pq
except ImportError:
    print(
        "Required package 'pyarrow' is not installed. Install it with `pip install pyarrow`.",
        file=sys.stderr,
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Project path detection
# ---------------------------------------------------------------------------


def _resolve_project_root() -> Path:
    """Return the repository root even when ``__file__`` is unavailable."""

    env_root = os.environ.get("REACH_SUSPENSIONS_ROOT")
    if env_root:
        candidate = Path(env_root).expanduser()
        if (candidate / "data-stage").exists():
            return candidate.resolve()

    try:
        start = Path(__file__).resolve()
    except NameError:  # pragma: no cover - triggered in interactive sessions
        start = Path.cwd().resolve()

    for candidate in [start, *start.parents]:
        if (candidate / "data-stage").exists() and (candidate / "graph_scripts").exists():
            return candidate

    raise RuntimeError(
        "Unable to locate the project root. Set REACH_SUSPENSIONS_ROOT or run from the repository."
    )


ROOT_DIR = _resolve_project_root()
DATA_STAGE = ROOT_DIR / "data-stage"
OUTPUT_DIR = ROOT_DIR / "outputs" / "graphs"
TEXT_DIR = OUTPUT_DIR / "descriptions"
DIAGNOSTIC_DIR = OUTPUT_DIR / "diagnostics"

LONG_PATH = DATA_STAGE / "susp_v6_long.parquet"
FEAT_PATH = DATA_STAGE / "susp_v6_features.parquet"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TEXT_DIR.mkdir(parents=True, exist_ok=True)
DIAGNOSTIC_DIR.mkdir(parents=True, exist_ok=True)

SPECIAL_SCHOOL_CODES: Set[str] = {"0000000", "0000001"}
DEFAULT_IS_TRADITIONAL = True
SETTINGS_TO_INCLUDE: Optional[Set[str]] = {"Traditional"}
DROP_UNKNOWN_QUARTILES = False


RACE_LEVELS: List[str] = [
    "Black/African American",
    "Hispanic/Latino",
    "White",
    "Asian",
    "American Indian/Alaska Native",
    "Native Hawaiian/Pacific Islander",
    "Filipino",
    "Two or More Races",
]

RACE_PALETTE: Dict[str, str] = {
    "Black/African American": "#D62828",
    "Hispanic/Latino": "#F77F00",
    "White": "#003049",
    "Asian": "#2A9D8F",
    "American Indian/Alaska Native": "#8E7DBE",
    "Native Hawaiian/Pacific Islander": "#577590",
    "Filipino": "#E9C46A",
    "Two or More Races": "#588157",
}

LEVEL_ORDER = ["Elementary", "Middle", "High"]
LOCALE_ORDER = ["City", "Suburban", "Town", "Rural"]
QUARTILE_GROUPS = [
    ("black_prop_q_label", "Q4 (Highest % Black)", "Highest-Black Enrollment Schools"),
    ("white_prop_q_label", "Q4 (Highest % White)", "Highest-White Enrollment Schools"),
]


def slugify_for_filename(value: str) -> str:
    """Return a filesystem-friendly slug for appending to filenames."""

    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in value)
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_")

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--diagnostics-only",
        action="store_true",
        help="Skip plotting and text generation; only refresh diagnostic tables.",
    )
    return parser.parse_args(argv)


def clean_columns(columns: Iterable[str]) -> List[str]:
    """Approximate janitor::clean_names for column headers."""
    cleaned: List[str] = []
    for col in columns:
        col = str(col).strip()
        col = col.replace("'", "").replace('"', "")
        col = col.replace("%", " percent ")
        col = col.replace("/", " ")
        col = col.replace("-", " ")
        col = col.replace("(", " ").replace(")", " ")
        col = col.replace("&", " and ")
        col = col.replace(".", " ")
        col = " ".join(col.split())
        col = col.lower().replace(" ", "_")
        col = "".join(ch for ch in col if ch.isalnum() or ch == "_")
        while "__" in col:
            col = col.replace("__", "_")
        col = col.strip("_")
        cleaned.append(col)
    return cleaned


def standardize_quartile_label(value: object, group: str) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    lowered = text.lower()
    if lowered.startswith("q1") or lowered == "1":
        return f"Q1 (Lowest % {group})"
    if lowered.startswith("q2") or lowered == "2":
        return "Q2"
    if lowered.startswith("q3") or lowered == "3":
        return "Q3"
    if lowered.startswith("q4") or lowered == "4":
        return f"Q4 (Highest % {group})"
    if lowered in {"unknown", "na"}:
        return None
    return text


def standardize_quartile_series(series: pd.Series, group: str) -> pd.Series:
    """Vectorized quartile normalization that mirrors the R logic."""

    working = pd.Series(series, copy=False).astype("string")
    stripped = working.str.strip()

    result = stripped.copy()
    result[stripped.isna() | (stripped == "")] = pd.NA

    lowered = stripped.str.lower()
    mask = lowered.isin({"unknown", "na"})
    result[mask] = pd.NA

    mask = lowered.str.startswith("q1") | (lowered == "1")
    result[mask] = f"Q1 (Lowest % {group})"

    mask = lowered.str.startswith("q2") | (lowered == "2")
    result[mask] = "Q2"

    mask = lowered.str.startswith("q3") | (lowered == "3")
    result[mask] = "Q3"

    mask = lowered.str.startswith("q4") | (lowered == "4")
    result[mask] = f"Q4 (Highest % {group})"

    return result.astype("string")


def load_joined_data() -> pd.DataFrame:
    """Load suspension data joined with school features."""
    if not LONG_PATH.exists() or not FEAT_PATH.exists():
        raise FileNotFoundError("Required parquet files are missing in data-stage/.")

    columns = [
        "school_code",
        "academic_year",
        "subgroup",
        "cumulative_enrollment",
        "total_suspensions",
        "school_level",
        "school_type",
        "school_locale",
        "locale_simple",
        "black_prop_q",
        "black_prop_q_label",
        "white_prop_q",
        "white_prop_q_label",
        "hispanic_prop_q",
        "hispanic_prop_q_label",
        "aggregate_level",
    ]
    susp = pq.read_table(LONG_PATH, columns=columns).to_pandas()
    susp.columns = clean_columns(susp.columns)
    susp["aggregate_level"] = susp["aggregate_level"].astype("string")
    susp = susp[susp["aggregate_level"].str.lower().isin({"s", "school"})]
    susp = susp.drop(columns=["aggregate_level"])
    susp["school_code"] = susp["school_code"].astype(str).str.strip().str.zfill(7)
    susp = susp[~susp["school_code"].isin(SPECIAL_SCHOOL_CODES)]
    susp["academic_year"] = susp["academic_year"].astype(str).str.strip()
    for col in [
        "subgroup",
        "school_level",
        "school_type",
        "school_locale",
        "locale_simple",
    ]:
        susp[col] = susp[col].astype("string").str.strip()
    susp["cumulative_enrollment"] = pd.to_numeric(
        susp["cumulative_enrollment"], errors="coerce"
    )
    susp["total_suspensions"] = pd.to_numeric(
        susp["total_suspensions"], errors="coerce"
    )
    susp["black_prop_q"] = pd.to_numeric(susp["black_prop_q"], errors="coerce").astype(
        "Int64"
    )
    susp["white_prop_q"] = pd.to_numeric(susp["white_prop_q"], errors="coerce").astype(
        "Int64"
    )
    susp["hispanic_prop_q"] = pd.to_numeric(
        susp["hispanic_prop_q"], errors="coerce"
    ).astype("Int64")
    susp["black_prop_q_label"] = standardize_quartile_series(
        susp["black_prop_q_label"], "Black"
    )
    susp["white_prop_q_label"] = standardize_quartile_series(
        susp["white_prop_q_label"], "White"
    )
    susp["hispanic_prop_q_label"] = standardize_quartile_series(
        susp["hispanic_prop_q_label"], "Hispanic/Latino"
    )

    feat = pq.read_table(
        FEAT_PATH, columns=["school_code", "academic_year", "is_traditional"]
    ).to_pandas()
    feat.columns = clean_columns(feat.columns)
    feat["school_code"] = feat["school_code"].astype(str).str.strip().str.zfill(7)
    feat["academic_year"] = feat["academic_year"].astype(str).str.strip()
    feat["is_traditional"] = feat["is_traditional"].astype("boolean")

    joined = susp.merge(feat, on=["school_code", "academic_year"], how="left")
    joined["is_traditional"] = (
        joined["is_traditional"].astype("boolean").fillna(DEFAULT_IS_TRADITIONAL)
    )
    joined["is_traditional"] = joined["is_traditional"].astype(bool)
    joined["setting"] = np.where(joined["is_traditional"], "Traditional", "Non-traditional")

    if DROP_UNKNOWN_QUARTILES:
        mask = (
            joined["black_prop_q_label"].notna()
            & joined["white_prop_q_label"].notna()
            & joined["hispanic_prop_q_label"].notna()
        )
        joined = joined[mask]
    else:
        joined["black_prop_q_label"] = joined["black_prop_q_label"].fillna("Unknown")
        joined["white_prop_q_label"] = joined["white_prop_q_label"].fillna("Unknown")
        joined["hispanic_prop_q_label"] = joined["hispanic_prop_q_label"].fillna(
            "Unknown"
        )

    if SETTINGS_TO_INCLUDE:
        joined = joined[joined["setting"].isin(SETTINGS_TO_INCLUDE)].copy()

    return joined


def prepare_base_data(joined: pd.DataFrame) -> pd.DataFrame:
    base = joined.copy()
    base = base[base["is_traditional"]]
    base = base[base["subgroup"].isin(RACE_LEVELS)]
    base["total_suspensions"] = pd.to_numeric(base["total_suspensions"], errors="coerce")
    base["cumulative_enrollment"] = pd.to_numeric(base["cumulative_enrollment"], errors="coerce")
    base = base.dropna(subset=["total_suspensions", "cumulative_enrollment", "subgroup", "academic_year"])
    base = base[base["cumulative_enrollment"] > 0]
    base["subgroup"] = pd.Categorical(base["subgroup"], categories=RACE_LEVELS, ordered=True)
    return base


def compute_group_rates(base: pd.DataFrame, extra_fields: Sequence[str]) -> pd.DataFrame:
    group_fields = ["academic_year", "subgroup", *extra_fields]
    grouped = (
        base.groupby(group_fields, dropna=False, observed=False)[["total_suspensions", "cumulative_enrollment"]]
        .sum(min_count=1)
        .reset_index()
    )
    grouped = grouped[grouped["cumulative_enrollment"] > 0]
    grouped["rate"] = grouped["total_suspensions"] / grouped["cumulative_enrollment"]
    grouped = grouped.dropna(subset=["rate"])
    grouped = grouped.sort_values([*extra_fields, "subgroup", "academic_year"])
    return grouped


def write_diagnostics(levels: pd.DataFrame) -> None:
    """Persist CSV summaries so R-based workflows can verify alignment."""

    if levels.empty:
        return

    level_path = DIAGNOSTIC_DIR / "statewide_level_rates.csv"
    columns = [
        "academic_year",
        "school_level",
        "subgroup",
        "total_suspensions",
        "cumulative_enrollment",
        "rate",
    ]
    levels.loc[:, columns].sort_values(columns[:3]).to_csv(level_path, index=False)

    elementary = levels[levels["school_level"] == "Elementary"]
    if not elementary.empty:
        focused_path = DIAGNOSTIC_DIR / "statewide_elementary_rates.csv"
        elementary.loc[:, columns].sort_values(columns[:2]).to_csv(
            focused_path, index=False
        )

def apply_reach_style(ax: plt.Axes, year_order: Sequence[str], y_limit: float) -> None:
    ax.set_facecolor("white")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(axis="y", color="#DFE2E5", linewidth=0.8)
    ax.grid(axis="x", color="#DFE2E5", linewidth=0.5, linestyle="--", alpha=0.4)
    ax.set_xticks(range(len(year_order)))
    ax.set_xticklabels(year_order, rotation=45, ha="right")
    ax.tick_params(axis="x", labelsize=10, pad=6)
    ax.tick_params(axis="y", labelsize=10)
    ax.margins(x=0.02)
    ax.set_xlim(-0.35, len(year_order) - 0.65)
    ax.set_ylim(0, y_limit)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y * 100:.1f}%"))


def annotate_points(
    ax: plt.Axes,
    df: pd.DataFrame,
    color: str,
    offset: float,
    label_last_only: bool = False,
) -> List[Annotation]:
    """Label data points while trying to reduce clutter on busy lines."""

    points = df
    if label_last_only:
        max_index = df["year_index"].max()
        points = df[df["year_index"] == max_index]

    annotations: List[Annotation] = []
    y_min, y_max = ax.get_ylim()
    base_offset = abs(offset)
    epsilon = max(base_offset * 0.1, 1e-6)

    for _, row in points.iterrows():
        label = f"{row['rate'] * 100:.1f}%"
        idx = row["year_index"]
        y_val = row["rate"]

        prev_points = df[df["year_index"] < idx]
        next_points = df[df["year_index"] > idx]
        slope_indicator = 0.0
        if not prev_points.empty:
            slope_indicator += y_val - prev_points.iloc[-1]["rate"]
        if not next_points.empty:
            slope_indicator += next_points.iloc[0]["rate"] - y_val

        space_above = max(y_max - y_val, 0.0)
        space_below = max(y_val - y_min, 0.0)

        if slope_indicator < -1e-9:
            direction = -1.0
        elif slope_indicator > 1e-9:
            direction = 1.0
        else:
            direction = 1.0 if space_above >= space_below else -1.0

        available_space = space_above if direction > 0 else space_below
        opposite_space = space_below if direction > 0 else space_above
        if available_space <= epsilon and opposite_space > available_space:
            direction *= -1.0
            available_space = opposite_space

        offset_magnitude = base_offset
        if available_space > 0:
            offset_magnitude = min(base_offset, max(available_space * 0.8, epsilon))

        target_y = y_val + direction * offset_magnitude
        target_y = min(max(target_y, y_min + epsilon), y_max - epsilon)
        va = "bottom" if direction >= 0 else "top"

        annotation = ax.annotate(
            label,
            xy=(idx, y_val),
            xycoords="data",
            xytext=(idx, target_y),
            textcoords="data",
            color=color,
            fontsize=8,
            ha="center",
            va=va,
            fontweight="bold",
            bbox={
                "boxstyle": "round,pad=0.18",
                "facecolor": "white",
                "edgecolor": "none",
                "alpha": 0.85,
            },
            annotation_clip=False,
        )
        annotation.set_clip_on(False)
        annotation.set_zorder(5)
        setattr(annotation, "_initial_xytext", (idx, target_y))
        setattr(annotation, "_annotation_color", color)
        annotations.append(annotation)

    return annotations

def resolve_label_overlaps(ax: plt.Axes, annotations: Sequence[Annotation]) -> None:
    """Nudge annotation labels to avoid overlap and add leader lines when moved."""

    if not annotations:
        return

    if len(annotations) == 1:
        annotations[0].set_visible(True)
        return

    initial_positions = [getattr(annotation, "_initial_xytext", annotation.get_position()) for annotation in annotations]

    max_iterations = max(50, min(180, len(annotations) * 5))

    adjust_text(
        annotations,
        ax=ax,
        only_move={"points": "y", "text": "xy"},
        expand_points=(1.2, 1.35),
        expand_text=(1.02, 1.1),
        force_points=(0.05, 0.2),
        force_text=(0.3, 0.6),
        add_objects=ax.lines,
        autoalign="y",
        lim=max_iterations,
    )

    for annotation in annotations:
        annotation.set_visible(True)

    for annotation, initial in zip(annotations, initial_positions):
        final_pos = annotation.get_position()
        if not isinstance(final_pos, tuple):
            final_pos = tuple(final_pos)

        if (
            abs(final_pos[0] - initial[0]) > 1e-6
            or abs(final_pos[1] - initial[1]) > 1e-6
        ):
            arrow = ax.annotate(
                "",
                xy=annotation.xy,
                xycoords="data",
                xytext=final_pos,
                textcoords="data",
                arrowprops={
                    "arrowstyle": "-",
                    "color": getattr(annotation, "_annotation_color", "#555555"),
                    "linewidth": 0.7,
                    "alpha": 0.75,
                },
                annotation_clip=False,
            )
            if arrow.arrow_patch is not None:
                arrow.arrow_patch.set_zorder(4)


def finalize_figure(
    fig: plt.Figure,
    title: str,
    subtitle: str,
    caption: str,
    handles: Sequence,
    labels: Sequence[str],
    legend_cols: int = 4,
    legend_y: float = 0.91,
) -> None:
    fig.patch.set_facecolor("white")
    if handles:
        fig.legend(
            handles,
            labels,
            loc="upper center",
            bbox_to_anchor=(0.5, legend_y),
            ncol=min(legend_cols, len(labels)),
            frameon=False,
            fontsize=10,
            title="Student group",
            title_fontsize=11,
        )
    fig.text(0.07, 0.965, title, fontsize=20, fontweight="bold", ha="left")
    fig.text(0.07, 0.933, subtitle, fontsize=13, ha="left")
    fig.text(0.07, 0.05, caption, fontsize=10, color="#4A4A4A", ha="left")
    fig.subplots_adjust(left=0.07, right=0.98, top=0.80, bottom=0.18, wspace=0.28, hspace=0.36)


def format_percent(value: float, accuracy: float = 0.1) -> str:
    if value is None or not math.isfinite(value):
        return "N/A"
    return f"{value * 100:.{0 if accuracy >= 1 else 1}f}%"


def build_level_figure(
    base: pd.DataFrame, *, render: bool = True
) -> Tuple[pd.DataFrame, List[str], List[Path]]:

    data = compute_group_rates(base, ["school_level"])
    data = data[data["school_level"].isin(LEVEL_ORDER)].copy()
    data["school_level"] = pd.Categorical(data["school_level"], categories=LEVEL_ORDER, ordered=True)
    year_order = sorted(data["academic_year"].unique())
    data["year_index"] = data["academic_year"].apply(lambda y: year_order.index(y))

    y_max = data["rate"].max() if not data.empty else 0.05
    y_limit = y_max + max(0.01, y_max * 0.18)
    offset = max(0.0015, y_limit * 0.015)

    saved_paths: List[Path] = []

    if render:
        caption = (
            "Source: California statewide suspension data (susp_v6_long.parquet + susp_v6_features.parquet). "
            "Traditional schools only; rates aggregate total suspensions ÷ cumulative enrollment."
        )
        subtitle = (
            "By grade span, 2017-18 through 2023-24 (no statewide reporting in 2020-21)."
        )

        for level in LEVEL_ORDER:
            subset = data[data["school_level"] == level]
            if subset.empty:
                continue

            subset = subset.sort_values(["subgroup", "year_index"])
            fig, ax = plt.subplots(figsize=(10, 9))
            apply_reach_style(ax, year_order, y_limit)
            axis_annotations: List[Annotation] = []
            handles: List[plt.Line2D] = []
            labels: List[str] = []

            for race in RACE_LEVELS:
                race_df = subset[subset["subgroup"] == race]
                if race_df.empty:
                    continue

                line, = ax.plot(
                    race_df["year_index"],
                    race_df["rate"],
                    color=RACE_PALETTE[race],
                    linewidth=2.3,
                    marker="o",
                    markersize=5.3,
                    markeredgecolor="white",
                    markeredgewidth=0.6,
                )
                handles.append(line)
                labels.append(race)

                axis_annotations.extend(
                    annotate_points(
                        ax,
                        race_df,
                        RACE_PALETTE[race],
                        offset,
                        label_last_only=False,
                    )
                )

            resolve_label_overlaps(ax, axis_annotations)
            ax.set_ylabel("Suspension rate", fontsize=11)

            finalize_figure(
                fig,
                title=f"Suspension Rates by Race – {level} Schools",
                subtitle=subtitle,
                caption=caption,
                handles=handles,
                labels=labels,
                legend_cols=4,
                legend_y=0.88,
            )

            filename = f"PY6_statewide_race_trends_by_level_{slugify_for_filename(level)}.png"
            out_path = OUTPUT_DIR / filename
            fig.savefig(out_path, dpi=320)
            plt.close(fig)
            saved_paths.append(out_path)


    return data, year_order, saved_paths


def build_locale_figure(
    base: pd.DataFrame, *, render: bool = True
) -> Tuple[pd.DataFrame, List[str], List[Path]]:

    data = compute_group_rates(base, ["locale_simple"])
    data = data[data["locale_simple"].isin(LOCALE_ORDER)].copy()
    data["locale_simple"] = pd.Categorical(data["locale_simple"], categories=LOCALE_ORDER, ordered=True)
    year_order = sorted(data["academic_year"].unique())
    data["year_index"] = data["academic_year"].apply(lambda y: year_order.index(y))

    y_max = data["rate"].max() if not data.empty else 0.05
    y_limit = y_max + max(0.01, y_max * 0.18)
    offset = max(0.0015, y_limit * 0.015)

    saved_paths: List[Path] = []

    if render:
        caption = (
            "Source: California statewide suspension data (susp_v6_long.parquet + susp_v6_features.parquet). "
            "Traditional schools only; locale grouping uses locale_simple categories."
        )
        subtitle = "By locale, 2017-18 through 2023-24 (no statewide reporting in 2020-21)."

        for locale in LOCALE_ORDER:
            subset = data[data["locale_simple"] == locale]
            if subset.empty:
                continue

            subset = subset.sort_values(["subgroup", "year_index"])
            fig, ax = plt.subplots(figsize=(10, 9))
            apply_reach_style(ax, year_order, y_limit)
            axis_annotations: List[Annotation] = []
            handles: List[plt.Line2D] = []
            labels: List[str] = []

            for race in RACE_LEVELS:
                race_df = subset[subset["subgroup"] == race]
                if race_df.empty:
                    continue

                line, = ax.plot(
                    race_df["year_index"],
                    race_df["rate"],
                    color=RACE_PALETTE[race],
                    linewidth=2.3,
                    marker="o",
                    markersize=5.3,
                    markeredgecolor="white",
                    markeredgewidth=0.6,
                )
                handles.append(line)
                labels.append(race)

                axis_annotations.extend(
                    annotate_points(
                        ax,
                        race_df,
                        RACE_PALETTE[race],
                        offset,
                        label_last_only=False,
                    )
                )

            resolve_label_overlaps(ax, axis_annotations)
            ax.set_ylabel("Suspension rate", fontsize=11)

            finalize_figure(
                fig,
                title=f"Suspension Rates by Race – {locale} Schools",
                subtitle=subtitle,
                caption=caption,
                handles=handles,
                labels=labels,
                legend_cols=4,
                legend_y=0.88,
            )

            filename = f"PY6_statewide_race_trends_by_locale_{slugify_for_filename(locale)}.png"
            out_path = OUTPUT_DIR / filename
            fig.savefig(out_path, dpi=320)
            plt.close(fig)
            saved_paths.append(out_path)

    return data, year_order, saved_paths


def build_quartile_figure(
    base: pd.DataFrame,
    *,
    base_subset: pd.DataFrame | None = None,
    title: str = "Suspension Rates in Highest-Black vs. Highest-White Enrollment Schools",
    subtitle: str = "Traditional schools in top quartile for Black vs. White enrollment, 2017-18 through 2023-24.",
    caption: str = (
        "Source: California statewide suspension data (susp_v6_long.parquet + susp_v6_features.parquet). "
        "Traditional schools only; quartiles reference highest shares of Black or White enrollment."
    ),
    output_filename: str = "statewide_race_trends_quartile_comparison.png",
    render: bool = True,
) -> Tuple[pd.DataFrame, List[str]]:
    working = base_subset if base_subset is not None else base

    pieces: List[pd.DataFrame] = []
    for field, value, label in QUARTILE_GROUPS:
        subset = working[working[field] == value].copy()
        if subset.empty:
            continue
        subset = subset.assign(quartile_group=label)
        pieces.append(subset)

    if not pieces:
        raise ValueError("Quartile subsets are empty; check data availability.")

    combined = pd.concat(pieces, ignore_index=True)
    data = compute_group_rates(combined, ["quartile_group"])
    quartile_order = [label for _, _, label in QUARTILE_GROUPS]
    data["quartile_group"] = pd.Categorical(data["quartile_group"], categories=quartile_order, ordered=True)
    year_order = sorted(data["academic_year"].unique())
    data["year_index"] = data["academic_year"].apply(lambda y: year_order.index(y))

    y_max = data["rate"].max() if not data.empty else 0.05
    y_limit = y_max + max(0.01, y_max * 0.18)
    offset = max(0.0015, y_limit * 0.015)

    if render:
        fig, axes = plt.subplots(1, len(quartile_order), figsize=(22, 9), sharey=True)
        legend_handles: Dict[str, plt.Line2D] = {}

        for idx, (label, ax) in enumerate(zip(quartile_order, np.atleast_1d(axes))):
            subset = data[data["quartile_group"] == label]
            if subset.empty:
                ax.axis("off")
                continue
            subset = subset.sort_values(["subgroup", "year_index"])
            apply_reach_style(ax, year_order, y_limit)
            ax.set_title(label, loc="left", fontsize=14, fontweight="bold", pad=4)
            axis_annotations: List[Annotation] = []
            for race in RACE_LEVELS:
                race_df = subset[subset["subgroup"] == race]
                if race_df.empty:
                    continue
                line, = ax.plot(
                    race_df["year_index"],
                    race_df["rate"],
                    color=RACE_PALETTE[race],
                    linewidth=2.3,
                    marker="o",
                    markersize=5.3,
                    markeredgecolor="white",
                    markeredgewidth=0.6,
                )
                axis_annotations.extend(
                    annotate_points(
                        ax,
                        race_df,
                        RACE_PALETTE[race],
                        offset,
                        label_last_only=False,
                    )
                )
                if race not in legend_handles:
                    legend_handles[race] = line
            resolve_label_overlaps(ax, axis_annotations)
            if idx > 0:
                ax.set_ylabel("")
            else:
                ax.set_ylabel("Suspension rate", fontsize=11)

        finalize_figure(
            fig,
            title=title,
            subtitle=subtitle,
            caption=caption,
            handles=list(legend_handles.values()),
            labels=list(legend_handles.keys()),
            legend_cols=4,
            legend_y=0.91,
        )

        out_path = OUTPUT_DIR / output_filename
        fig.savefig(out_path, dpi=320)
        plt.close(fig)

    return data, year_order


def write_description(text: str, filename: str) -> None:
    path = TEXT_DIR / filename
    path.write_text(text.strip() + "\n", encoding="utf-8")


def describe_levels(data: pd.DataFrame, year_order: Sequence[str]) -> str:
    latest_year = year_order[-1]
    earliest_year = year_order[0]
    lines: List[str] = []
    for level in LEVEL_ORDER:
        subset_latest = data[(data["school_level"] == level) & (data["academic_year"] == latest_year)]
        subset_earliest = data[(data["school_level"] == level) & (data["academic_year"] == earliest_year)]
        if subset_latest.empty:
            continue
        black_rate = subset_latest.loc[subset_latest["subgroup"] == "Black/African American", "rate"].max()
        white_rate = subset_latest.loc[subset_latest["subgroup"] == "White", "rate"].max()
        hisp_rate = subset_latest.loc[subset_latest["subgroup"] == "Hispanic/Latino", "rate"].max()
        change_black = None
        if not subset_earliest.empty:
            first_black = subset_earliest.loc[
                subset_earliest["subgroup"] == "Black/African American", "rate"
            ].max()
            if pd.notnull(first_black) and pd.notnull(black_rate):
                change_black = black_rate - first_black
        ratio = (black_rate / white_rate) if white_rate and white_rate > 0 else None
        piece = (
            f"In {latest_year}, {level.lower()} schools suspended Black students at {format_percent(black_rate)} "
            f"compared with {format_percent(white_rate)} for White peers and {format_percent(hisp_rate)} for Hispanic/Latino students."
        )
        if ratio and math.isfinite(ratio):
            piece += f" That is roughly {ratio:.1f}× the White student rate."
        if change_black is not None and math.isfinite(change_black):
            direction = "higher" if change_black > 0 else "lower"
            piece += f" Black rates are {abs(change_black) * 100:.1f}% {direction} than in {earliest_year}."
        lines.append(piece)
    lines.append(
        "Rates reflect total suspensions divided by cumulative enrollment, summed across traditional schools with valid counts."
    )
    lines.append(
        "Years 2017-18 through 2023-24 are shown; 2020-21 reporting was not published statewide."
    )
    return " ".join(lines)


def describe_locales(data: pd.DataFrame, year_order: Sequence[str]) -> str:
    latest_year = year_order[-1]
    lines: List[str] = []
    for locale in LOCALE_ORDER:
        subset = data[(data["locale_simple"] == locale) & (data["academic_year"] == latest_year)]
        if subset.empty:
            continue
        black_rate = subset.loc[subset["subgroup"] == "Black/African American", "rate"].max()
        white_rate = subset.loc[subset["subgroup"] == "White", "rate"].max()
        hisp_rate = subset.loc[subset["subgroup"] == "Hispanic/Latino", "rate"].max()
        piece = (
            f"{locale} schools show Black suspension rates of {format_percent(black_rate)} in {latest_year}, "
            f"versus {format_percent(white_rate)} for White students and {format_percent(hisp_rate)} for Hispanic/Latino peers."
        )
        if white_rate and white_rate > 0 and black_rate and math.isfinite(black_rate):
            piece += f" The Black-White gap is {((black_rate - white_rate) * 100):.1f} percentage points."
        lines.append(piece)
    lines.append(
        "Locale categories follow the state locale_simple coding and exclude campuses without a reported locale."
    )
    lines.append(
        "Traditional-school filter and rate calculation mirror the level view: total suspensions ÷ cumulative enrollment each year."
    )
    return " ".join(lines)


def describe_quartiles(
    data: pd.DataFrame,
    year_order: Sequence[str],
    population_note: str | None = None,
) -> str:
    latest_year = year_order[-1]
    lines: List[str] = []
    for _, _, label in QUARTILE_GROUPS:
        subset = data[(data["quartile_group"] == label) & (data["academic_year"] == latest_year)]
        if subset.empty:
            continue
        black_rate = subset.loc[subset["subgroup"] == "Black/African American", "rate"].max()
        white_rate = subset.loc[subset["subgroup"] == "White", "rate"].max()
        hisp_rate = subset.loc[subset["subgroup"] == "Hispanic/Latino", "rate"].max()
        lines.append(
            f"Within {label}, Black students experienced {format_percent(black_rate)} suspensions in {latest_year}, "
            f"while White students were at {format_percent(white_rate)} and Hispanic/Latino students at {format_percent(hisp_rate)}."
        )
    black_label = QUARTILE_GROUPS[0][2]
    white_label = QUARTILE_GROUPS[1][2]
    black_subset = data[(data["quartile_group"] == black_label) & (data["academic_year"] == latest_year)]
    white_subset = data[(data["quartile_group"] == white_label) & (data["academic_year"] == latest_year)]
    if not black_subset.empty and not white_subset.empty:
        black_rate_black = black_subset.loc[black_subset["subgroup"] == "Black/African American", "rate"].max()
        black_rate_white = white_subset.loc[white_subset["subgroup"] == "Black/African American", "rate"].max()
        if black_rate_white and black_rate_white > 0 and math.isfinite(black_rate_black):
            ratio = black_rate_black / black_rate_white
            lines.append(
                f"Black students in highest-Black-enrollment schools are suspended about {ratio:.1f}× as often as Black students in highest-White-enrollment schools."
            )
    lines.append(
        population_note
        if population_note is not None
        else "Quartile comparisons rely on the state's enrollment composition flags, with traditional schools aggregated statewide."
    )
    return " ".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    joined = load_joined_data()
    base = prepare_base_data(joined)

    level_data, level_years, level_paths = build_level_figure(
        base, render=not args.diagnostics_only
    )

    write_diagnostics(level_data)

    if args.diagnostics_only:
        return

    locale_data, locale_years, locale_paths = build_locale_figure(base, render=True)

    quartile_data, quartile_years = build_quartile_figure(base, render=True)

    elementary_base = base[base["school_level"] == "Elementary"].copy()
    if elementary_base.empty:
        raise ValueError("No elementary school records available for quartile figure.")
    elementary_quartile_data, elementary_quartile_years = build_quartile_figure(
        base,
        base_subset=elementary_base,
        title="Elementary Suspension Rates in Highest-Black vs. Highest-White Enrollment Schools",
        subtitle="Traditional elementary schools in top quartile for Black vs. White enrollment, 2017-18 through 2023-24.",
        caption=(
            "Source: California statewide suspension data (susp_v6_long.parquet + susp_v6_features.parquet). "
            "Traditional elementary schools only; quartiles reference highest shares of Black or White enrollment."
        ),
        output_filename="statewide_race_trends_quartile_elementary.png",
        render=True,
    )

    write_description(describe_levels(level_data, level_years), "statewide_race_trends_by_level.txt")
    write_description(describe_locales(locale_data, locale_years), "statewide_race_trends_by_locale.txt")
    write_description(describe_quartiles(quartile_data, quartile_years), "statewide_race_trends_quartile_comparison.txt")
    write_description(
        describe_quartiles(
            elementary_quartile_data,
            elementary_quartile_years,
            population_note=(
                "Quartile comparisons rely on the state's enrollment composition flags, aggregated statewide across traditional elementary schools."
            ),
        ),
        "statewide_race_trends_quartile_comparison_elementary.txt",
    )

    for path in level_paths:
        print(f"Saved {path.name}")
    for path in locale_paths:
        print(f"Saved {path.name}")

    print("Saved statewide_race_trends_quartile_comparison.png")
    print("Saved statewide_race_trends_quartile_elementary.png")


if __name__ == "__main__":
    main(sys.argv[1:])
