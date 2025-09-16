#!/usr/bin/env python3
"""Generate statewide suspension trends by race across levels, locales, and quartiles."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import sys

try:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
except ImportError:
    print(
        "Required package 'matplotlib' is not installed. Install it with `pip install matplotlib`.",
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

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_STAGE = ROOT_DIR / "data-stage"
OUTPUT_DIR = ROOT_DIR / "outputs" / "graphs"
TEXT_DIR = OUTPUT_DIR / "descriptions"

SUSP_PATH = DATA_STAGE / "susp_v5.parquet"
FEAT_PATH = DATA_STAGE / "susp_v6_features.parquet"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TEXT_DIR.mkdir(parents=True, exist_ok=True)

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


def load_joined_data() -> pd.DataFrame:
    """Load suspension data joined with school features."""
    if not SUSP_PATH.exists() or not FEAT_PATH.exists():
        raise FileNotFoundError("Required parquet files are missing in data-stage/.")

    susp = pq.read_table(SUSP_PATH).to_pandas()
    susp.columns = clean_columns(susp.columns)
    susp = susp[[
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
    ]]

    feat = pq.read_table(FEAT_PATH).to_pandas()
    feat.columns = clean_columns(feat.columns)
    feat = feat[[
        "school_code",
        "academic_year",
        "is_traditional",
        "black_prop_q",
        "black_prop_q_label",
    ]].rename(
        columns={
            "black_prop_q": "feat_black_prop_q",
            "black_prop_q_label": "feat_black_prop_q_label",
        }
    )

    joined = susp.merge(feat, on=["school_code", "academic_year"], how="left")
    joined["academic_year"] = joined["academic_year"].astype(str).str.strip()
    joined["school_code"] = joined["school_code"].astype(str).str.strip()
    joined["is_traditional"] = joined["is_traditional"].fillna(False)
    joined["black_prop_q"] = joined["black_prop_q"].where(
        joined["black_prop_q"].notna(), joined["feat_black_prop_q"]
    )
    joined["black_prop_q_label"] = joined["black_prop_q_label"].where(
        joined["black_prop_q_label"].notna(), joined["feat_black_prop_q_label"]
    )

    return joined.drop(columns=["feat_black_prop_q", "feat_black_prop_q_label"])


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
    label_last_only: bool = True,
) -> None:
    """Label data points while trying to reduce clutter on busy lines."""

    points = df
    if label_last_only:
        max_index = df["year_index"].max()
        points = df[df["year_index"] == max_index]

    for _, row in points.iterrows():
        label = f"{row['rate'] * 100:.1f}%"
        ax.text(
            row["year_index"],
            row["rate"] + offset,
            label,
            color=color,
            fontsize=8,
            ha="center",
            va="bottom",
            fontweight="bold",
            clip_on=False,
            bbox={
                "boxstyle": "round,pad=0.18",
                "facecolor": "white",
                "edgecolor": "none",
                "alpha": 0.85,
            },
        )

def finalize_figure(
    fig: plt.Figure,
    title: str,
    subtitle: str,
    caption: str,
    handles: Sequence,
    labels: Sequence[str],
    legend_cols: int = 4,
    legend_y: float = 0.86,
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
    fig.subplots_adjust(left=0.07, right=0.98, top=0.82, bottom=0.18, wspace=0.28, hspace=0.36)


def format_percent(value: float, accuracy: float = 0.1) -> str:
    if value is None or not math.isfinite(value):
        return "N/A"
    return f"{value * 100:.{0 if accuracy >= 1 else 1}f}%"


def build_level_figure(base: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    data = compute_group_rates(base, ["school_level"])
    data = data[data["school_level"].isin(LEVEL_ORDER)].copy()
    data["school_level"] = pd.Categorical(data["school_level"], categories=LEVEL_ORDER, ordered=True)
    year_order = sorted(data["academic_year"].unique())
    data["year_index"] = data["academic_year"].apply(lambda y: year_order.index(y))

    y_max = data["rate"].max() if not data.empty else 0.05
    y_limit = y_max + max(0.01, y_max * 0.18)
    offset = max(0.0015, y_limit * 0.015)

    fig, axes = plt.subplots(1, len(LEVEL_ORDER), figsize=(22, 7.5), sharey=True)
    legend_handles: Dict[str, plt.Line2D] = {}

    for idx, (level, ax) in enumerate(zip(LEVEL_ORDER, np.atleast_1d(axes))):
        subset = data[data["school_level"] == level]
        if subset.empty:
            ax.axis("off")
            continue
        subset = subset.sort_values(["subgroup", "year_index"])
        apply_reach_style(ax, year_order, y_limit)
        ax.set_title(f"{level} Schools", loc="left", fontsize=14, fontweight="bold", pad=16)
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
            annotate_points(ax, race_df, RACE_PALETTE[race], offset)
            if race not in legend_handles:
                legend_handles[race] = line
        if idx > 0:
            ax.set_ylabel("")
        else:
            ax.set_ylabel("Suspension rate", fontsize=11)

    caption = (
        "Source: California statewide suspension data (susp_v5.parquet + susp_v6_features.parquet). "
        "Traditional schools only; rates aggregate total suspensions ÷ cumulative enrollment."
    )
    subtitle = (
        "By grade span, 2017-18 through 2023-24 (no statewide reporting in 2020-21)."
    )
    finalize_figure(
        fig,
        title="Suspension Rates by Race Across School Levels",
        subtitle=subtitle,
        caption=caption,
        handles=list(legend_handles.values()),
        labels=list(legend_handles.keys()),
        legend_cols=4,
        legend_y=0.87,
    )

    out_path = OUTPUT_DIR / "statewide_race_trends_by_level.png"
    fig.savefig(out_path, dpi=320)
    plt.close(fig)
    return data, year_order


def build_locale_figure(base: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    data = compute_group_rates(base, ["locale_simple"])
    data = data[data["locale_simple"].isin(LOCALE_ORDER)].copy()
    data["locale_simple"] = pd.Categorical(data["locale_simple"], categories=LOCALE_ORDER, ordered=True)
    year_order = sorted(data["academic_year"].unique())
    data["year_index"] = data["academic_year"].apply(lambda y: year_order.index(y))

    y_max = data["rate"].max() if not data.empty else 0.05
    y_limit = y_max + max(0.01, y_max * 0.18)
    offset = max(0.0015, y_limit * 0.015)

    fig, axes = plt.subplots(2, 2, figsize=(22, 12), sharex=False, sharey=True)
    axes_flat = axes.flatten()
    legend_handles: Dict[str, plt.Line2D] = {}

    for idx, (locale, ax) in enumerate(zip(LOCALE_ORDER, axes_flat)):
        subset = data[data["locale_simple"] == locale]
        ax.set_title(f"{locale} Schools", loc="left", fontsize=14, fontweight="bold", pad=16)
        if subset.empty:
            ax.axis("off")
            continue
        subset = subset.sort_values(["subgroup", "year_index"])
        apply_reach_style(ax, year_order, y_limit)
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
            annotate_points(ax, race_df, RACE_PALETTE[race], offset)
            if race not in legend_handles:
                legend_handles[race] = line
        if idx % 2 == 0:
            ax.set_ylabel("Suspension rate", fontsize=11)
        else:
            ax.set_ylabel("")

    for ax in axes_flat[len(LOCALE_ORDER):]:
        ax.axis("off")

    caption = (
        "Source: California statewide suspension data (susp_v5.parquet + susp_v6_features.parquet). "
        "Traditional schools only; locale grouping uses locale_simple categories."
    )
    subtitle = "By locale, 2017-18 through 2023-24 (no statewide reporting in 2020-21)."
    finalize_figure(
        fig,
        title="Suspension Rates by Race Across School Locales",
        subtitle=subtitle,
        caption=caption,
        handles=list(legend_handles.values()),
        labels=list(legend_handles.keys()),
        legend_cols=4,
        legend_y=0.88,
    )

    out_path = OUTPUT_DIR / "statewide_race_trends_by_locale.png"
    fig.savefig(out_path, dpi=320)
    plt.close(fig)
    return data, year_order


def build_quartile_figure(base: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    pieces: List[pd.DataFrame] = []
    for field, value, label in QUARTILE_GROUPS:
        subset = base[base[field] == value].copy()
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

    fig, axes = plt.subplots(1, len(quartile_order), figsize=(22, 7.5), sharey=True)
    legend_handles: Dict[str, plt.Line2D] = {}

    for idx, (label, ax) in enumerate(zip(quartile_order, np.atleast_1d(axes))):
        subset = data[data["quartile_group"] == label]
        if subset.empty:
            ax.axis("off")
            continue
        subset = subset.sort_values(["subgroup", "year_index"])
        apply_reach_style(ax, year_order, y_limit)
        ax.set_title(label, loc="left", fontsize=14, fontweight="bold", pad=16)
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
            annotate_points(ax, race_df, RACE_PALETTE[race], offset)
            if race not in legend_handles:
                legend_handles[race] = line
        if idx > 0:
            ax.set_ylabel("")
        else:
            ax.set_ylabel("Suspension rate", fontsize=11)

    caption = (
        "Source: California statewide suspension data (susp_v5.parquet + susp_v6_features.parquet). "
        "Traditional schools only; quartiles reference highest shares of Black or White enrollment."
    )
    subtitle = "Traditional schools in top quartile for Black vs. White enrollment, 2017-18 through 2023-24."
    finalize_figure(
        fig,
        title="Suspension Rates in Highest-Black vs. Highest-White Enrollment Schools",
        subtitle=subtitle,
        caption=caption,
        handles=list(legend_handles.values()),
        labels=list(legend_handles.keys()),
        legend_cols=4,
        legend_y=0.87,
    )

    out_path = OUTPUT_DIR / "statewide_race_trends_quartile_comparison.png"
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


def describe_quartiles(data: pd.DataFrame, year_order: Sequence[str]) -> str:
    latest_year = year_order[-1]
    lines: List[str] = []
    quartile_lookup = {label: label for _, _, label in QUARTILE_GROUPS}
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
        "Quartile comparisons rely on the state's enrollment composition flags, with traditional schools aggregated statewide."
    )
    return " ".join(lines)


def main() -> None:
    joined = load_joined_data()
    base = prepare_base_data(joined)

    level_data, level_years = build_level_figure(base)
    locale_data, locale_years = build_locale_figure(base)
    quartile_data, quartile_years = build_quartile_figure(base)

    write_description(describe_levels(level_data, level_years), "statewide_race_trends_by_level.txt")
    write_description(describe_locales(locale_data, locale_years), "statewide_race_trends_by_locale.txt")
    write_description(describe_quartiles(quartile_data, quartile_years), "statewide_race_trends_quartile_comparison.txt")

    print("Saved statewide_race_trends_by_level.png")
    print("Saved statewide_race_trends_by_locale.png")
    print("Saved statewide_race_trends_quartile_comparison.png")


if __name__ == "__main__":
    main()
