"""Generate UCLA-branded suspension reason trend charts by level and locale.

This module can be executed as a script.  By default it reads the long-format
parquet export and renders one chart per combination of school level and
locale, storing PNG images in
``outputs/20_suspension_reason_trends_by_level_and_locale``. An additional
"All Traditional" aggregate (non-charter schools) is generated for each level
to provide a systemwide comparison across locales.
The script also emits a statewide aggregate chart that combines traditional
elementary, middle, and high school results into a single view.

The output directory, subset of levels/locales, and image format (``png`` or
``svg``) can be overridden via command-line flags.
"""

from __future__ import annotations

import argparse
import io
import math
from contextlib import redirect_stdout
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from adjustText import adjust_text

# UCLA brand-aligned palette
UCLA_PALETTE = {
    "Darkest Blue": "#003B5C",
    "Darker Blue": "red",
    "UCLA Blue": "#2774AE",
    "Lighter Blue": "#8BB8E8",
    "UCLA Gold": "#FFD100",
    "Darker Gold": "#FFC72C",
    "Darkest Gold": "#FFB81C",
    "Purple": "#8A69D4",
    "Green": "#00FF87",
    "Magenta": "#FF00A5",
    "Cyan": "#00FFFF",
}

TEXT_COLOR = UCLA_PALETTE["Darkest Blue"]
GRID_COLOR = UCLA_PALETTE["Lighter Blue"]

REASON_COLUMNS = {
    "suspension_count_violent_incident_injury": "Violent (Injury)",
    "suspension_count_violent_incident_no_injury": "Violent (No Injury)",
    "suspension_count_weapons_possession": "Weapons",
    "suspension_count_illicit_drug_related": "Illicit Drugs",
    "suspension_count_defiance_only": "Willful Defiance",
    "suspension_count_other_reasons": "Other",
}

REASON_PALETTE = {
    "Violent (Injury)": UCLA_PALETTE["Darkest Blue"],
    "Violent (No Injury)": UCLA_PALETTE["Darker Blue"],
    "Weapons": UCLA_PALETTE["UCLA Blue"],
    "Illicit Drugs": UCLA_PALETTE["Purple"],
    "Willful Defiance": UCLA_PALETTE["Green"],
    "Other": UCLA_PALETTE["Magenta"],
}

LEVEL_ORDER = ["Elementary", "Middle", "High"]
LOCALE_COLUMN = "locale_simple"
LOCALE_ORDER = ["City", "Suburban", "Town", "Rural", "Unknown"]

DEFAULT_DATA_PATH = Path("data-stage") / "susp_v6_long.parquet"
DEFAULT_OUTPUT_DIR = Path("outputs") / "20_suspension_reason_trends_by_level_and_locale"
DEFAULT_IMAGE_FORMAT = "png"

# ----------------------------------------------------------------------------
# Data preparation
# ----------------------------------------------------------------------------

read_columns = [
    "academic_year",
    "school_level",
    "subgroup",
    "category_type",
    LOCALE_COLUMN,
    "cumulative_enrollment",
    "charter_yn_std",
    *REASON_COLUMNS.keys(),
]

def load_data(data_path: Path) -> pd.DataFrame:
    """Return the suspension detail parquet as a pandas DataFrame."""

    print(f"Loading suspension detail parquet from {data_path}…")
    parquet_table = pq.read_table(data_path, columns=read_columns)
    return parquet_table.to_pandas()


def prepare_data(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter and aggregate suspension reason counts by level/year/locale.

    Returns a tuple containing the level/locale melt and the statewide
    traditional aggregate.
    """

    filtered = (
        raw_df
        .loc[
            (raw_df["category_type"] == "Race/Ethnicity")
            & (raw_df["subgroup"] == "All Students")
            & (raw_df["school_level"].isin(LEVEL_ORDER))
        ]
        .copy()
    )

    if LOCALE_COLUMN not in filtered.columns:
        raise SystemExit(
            f"Expected '{LOCALE_COLUMN}' column in the dataset."
        )

    filtered[LOCALE_COLUMN] = (
        filtered[LOCALE_COLUMN]
        .astype("string")
        .fillna("Unknown")
        .replace({"": "Unknown"})
    )

    filtered["academic_year"] = pd.Categorical(
        filtered["academic_year"],
        ordered=True,
        categories=sorted(filtered["academic_year"].dropna().unique()),
    )
    filtered["school_level"] = pd.Categorical(
        filtered["school_level"], categories=LEVEL_ORDER, ordered=True
    )

    observed_locales = filtered[LOCALE_COLUMN].dropna().unique().tolist()
    locale_categories = [locale for locale in LOCALE_ORDER if locale in observed_locales]
    # include any unforeseen locales at the end to avoid data loss
    extras = [
        locale
        for locale in sorted(observed_locales)
        if locale not in locale_categories
    ]
    base_locale_categories = [*locale_categories, *extras]
    filtered[LOCALE_COLUMN] = pd.Categorical(
        filtered[LOCALE_COLUMN],
        categories=base_locale_categories,
        ordered=True,
    )

    filtered["charter_yn_std"] = filtered["charter_yn_std"].fillna("Unknown")

    agg_dict = {col: "sum" for col in REASON_COLUMNS}
    agg_dict["cumulative_enrollment"] = "sum"

    aggregated = (
        filtered
        .groupby(["academic_year", "school_level", LOCALE_COLUMN], observed=True, dropna=False)
        .agg(agg_dict)
        .reset_index()
    )

    traditional = filtered[filtered["charter_yn_std"] == "No"].copy()
    if not traditional.empty:
        agg_traditional = (
            traditional
            .groupby(["academic_year", "school_level"], observed=True, dropna=False)
            .agg(agg_dict)
            .reset_index()
        )
        agg_traditional[LOCALE_COLUMN] = "All Traditional"
        aggregated = pd.concat([aggregated, agg_traditional], ignore_index=True, sort=False)

    statewide_melted = pd.DataFrame(
        columns=[
            "academic_year",
            "cumulative_enrollment",
            "reason",
            "count",
            "reason_label",
            "rate",
            "school_level",
        ]
    )
    if not traditional.empty:
        statewide_agg = (
            traditional
            .groupby(["academic_year"], observed=True, dropna=False)
            .agg(agg_dict)
            .reset_index()
        )
        statewide_agg["academic_year"] = pd.Categorical(
            statewide_agg["academic_year"],
            categories=filtered["academic_year"].cat.categories,
            ordered=True,
        )
        statewide_melted = statewide_agg.melt(
            id_vars=["academic_year", "cumulative_enrollment"],
            value_vars=list(REASON_COLUMNS.keys()),
            var_name="reason",
            value_name="count",
        )
        statewide_melted["reason_label"] = statewide_melted["reason"].map(REASON_COLUMNS)
        statewide_melted["rate"] = np.where(
            statewide_melted["cumulative_enrollment"] > 0,
            statewide_melted["count"] / statewide_melted["cumulative_enrollment"],
            np.nan,
        )
        statewide_melted["school_level"] = "All Traditional"
        statewide_melted = statewide_melted.dropna(subset=["reason_label"]).copy()
        
    if "All Traditional" in aggregated[LOCALE_COLUMN].astype("string").unique():
        aggregated[LOCALE_COLUMN] = pd.Categorical(
            aggregated[LOCALE_COLUMN].astype("string"),
            categories=[*base_locale_categories, "All Traditional"],
            ordered=True,
        )
    else:
        aggregated[LOCALE_COLUMN] = pd.Categorical(
            aggregated[LOCALE_COLUMN].astype("string"),
            categories=base_locale_categories,
            ordered=True,
        )

    melted = aggregated.melt(
        id_vars=["academic_year", "school_level", LOCALE_COLUMN, "cumulative_enrollment"],
        value_vars=list(REASON_COLUMNS.keys()),
        var_name="reason",
        value_name="count",
    )

    melted["reason_label"] = melted["reason"].map(REASON_COLUMNS)

    melted["rate"] = np.where(
        melted["cumulative_enrollment"] > 0,
        melted["count"] / melted["cumulative_enrollment"],
        np.nan,
    )

    melted = melted.dropna(subset=["reason_label"]).copy()

    if melted.empty:
        raise SystemExit("No suspension reason data available after filtering.")

    return melted, statewide_melted

# ----------------------------------------------------------------------------
# Plotting helpers
# ----------------------------------------------------------------------------

def _format_percent(value: float) -> str:
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return "NA"
    return f"{value * 100:.1f}%"


def _slugify(value: str) -> str:
    """Return a simple filesystem-friendly slug."""

    text = str(value)
    return (
        text.lower()
        .replace("/", "-")
        .replace(" ", "_")
        .replace("__", "_")
    )


def plot_level_locale(
    df: pd.DataFrame,
    level: str,
    locale: str,
    output_dir: Path,
    image_format: str = DEFAULT_IMAGE_FORMAT,
    dpi: int | None = None,
) -> None:
    level_df = df[
        (df["school_level"] == level)
        & (df[LOCALE_COLUMN] == locale)
    ].copy()
    level_df = level_df.dropna(subset=["rate"])

    if level_df.empty:
        print(f"Skipping {level} / {locale}: no data to plot.")
        return

    years = level_df["academic_year"].cat.categories
    x_positions = {year: idx for idx, year in enumerate(years)}

    fig, ax = plt.subplots(figsize=(10, 6), dpi=dpi or 300)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    texts = []
    for reason_label, color in REASON_PALETTE.items():
        reason_df = level_df[level_df["reason_label"] == reason_label]
        if reason_df.empty:
            continue
        reason_df = reason_df.sort_values("academic_year")
        xs = [x_positions[year] for year in reason_df["academic_year"]]
        ys = reason_df["rate"].to_numpy() * 100
        ax.plot(xs, ys, label=reason_label, color=color, linewidth=2.2, marker="o", markersize=6)
        for x_val, y_val, rate_val in zip(xs, ys, reason_df["rate"]):
            label = _format_percent(rate_val)
            text = ax.text(
                x_val,
                y_val,
                label,
                color=color,
                fontsize=9,
                fontweight="bold",
            )
            texts.append(text)

    if texts:
        with redirect_stdout(io.StringIO()):
            adjust_text(
                texts,
                ax=ax,
                expand_points=(1.1, 1.4),
                expand_text=(1.1, 1.4),
                only_move={"points": "y", "text": "xy"},
            )

    ax.set_xticks(list(x_positions.values()))
    ax.set_xticklabels(years, rotation=45, ha="right", color=TEXT_COLOR)
    ax.tick_params(axis="y", colors=TEXT_COLOR)

    ax.set_ylabel("Suspension Rate (Percent)", color=TEXT_COLOR, fontweight="bold")
    ax.set_xlabel("Academic Year", color=TEXT_COLOR, fontweight="bold")
    ax.set_title(
        f"{level} Schools — {locale} Locale\nSuspension Rates by Reason",
        color=TEXT_COLOR,
        fontsize=16,
        fontweight="bold",
        pad=15,
    )

    ax.grid(True, axis="y", color=GRID_COLOR, linestyle="-", linewidth=0.8)
    ax.grid(False, axis="x")

    for spine in ax.spines.values():
        spine.set_visible(False)

    legend = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.2),
        ncol=3,
        frameon=False,
        labelcolor=TEXT_COLOR,
    )
    for text in legend.get_texts():
        text.set_fontweight("bold")

    ax.set_ylim(bottom=0)
    ax.margins(x=0.02, y=0.15)

    plt.tight_layout()
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = image_format.lower().lstrip(".")
    level_slug = _slugify(level)
    locale_slug = _slugify(locale)
    output_path = output_dir / f"20_suspension_reason_trends_{level_slug}_{locale_slug}.{suffix}"
    save_kwargs = {"format": suffix}
    if suffix != "svg" and dpi is not None:
        save_kwargs["dpi"] = dpi
    fig.savefig(output_path, **save_kwargs)
    plt.close(fig)
    print(f"Saved chart: {output_path}")


def plot_statewide(
    df: pd.DataFrame,
    output_dir: Path,
    image_format: str = DEFAULT_IMAGE_FORMAT,
    dpi: int | None = None,
) -> None:
    """Render the statewide traditional aggregate chart."""

    statewide_df = df.dropna(subset=["rate"]).copy()
    if statewide_df.empty:
        print("Skipping statewide aggregate: no data to plot.")
        return

    if isinstance(statewide_df["academic_year"].dtype, pd.CategoricalDtype):
        years = statewide_df["academic_year"].cat.categories
    else:
        years = sorted(statewide_df["academic_year"].unique())

    x_positions = {year: idx for idx, year in enumerate(years)}

    fig, ax = plt.subplots(figsize=(10, 6), dpi=dpi or 300)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    texts = []
    for reason_label, color in REASON_PALETTE.items():
        reason_df = statewide_df[statewide_df["reason_label"] == reason_label]
        if reason_df.empty:
            continue
        reason_df = reason_df.sort_values("academic_year")
        xs = [x_positions[year] for year in reason_df["academic_year"]]
        ys = reason_df["rate"].to_numpy() * 100
        ax.plot(xs, ys, label=reason_label, color=color, linewidth=2.2, marker="o", markersize=6)
        for x_val, y_val, rate_val in zip(xs, ys, reason_df["rate"]):
            label = _format_percent(rate_val)
            text = ax.text(
                x_val,
                y_val,
                label,
                color=color,
                fontsize=9,
                fontweight="bold",
            )
            texts.append(text)

    if texts:
        with redirect_stdout(io.StringIO()):
            adjust_text(
                texts,
                ax=ax,
                expand_points=(1.1, 1.4),
                expand_text=(1.1, 1.4),
                only_move={"points": "y", "text": "xy"},
            )

    ax.set_xticks(list(x_positions.values()))
    ax.set_xticklabels(years, rotation=45, ha="right", color=TEXT_COLOR)
    ax.tick_params(axis="y", colors=TEXT_COLOR)

    ax.set_ylabel("Suspension Rate (Percent)", color=TEXT_COLOR, fontweight="bold")
    ax.set_xlabel("Academic Year", color=TEXT_COLOR, fontweight="bold")
    ax.set_title(
        "All Traditional Schools — Statewide Suspension Rates by Reason",
        color=TEXT_COLOR,
        fontsize=16,
        fontweight="bold",
        pad=15,
    )

    ax.grid(True, axis="y", color=GRID_COLOR, linestyle="-", linewidth=0.8)
    ax.grid(False, axis="x")

    for spine in ax.spines.values():
        spine.set_visible(False)

    legend = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.2),
        ncol=3,
        frameon=False,
        labelcolor=TEXT_COLOR,
    )
    for text in legend.get_texts():
        text.set_fontweight("bold")

    ax.set_ylim(bottom=0)
    ax.margins(x=0.02, y=0.15)

    plt.tight_layout()
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = image_format.lower().lstrip(".")
    output_path = output_dir / f"20_suspension_reason_trends_all_traditional_statewide.{suffix}"
    save_kwargs = {"format": suffix}
    if suffix != "svg" and dpi is not None:
        save_kwargs["dpi"] = dpi
    fig.savefig(output_path, **save_kwargs)
    plt.close(fig)
    print(f"Saved chart: {output_path}")

def plot_all_levels(
    df: pd.DataFrame,
    output_dir: Path,
    image_format: str = DEFAULT_IMAGE_FORMAT,
    dpi: int | None = None,
    levels: Iterable[str] | None = None,
    locales: Iterable[str] | None = None,
) -> None:
    """Render a chart for each requested school level and locale."""

    selected_levels = list(levels) if levels is not None else LEVEL_ORDER
    available_locales = (
        list(locales)
        if locales is not None
        else [
            locale
            for locale in LOCALE_ORDER
            if locale in set(df[LOCALE_COLUMN].astype(str))
        ]
    )
    if locales is None:
        extras = [
            locale
            for locale in sorted(df[LOCALE_COLUMN].astype(str).unique())
            if locale not in available_locales
        ]
        available_locales.extend(extras)

    for school_level in selected_levels:
        for locale in available_locales:
            plot_level_locale(
                df,
                school_level,
                locale,
                output_dir=output_dir,
                image_format=image_format,
                dpi=dpi,
            )

    print(f"Completed charts saved in {output_dir.resolve()}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-path",
        type=Path,
        default=DEFAULT_DATA_PATH,
        help="Path to the long-format suspension parquet export.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where charts will be written.",
    )
    parser.add_argument(
        "--image-format",
        choices=["png", "svg"],
        default=DEFAULT_IMAGE_FORMAT,
        help="Image format for saved charts (default: png).",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="DPI to use when saving raster formats (ignored for SVG).",
    )
    parser.add_argument(
        "--levels",
        nargs="*",
        default=None,
        help="Optional subset of school levels to render.",
    )
    parser.add_argument(
        "--locales",
        nargs="*",
        default=None,
        help="Optional subset of locale categories to render.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_df = load_data(args.data_path)
    melted, statewide = prepare_data(raw_df)
    dpi = args.dpi if args.image_format != "svg" else None
    plot_all_levels(
        melted,
        output_dir=args.output_dir,
        image_format=args.image_format,
        dpi=dpi,
        levels=args.levels,
        locales=args.locales,
    )
    plot_statewide(
        statewide,
        output_dir=args.output_dir,
        image_format=args.image_format,
        dpi=dpi,
    )


if __name__ == "__main__":
    main()
