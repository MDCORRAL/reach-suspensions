"""Generate UCLA-branded suspension reason trend charts by school level.

This module can be executed as a script.  By default it reads the long-format
parquet export and renders one chart per school level, storing the images in
``outputs/20_reason_trends_by_level``.  The output directory and image format
(`svg` or `png`) can be overridden via command-line flags.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from adjustText import adjust_text

# UCLA brand colors
UCLA_DARKEST_BLUE = "#003B5C"
UCLA_DARKER_BLUE = "red" 
UCLA_BLUE = "#2774AE"
UCLA_DARKEST_GOLD = "#FFB81C"
UCLA_DARKER_GOLD = "#8A69D4"
UCLA_GOLD = "#FFD100"
RED = "red"
UCLA_LIGHTEST_BLUE = "#8BB8E8"

TEXT_COLOR = UCLA_DARKEST_BLUE
GRID_COLOR = UCLA_LIGHTEST_BLUE

REASON_COLUMNS = {
    "suspension_count_violent_incident_injury": "Violent (Injury)",
    "suspension_count_violent_incident_no_injury": "Violent (No Injury)",
    "suspension_count_weapons_possession": "Weapons",
    "suspension_count_illicit_drug_related": "Illicit Drugs",
    "suspension_count_defiance_only": "Willful Defiance",
    "suspension_count_other_reasons": "Other",
}

REASON_PALETTE = {
    "Violent (Injury)": UCLA_DARKEST_BLUE,
    "Violent (No Injury)": UCLA_DARKER_BLUE,
    "Weapons": UCLA_BLUE,
    "Illicit Drugs": UCLA_DARKEST_GOLD,
    "Willful Defiance": RED,
    "Other": UCLA_GOLD,
}

LEVEL_ORDER = ["Elementary", "Middle", "High"]

DEFAULT_DATA_PATH = Path("data-stage") / "susp_v6_long.parquet"
DEFAULT_OUTPUT_DIR = Path("outputs") / "20_reason_trends_by_level"
DEFAULT_IMAGE_FORMAT = "png"

# ----------------------------------------------------------------------------
# Data preparation
# ----------------------------------------------------------------------------

read_columns = [
    "academic_year",
    "school_level",
    "subgroup",
    "category_type",
    "cumulative_enrollment",
    *REASON_COLUMNS.keys(),
]

def load_data(data_path: Path) -> pd.DataFrame:
    """Return the suspension detail parquet as a pandas DataFrame."""

    print(f"Loading suspension detail parquet from {data_path}…")
    parquet_table = pq.read_table(data_path, columns=read_columns)
    return parquet_table.to_pandas()


def prepare_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Filter and aggregate suspension reason counts by level/year."""

    filtered = (
        raw_df
        .loc[
            (raw_df["category_type"] == "Race/Ethnicity")
            & (raw_df["subgroup"] == "All Students")
            & (raw_df["school_level"].isin(LEVEL_ORDER))
        ]
        .copy()
    )

    filtered["academic_year"] = pd.Categorical(
        filtered["academic_year"],
        ordered=True,
        categories=sorted(filtered["academic_year"].dropna().unique()),
    )
    filtered["school_level"] = pd.Categorical(
        filtered["school_level"], categories=LEVEL_ORDER, ordered=True
    )

    agg_dict = {col: "sum" for col in REASON_COLUMNS}
    agg_dict["cumulative_enrollment"] = "sum"

    aggregated = (
        filtered
        .groupby(["academic_year", "school_level"], observed=True, dropna=False)
        .agg(agg_dict)
        .reset_index()
    )

    melted = aggregated.melt(
        id_vars=["academic_year", "school_level", "cumulative_enrollment"],
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

    return melted

# ----------------------------------------------------------------------------
# Plotting helpers
# ----------------------------------------------------------------------------

def _format_percent(value: float) -> str:
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return "NA"
    return f"{value * 100:.1f}%"


def plot_level(
    df: pd.DataFrame,
    level: str,
    output_dir: Path,
    image_format: str = DEFAULT_IMAGE_FORMAT,
    dpi: int | None = None,
) -> None:
    level_df = df[df["school_level"] == level].copy()
    level_df = level_df.dropna(subset=["rate"])

    if level_df.empty:
        print(f"Skipping {level}: no data to plot.")
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
        linestyle = "--" if reason_label == "Willful Defiance" else "-"
        ax.plot(
            xs,
            ys,
            label=reason_label,
            color=color,
            linewidth=2.2,
            marker="o",
            markersize=6,
            linestyle=linestyle,
        )
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

    adjust_text(
        texts,
        ax=ax,
        expand_points=(1.1, 1.4),
        expand_text=(1.1, 1.4),
        arrowprops=dict(arrowstyle="-", color=TEXT_COLOR, lw=0.5, shrinkA=5, shrinkB=5),
        only_move={"points": "y", "text": "xy"},
    )

    ax.set_xticks(list(x_positions.values()))
    ax.set_xticklabels(years, rotation=45, ha="right", color=TEXT_COLOR)
    ax.tick_params(axis="y", colors=TEXT_COLOR)

    ax.set_ylabel("Suspension Rate (Percent)", color=TEXT_COLOR, fontweight="bold")
    ax.set_xlabel("Academic Year", color=TEXT_COLOR, fontweight="bold")
    ax.set_title(
        f"{level} Schools — Suspension Rates by Reason",
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
    output_path = output_dir / f"20_suspension_reason_trends_{level.lower()}.{suffix}"
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
) -> None:
    """Render a chart for each requested school level."""

    selected_levels = list(levels) if levels is not None else LEVEL_ORDER
    for school_level in selected_levels:
        plot_level(df, school_level, output_dir=output_dir, image_format=image_format, dpi=dpi)

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
        choices=["svg", "png"],
        default=DEFAULT_IMAGE_FORMAT,
        help="Image format for saved charts.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="DPI to use when saving raster formats (ignored for SVG).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_df = load_data(args.data_path)
    melted = prepare_data(raw_df)
    dpi = args.dpi if args.image_format != "svg" else None
    plot_all_levels(
        melted,
        output_dir=args.output_dir,
        image_format=args.image_format,
        dpi=dpi,
    )


if __name__ == "__main__":
    main()
