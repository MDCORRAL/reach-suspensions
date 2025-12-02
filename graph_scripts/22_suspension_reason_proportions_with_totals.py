"""Generate chart showing suspension reason proportions with total suspension context.

This module creates a visualization combining:
1. Line charts showing each suspension category as a proportion of total suspensions
2. Background bar chart showing total suspension counts

The goal is to demonstrate the "replacement phenomenon" where Willful Defiance
declines proportionally while other categories increase, while the total number
of suspensions remains relatively stable.
"""

from __future__ import annotations

import argparse
import importlib.util
import math
import sys
import textwrap
from pathlib import Path

# Handle both script and interactive execution contexts
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
except NameError:  # pragma: no cover - interactive contexts without __file__
    SCRIPT_DIR = Path.cwd() / "graph_scripts"

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

REQUIRED_MODULES = {
    "matplotlib": "pip install -r graph_scripts/requirements.txt",
    "pandas": "pip install -r graph_scripts/requirements.txt",
    "numpy": "pip install -r graph_scripts/requirements.txt",
    "pyarrow": "pip install -r graph_scripts/requirements.txt",
    "palette_utils": "run from repo root or ensure graph_scripts is on PYTHONPATH",
    "data_validations": "run from repo root or ensure graph_scripts is on PYTHONPATH",
}

missing = [
    name
    for name, install_hint in REQUIRED_MODULES.items()
    if importlib.util.find_spec(name) is None
]
if missing:
    hints = [f"- {name}: {REQUIRED_MODULES[name]}" for name in missing]
    message = (
        "Missing required Python packages for 22_suspension_reason_proportions_with_totals.\n"
        "Install the dependencies before running (e.g., pip install -r graph_scripts/requirements.txt).\n"
        "Missing modules:\n" + "\n".join(hints)
    )
    raise SystemExit(message)

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from palette_utils import DISCIPLINE_BASE_PALETTE, DISCIPLINE_REASON_PALETTE, STANDARD_CITATION
from data_validations import audit_counts_against_enrollment, ensure_audit_dir, sanitize_rate_column

TEXT_COLOR = DISCIPLINE_BASE_PALETTE["Darkest Blue"]
CAPTION_COLOR = DISCIPLINE_BASE_PALETTE["Grey"]
GRID_COLOR = DISCIPLINE_BASE_PALETTE["Lighter Blue"]

# Map reason columns to display labels
REASON_COLUMNS = {
    "suspension_count_violent_incident_injury": "Violent (Injury)",
    "suspension_count_violent_incident_no_injury": "Violent (No Injury)",
    "suspension_count_weapons_possession": "Weapons",
    "suspension_count_illicit_drug_related": "Illicit Drugs",
    "suspension_count_defiance_only": "Willful Defiance",
    "suspension_count_other_reasons": "Other",
}

# Use UCLA color palette from standardized palette
REASON_PALETTE = DISCIPLINE_REASON_PALETTE.copy()

LEVEL_ORDER = ["Elementary", "Middle", "High"]

# Handle PROJECT_ROOT for both script and interactive contexts
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:  # pragma: no cover - interactive contexts without __file__
    # When __file__ is not available (interactive/reticulate), search for project root
    start = Path.cwd().resolve()
    for candidate in [start, *start.parents]:
        if (candidate / "data-stage").exists() and (candidate / "graph_scripts").exists():
            PROJECT_ROOT = candidate
            break
    else:
        # Fallback to current directory if markers not found
        PROJECT_ROOT = Path.cwd()

DEFAULT_DATA_PATH = PROJECT_ROOT / "data-stage" / "susp_v6_long.parquet"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "22_suspension_reason_proportions"
DEFAULT_IMAGE_FORMAT = "png"
AUDIT_DIR = ensure_audit_dir(PROJECT_ROOT)

# Methodology explanation for charts (simplified for non-experts)
METHODOLOGY_TEXT = (
    "How to Read This Chart:\n"
    "• Colored lines (left axis) show each category as a % of all suspensions that year\n"
    "• Light blue bars (right axis) show the total number of suspensions\n"
    "• Data includes all traditional (non-charter) elementary, middle, and high schools statewide"
)

# ----------------------------------------------------------------------------
# Data preparation
# ----------------------------------------------------------------------------

read_columns = [
    "academic_year",
    "school_level",
    "subgroup",
    "category_type",
    "cumulative_enrollment",
    "charter_yn_std",
    *REASON_COLUMNS.keys(),
]


def load_data(data_path: Path) -> pd.DataFrame:
    """Return the suspension detail parquet as a pandas DataFrame."""

    print(f"Loading suspension detail parquet from {data_path}…")
    parquet_table = pq.read_table(data_path, columns=read_columns)
    return parquet_table.to_pandas()


def prepare_proportion_data(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter and aggregate suspension reasons to calculate proportions.

    Returns:
        tuple: (proportion_df, totals_df)
            - proportion_df: DataFrame with academic_year, reason_label, and proportion columns
            - totals_df: DataFrame with academic_year and total_suspensions columns
    """

    # Filter to traditional schools, all students, race/ethnicity category
    filtered = (
        raw_df
        .loc[
            (raw_df["category_type"] == "Race/Ethnicity")
            & (raw_df["subgroup"] == "All Students")
            & (raw_df["school_level"].isin(LEVEL_ORDER))
            & (raw_df["charter_yn_std"].fillna("Unknown") == "No")
        ]
        .copy()
    )

    filtered["academic_year"] = pd.Categorical(
        filtered["academic_year"],
        ordered=True,
        categories=sorted(filtered["academic_year"].dropna().unique()),
    )

    # Aggregate by academic year
    agg_dict = {col: "sum" for col in REASON_COLUMNS}
    agg_dict["cumulative_enrollment"] = "sum"

    aggregated = (
        filtered
        .groupby(["academic_year"], observed=True, dropna=False)
        .agg(agg_dict)
        .reset_index()
    )

    # Audit counts
    aggregated = audit_counts_against_enrollment(
        aggregated,
        count_columns=list(REASON_COLUMNS.keys()),
        enrollment_column="cumulative_enrollment",
        context="21_proportions.aggregated",
        audit_dir=AUDIT_DIR,
    )

    # Calculate total suspensions per year
    aggregated["total_suspensions"] = aggregated[list(REASON_COLUMNS.keys())].sum(axis=1)

    # Prepare totals DataFrame
    totals_df = aggregated[["academic_year", "total_suspensions"]].copy()

    # Melt to long format for proportions
    melted = aggregated.melt(
        id_vars=["academic_year", "total_suspensions"],
        value_vars=list(REASON_COLUMNS.keys()),
        var_name="reason",
        value_name="count",
    )

    melted["reason_label"] = melted["reason"].map(REASON_COLUMNS)

    # Calculate proportions (count / total_suspensions)
    melted["proportion"] = np.where(
        melted["total_suspensions"] > 0,
        melted["count"] / melted["total_suspensions"],
        np.nan,
    )

    melted = melted.dropna(subset=["reason_label"]).copy()

    # Clip proportions to [0, 1] range
    melted["proportion"] = melted["proportion"].clip(0, 1)

    if melted.empty:
        raise SystemExit("No suspension proportion data available after filtering.")

    return melted, totals_df


# ----------------------------------------------------------------------------
# Plotting
# ----------------------------------------------------------------------------

def _format_percent(value: float) -> str:
    """Format a proportion as a percentage string."""
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return "NA"
    return f"{value * 100:.1f}%"


def _format_count(value: float) -> str:
    """Format a count with thousands separator."""
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return "NA"
    return f"{value:,.0f}"


def _format_count_compact(value: float) -> str:
    """Format a count in compact notation (e.g., 120K, 1.2M) for chart labels."""
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return "NA"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.0f}K"
    else:
        return f"{value:.0f}"


def _add_wrapped_text(
    fig,
    x: float,
    y: float,
    text: str,
    fontsize: int,
    color: str,
    max_width: int = 120,
    **kwargs
) -> None:
    """Add wrapped text to a figure at the specified position.

    Args:
        fig: Matplotlib figure object
        x: X position (0-1 in figure coordinates)
        y: Y position (0-1 in figure coordinates)
        text: Text to wrap and display
        fontsize: Font size
        color: Text color
        max_width: Maximum character width before wrapping
        **kwargs: Additional arguments passed to fig.text()
    """
    wrapped = textwrap.fill(text, width=max_width)
    fig.text(x, y, wrapped, fontsize=fontsize, color=color, **kwargs)


def plot_proportions_with_totals(
    proportion_df: pd.DataFrame,
    totals_df: pd.DataFrame,
    output_dir: Path,
    image_format: str = DEFAULT_IMAGE_FORMAT,
    dpi: int | None = None,
) -> None:
    """Render a chart showing suspension reason proportions with total suspensions as background."""

    if proportion_df.empty or totals_df.empty:
        print("Skipping proportion chart: no data to plot.")
        return

    # Get unique years
    if isinstance(proportion_df["academic_year"].dtype, pd.CategoricalDtype):
        years = proportion_df["academic_year"].cat.categories.tolist()
    else:
        years = sorted(proportion_df["academic_year"].unique())

    x_positions = {year: idx for idx, year in enumerate(years)}

    # Create figure with dual y-axes
    fig, ax1 = plt.subplots(figsize=(14, 8), dpi=dpi or 300)
    fig.patch.set_facecolor("white")
    ax1.set_facecolor("white")

    # Create second y-axis for total suspensions
    ax2 = ax1.twinx()

    # --- BACKGROUND: Bar chart of total suspensions (ax2 - right y-axis) ---
    totals_sorted = totals_df.sort_values("academic_year")
    bar_xs = [x_positions[year] for year in totals_sorted["academic_year"]]
    bar_ys = totals_sorted["total_suspensions"].to_numpy()

    bars = ax2.bar(
        bar_xs,
        bar_ys,
        color=DISCIPLINE_BASE_PALETTE["Lighter Blue"],
        alpha=0.25,  # Slightly increased from 0.2 for better visibility
        width=0.6,
        edgecolor=DISCIPLINE_BASE_PALETTE["Darker Blue"],
        linewidth=1.0,
        label="Total Suspensions (Background)",
        zorder=1,
    )

    # Add data labels to bars
    for bar, value in zip(bars, bar_ys):
        height = bar.get_height()
        # Position label at top of bar
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            height,
            _format_count_compact(value),
            ha="center",
            va="bottom",
            fontsize=8,
            fontweight="bold",
            color=DISCIPLINE_BASE_PALETTE["Darker Blue"],
            zorder=2,  # Ensure labels appear above bars
        )

    # Configure right y-axis (total suspensions)
    ax2.set_ylabel(
        "Total Suspensions\n(Background Bars)",
        color=DISCIPLINE_BASE_PALETTE["Darker Blue"],
        fontweight="bold",
        fontsize=11,
    )
    ax2.tick_params(axis="y", colors=DISCIPLINE_BASE_PALETTE["Darker Blue"])
    # Add padding at top to accommodate labels
    ax2.set_ylim(bottom=0, top=max(bar_ys) * 1.08)

    # --- FOREGROUND: Line charts of proportions (ax1 - left y-axis) ---
    for reason_label in REASON_COLUMNS.values():
        reason_df = proportion_df[proportion_df["reason_label"] == reason_label].copy()
        if reason_df.empty:
            continue

        reason_df = reason_df.sort_values("academic_year")
        xs = [x_positions[year] for year in reason_df["academic_year"]]
        ys = reason_df["proportion"].to_numpy() * 100  # Convert to percentage

        # Special styling for Willful Defiance
        if reason_label == "Willful Defiance":
            linestyle = "--"
            linewidth = 3.5
            marker = "s"
            markersize = 9
            alpha = 1.0
        # De-emphasize stable categories (Other, Weapons) with transparency
        elif reason_label in ["Other", "Weapons"]:
            linestyle = "-"
            linewidth = 2.5
            marker = "o"
            markersize = 7
            alpha = 0.25  # 75% transparent to reduce visual noise
        else:
            linestyle = "-"
            linewidth = 2.5
            marker = "o"
            markersize = 7
            alpha = 0.9

        color = REASON_PALETTE.get(reason_label, DISCIPLINE_BASE_PALETTE["Grey"])

        ax1.plot(
            xs,
            ys,
            label=reason_label,
            color=color,
            linewidth=linewidth,
            marker=marker,
            markersize=markersize,
            linestyle=linestyle,
            alpha=alpha,
            zorder=10,  # Ensure lines appear above bars
        )

    # Configure left y-axis (proportions)
    ax1.set_ylabel(
        "Percentage of Total Suspensions\n(Colored Lines)",
        color=TEXT_COLOR,
        fontweight="bold",
        fontsize=11,
    )
    ax1.tick_params(axis="y", colors=TEXT_COLOR)
    ax1.set_ylim(0, 100)

    # Configure x-axis
    ax1.set_xticks(list(x_positions.values()))
    ax1.set_xticklabels(years, rotation=45, ha="right", color=TEXT_COLOR, fontweight="bold")
    ax1.set_xlabel("Academic Year", color=TEXT_COLOR, fontweight="bold", fontsize=11)

    # Grid styling (on ax1 only)
    ax1.grid(True, axis="y", color=GRID_COLOR, linestyle="-", linewidth=0.8, alpha=0.5, zorder=0)
    ax1.grid(False, axis="x")

    # Remove spines for cleaner look
    for spine in ax1.spines.values():
        spine.set_visible(False)
    for spine in ax2.spines.values():
        spine.set_visible(False)

    # Create separate legends for better clarity
    # Get handles and labels from both axes
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()

    # First legend: Suspension reason lines (top row)
    legend1 = ax1.legend(
        handles1,
        labels1,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.22),
        ncol=3,
        frameon=True,
        fancybox=False,
        edgecolor=GRID_COLOR,
        labelcolor=TEXT_COLOR,
        fontsize=9,
        title="Suspension Reason Categories (Lines)",
        title_fontsize=9,
    )
    legend1.get_title().set_fontweight("bold")
    for text in legend1.get_texts():
        text.set_fontweight("bold")

    # Second legend: Total suspensions bar (bottom row)
    legend2 = ax1.legend(
        handles2,
        labels2,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.38),
        ncol=1,
        frameon=True,
        fancybox=False,
        edgecolor=GRID_COLOR,
        labelcolor=DISCIPLINE_BASE_PALETTE["Darker Blue"],
        fontsize=9,
        title="Context",
        title_fontsize=9,
    )
    legend2.get_title().set_fontweight("bold")
    for text in legend2.get_texts():
        text.set_fontweight("bold")

    # Add first legend back to the plot (matplotlib only keeps the last one by default)
    ax1.add_artist(legend1)

    ax1.margins(x=0.03)

    # Add title, subtitle, methodology, and citation
    title = "Traditional Schools Statewide — Suspension Reason Trends"
    subtitle = "Key Finding: As Willful Defiance drops, other categories increase — but total suspensions stay relatively stable"

    fig.text(0.08, 0.97, title, fontsize=14, fontweight="bold", ha="left", color=TEXT_COLOR)
    fig.text(
        0.08,
        0.94,
        subtitle,
        fontsize=10,
        ha="left",
        color=DISCIPLINE_BASE_PALETTE["Darker Blue"],
        style="italic",
    )

    _add_wrapped_text(
        fig,
        0.08,
        0.90,
        METHODOLOGY_TEXT,
        fontsize=8,
        color=TEXT_COLOR,
        ha="left",
        va="top",
        max_width=180,
    )
    _add_wrapped_text(
        fig,
        0.08,
        0.02,
        STANDARD_CITATION,
        fontsize=6,
        color=CAPTION_COLOR,
        ha="left",
        va="bottom",
        max_width=180,
    )

    # Adjusted bottom margin to accommodate two-row legend
    fig.subplots_adjust(left=0.08, right=0.92, top=0.81, bottom=0.30)

    # Save figure
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = image_format.lower().lstrip(".")
    output_path = output_dir / f"suspension_reason_proportions_with_totals.{suffix}"
    save_kwargs = {"format": suffix}
    if suffix != "svg" and dpi is not None:
        save_kwargs["dpi"] = dpi
    fig.savefig(output_path, **save_kwargs)
    plt.close(fig)
    print(f"Saved chart: {output_path}")


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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_df = load_data(args.data_path)
    proportion_df, totals_df = prepare_proportion_data(raw_df)
    dpi = args.dpi if args.image_format != "svg" else None
    plot_proportions_with_totals(
        proportion_df,
        totals_df,
        output_dir=args.output_dir,
        image_format=args.image_format,
        dpi=dpi,
    )
    print(f"Proportion with totals chart generation complete.")


if __name__ == "__main__":
    main()
