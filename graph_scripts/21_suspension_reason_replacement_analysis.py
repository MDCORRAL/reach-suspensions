"""Generate UCLA-branded chart showing suspension reason "replacement" phenomenon.

This module creates a dual-panel visualization showing:
1. Absolute suspension rates by category over time
2. Indexed trends (2017-18 = 100%) to highlight diverging trajectories

The goal is to clearly show how willful defiance suspensions have declined while
other suspension categories have increased, suggesting a possible "replacement"
phenomenon where schools may be shifting from one category to others.
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
        "Missing required Python packages for 21_suspension_reason_replacement_analysis.\n"
        "Install the dependencies before running (e.g., pip install -r graph_scripts/requirements.txt).\n"
        "Missing modules:\n" + "\n".join(hints)
    )
    raise SystemExit(message)

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "custom"
DEFAULT_IMAGE_FORMAT = "png"
AUDIT_DIR = ensure_audit_dir(PROJECT_ROOT)

# Methodology explanation for charts
METHODOLOGY_TEXT = (
    "Methodology: Suspension rates calculated as suspensions divided by cumulative enrollment for each academic year. "
    "Data aggregated from all traditional (non-charter) elementary, middle, and high schools statewide. "
    "Chart shows absolute rates to highlight the diverging trends between declining willful defiance suspensions "
    "and increasing rates in other categories."
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


def prepare_rate_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Filter and aggregate suspension reasons to calculate rates by category.

    Returns a DataFrame with academic_year, reason_label, and rate columns.
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
        context="21_replacement.aggregated",
        audit_dir=AUDIT_DIR,
    )

    # Melt to long format
    melted = aggregated.melt(
        id_vars=["academic_year", "cumulative_enrollment"],
        value_vars=list(REASON_COLUMNS.keys()),
        var_name="reason",
        value_name="count",
    )

    melted["reason_label"] = melted["reason"].map(REASON_COLUMNS)

    # Calculate rates
    melted["rate"] = np.where(
        melted["cumulative_enrollment"] > 0,
        melted["count"] / melted["cumulative_enrollment"],
        np.nan,
    )

    melted = melted.dropna(subset=["reason_label"]).copy()
    melted = sanitize_rate_column(
        melted,
        rate_column="rate",
        context="21_replacement.melted",
        audit_dir=AUDIT_DIR,
    )

    if melted.empty:
        raise SystemExit("No suspension reason data available after filtering.")

    return melted


# ----------------------------------------------------------------------------
# Plotting
# ----------------------------------------------------------------------------

def _format_percent(value: float) -> str:
    """Format a proportion as a percentage string."""
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return "NA"
    return f"{value * 100:.1f}%"


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


def plot_replacement_phenomenon(
    df: pd.DataFrame,
    output_dir: Path,
    image_format: str = DEFAULT_IMAGE_FORMAT,
    dpi: int | None = None,
) -> None:
    """Render a chart showing suspension reason trends highlighting replacement phenomenon."""

    if df.empty:
        print("Skipping replacement chart: no data to plot.")
        return

    # Get unique years
    if isinstance(df["academic_year"].dtype, pd.CategoricalDtype):
        years = df["academic_year"].cat.categories.tolist()
    else:
        years = sorted(df["academic_year"].unique())

    x_positions = {year: idx for idx, year in enumerate(years)}

    # Create figure with single panel
    fig, ax = plt.subplots(figsize=(12, 8), dpi=dpi or 300)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    # Plot each reason category
    for reason_label in REASON_COLUMNS.values():
        reason_df = df[df["reason_label"] == reason_label].copy()
        if reason_df.empty:
            continue

        reason_df = reason_df.sort_values("academic_year")
        xs = [x_positions[year] for year in reason_df["academic_year"]]
        ys = reason_df["rate"].to_numpy() * 100

        # Special styling for Willful Defiance
        if reason_label == "Willful Defiance":
            linestyle = "--"
            linewidth = 3.0
            marker = "s"
            markersize = 8
        else:
            linestyle = "-"
            linewidth = 2.5
            marker = "o"
            markersize = 6

        color = REASON_PALETTE.get(reason_label, DISCIPLINE_BASE_PALETTE["Grey"])

        ax.plot(
            xs,
            ys,
            label=reason_label,
            color=color,
            linewidth=linewidth,
            marker=marker,
            markersize=markersize,
            linestyle=linestyle,
            alpha=0.9,
        )

        # Add value labels at endpoints only for cleaner look
        if len(xs) > 0:
            # First point
            ax.text(
                xs[0],
                ys[0],
                _format_percent(reason_df["rate"].iloc[0]),
                color=color,
                fontsize=8,
                fontweight="bold",
                ha="right",
                va="bottom",
            )
            # Last point
            ax.text(
                xs[-1],
                ys[-1],
                _format_percent(reason_df["rate"].iloc[-1]),
                color=color,
                fontsize=8,
                fontweight="bold",
                ha="left",
                va="bottom",
            )

    # Configure axes
    ax.set_xticks(list(x_positions.values()))
    ax.set_xticklabels(years, rotation=45, ha="right", color=TEXT_COLOR, fontweight="bold")
    ax.tick_params(axis="y", colors=TEXT_COLOR)

    ax.set_ylabel("Suspension Rate (Percent)", color=TEXT_COLOR, fontweight="bold", fontsize=11)
    ax.set_xlabel("Academic Year", color=TEXT_COLOR, fontweight="bold", fontsize=11)

    # Grid styling
    ax.grid(True, axis="y", color=GRID_COLOR, linestyle="-", linewidth=0.8, alpha=0.5)
    ax.grid(False, axis="x")

    # Remove spines for cleaner look
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Legend positioned BELOW x-axis label
    legend = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.18),
        ncol=3,
        frameon=False,
        labelcolor=TEXT_COLOR,
        fontsize=10,
    )
    for text in legend.get_texts():
        text.set_fontweight("bold")

    ax.set_ylim(bottom=0)
    ax.margins(x=0.05, y=0.08)

    # Add annotation callouts for key trends
    # Find willful defiance data
    defiance_df = df[df["reason_label"] == "Willful Defiance"].sort_values("academic_year")
    if not defiance_df.empty and len(defiance_df) >= 2:
        first_rate = defiance_df["rate"].iloc[0] * 100
        last_rate = defiance_df["rate"].iloc[-1] * 100
        pct_change = ((last_rate - first_rate) / first_rate) * 100 if first_rate > 0 else 0

        # Add annotation box
        annotation_text = f"Willful Defiance:\n{pct_change:.0f}% decline"
        ax.text(
            0.98,
            0.95,
            annotation_text,
            transform=ax.transAxes,
            fontsize=10,
            fontweight="bold",
            color="red",
            ha="right",
            va="top",
            bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor="red", linewidth=2)
        )

    # Add title, subtitle, methodology, and citation
    title = "Traditional Schools Statewide — Suspension Rate Trends by Reason"
    subtitle = "Showing Potential \"Replacement\" Phenomenon: Willful Defiance Declining, Other Categories Increasing"

    fig.text(0.08, 0.97, title, fontsize=14, fontweight="bold", ha="left", color=TEXT_COLOR)
    fig.text(0.08, 0.94, subtitle, fontsize=10, ha="left", color=DISCIPLINE_BASE_PALETTE["Darker Blue"], style="italic")

    _add_wrapped_text(
        fig,
        0.08,
        0.91,
        METHODOLOGY_TEXT,
        fontsize=7,
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

    fig.subplots_adjust(left=0.08, right=0.96, top=0.82, bottom=0.28)

    # Save figure
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = image_format.lower().lstrip(".")
    output_path = output_dir / f"suspension_reason_replacement_phenomenon.{suffix}"
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
    rate_df = prepare_rate_data(raw_df)
    dpi = args.dpi if args.image_format != "svg" else None
    plot_replacement_phenomenon(
        rate_df,
        output_dir=args.output_dir,
        image_format=args.image_format,
        dpi=dpi,
    )
    print(f"Replacement phenomenon chart generation complete.")


if __name__ == "__main__":
    main()
