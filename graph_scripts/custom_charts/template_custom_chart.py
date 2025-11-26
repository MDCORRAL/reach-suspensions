#!/usr/bin/env python3
"""[DESCRIPTION]: Brief description of what this custom chart shows.

[DETAILED EXPLANATION]:
Explain what makes this chart different from standard outputs,
what specific question it answers, and who the intended audience is.

Output: outputs/custom_charts/[OUTPUT_FILENAME]
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add graph_scripts to path
SCRIPT_DIR = Path(__file__).resolve().parent
GRAPH_SCRIPTS_DIR = SCRIPT_DIR.parent
if str(GRAPH_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(GRAPH_SCRIPTS_DIR))

import matplotlib.pyplot as plt
import pandas as pd

from palette_utils import (
    DISCIPLINE_BASE_PALETTE,
    DISCIPLINE_REASON_PALETTE,
    STANDARD_CITATION,
)
from data_sources import get_project_root, load_susp_v6_long, filter_traditional_schools
from plotting_helpers import (
    apply_ucla_style,
    add_standard_labels,
    smooth_line_data,
    calculate_y_limit,
)
from data_validations import audit_counts_against_enrollment, sanitize_rate_column, ensure_audit_dir

# ============================================================================
# CONFIGURATION - EDIT THIS SECTION
# ============================================================================

# Chart details
CHART_TITLE = "Your Chart Title Here"
CHART_SUBTITLE = "Descriptive subtitle with time period and scope"
OUTPUT_FILENAME = "your_chart_name.png"

# Data filters
SCHOOL_LEVELS = ["Elementary", "Middle", "High"]  # Which levels to include
LOCALES = None  # None = all, or ["City", "Suburban", "Town", "Rural"]
SUBGROUP = "All Students"  # Student subgroup to analyze

# Styling options
SMOOTHNESS = 300  # For smooth_line_data() - higher = smoother
LINE_WIDTH = 2.2
DPI = 300
FIGURE_SIZE = (10, 6)
SHOW_DATA_LABELS = False  # Set to True to show data labels
SHOW_MARKERS = True  # Set to False to hide markers

# Colors
TEXT_COLOR = DISCIPLINE_BASE_PALETTE["Darkest Blue"]
CAPTION_COLOR = DISCIPLINE_BASE_PALETTE["Grey"]
GRID_COLOR = DISCIPLINE_BASE_PALETTE["Lighter Blue"]

# ============================================================================
# DATA LOADING AND PREPARATION
# ============================================================================

def load_and_prepare_data() -> pd.DataFrame:
    """Load and prepare data for the chart.

    Customize this function based on what data you need.

    Returns:
        DataFrame ready for plotting
    """
    print("Loading data...")

    # Load long-format suspension data
    # Specify only the columns you need to improve performance
    columns = [
        "academic_year",
        "school_level",
        "subgroup",
        "cumulative_enrollment",
        "total_suspensions",
        # Add other columns as needed
    ]

    df = load_susp_v6_long(columns=columns)

    # Filter to traditional schools (optional)
    # df = filter_traditional_schools(df)

    # Apply your filters
    df = df[
        (df["subgroup"] == SUBGROUP)
        & (df["school_level"].isin(SCHOOL_LEVELS))
    ].copy()

    # If filtering by locale
    if LOCALES is not None and "locale_simple" in df.columns:
        df = df[df["locale_simple"].isin(LOCALES)]

    print(f"  Loaded {len(df):,} records")

    # Aggregate data
    # Customize grouping and aggregation based on your needs
    aggregated = (
        df
        .groupby(["academic_year"], observed=True)
        .agg({
            "cumulative_enrollment": "sum",
            "total_suspensions": "sum",
        })
        .reset_index()
    )

    # Calculate rates
    aggregated["rate"] = aggregated.apply(
        lambda row: row["total_suspensions"] / row["cumulative_enrollment"]
        if row["cumulative_enrollment"] > 0
        else float("nan"),
        axis=1,
    )

    # Validate data quality
    PROJECT_ROOT = get_project_root()
    AUDIT_DIR = ensure_audit_dir(PROJECT_ROOT)

    aggregated = audit_counts_against_enrollment(
        aggregated,
        count_columns=["total_suspensions"],
        enrollment_column="cumulative_enrollment",
        context="template_custom_chart",
        audit_dir=AUDIT_DIR,
    )

    aggregated = sanitize_rate_column(
        aggregated,
        rate_column="rate",
        context="template_custom_chart",
        audit_dir=AUDIT_DIR,
    )

    print(f"  Prepared {len(aggregated)} data points")

    return aggregated


# ============================================================================
# PLOTTING
# ============================================================================

def create_chart(df: pd.DataFrame, output_path: Path) -> None:
    """Create the custom chart.

    Customize this function to create your specific visualization.

    Args:
        df: Prepared DataFrame
        output_path: Path to save PNG file
    """
    print("Creating chart...")

    # Extract years for x-axis
    years = sorted(df["academic_year"].unique())
    x_positions = {year: idx for idx, year in enumerate(years)}

    # Calculate y-axis limit
    max_rate = df["rate"].max()
    y_limit = calculate_y_limit(max_rate, padding=0.18)

    # Create figure
    fig, ax = plt.subplots(figsize=FIGURE_SIZE, dpi=DPI)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    # Plot data
    # CUSTOMIZE THIS SECTION FOR YOUR CHART

    # Example: Simple line plot
    xs = [x_positions[year] for year in df["academic_year"]]
    ys = df["rate"].to_numpy()

    # Option 1: Straight lines
    if SMOOTHNESS == 0:
        ax.plot(
            xs,
            ys,
            color=DISCIPLINE_BASE_PALETTE["UCLA Blue"],
            linewidth=LINE_WIDTH,
            marker="o" if SHOW_MARKERS else None,
            markersize=5.3,
            markeredgecolor="white",
            markeredgewidth=0.6,
            label="Your Data",
        )
    else:
        # Option 2: Smooth interpolated curves
        x_smooth, y_smooth = smooth_line_data(xs, ys, smoothness=SMOOTHNESS)
        ax.plot(
            x_smooth,
            y_smooth,
            color=DISCIPLINE_BASE_PALETTE["UCLA Blue"],
            linewidth=LINE_WIDTH,
            label="Your Data",
        )

    # Add data labels (optional)
    if SHOW_DATA_LABELS:
        for x, y in zip(xs, ys):
            ax.text(
                x,
                y,
                f"{y * 100:.1f}%",
                color=DISCIPLINE_BASE_PALETTE["UCLA Blue"],
                fontsize=9,
                fontweight="bold",
                ha="center",
                va="bottom",
            )

    # Apply UCLA styling
    apply_ucla_style(ax, years, y_limit, show_x_grid=True, show_y_grid=True)

    # Axis labels
    ax.set_ylabel("Suspension Rate (Percent)", color=TEXT_COLOR, fontweight="bold")
    ax.set_xlabel("Academic Year", color=TEXT_COLOR, fontweight="bold")

    # Grid
    ax.grid(True, axis="y", color=GRID_COLOR, linestyle="-", linewidth=0.8)
    ax.grid(False, axis="x")

    # Remove spines
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Legend (if needed)
    legend = ax.legend(
        loc="upper right",
        frameon=False,
        labelcolor=TEXT_COLOR,
    )
    for text in legend.get_texts():
        text.set_fontweight("bold")

    # Add standard labels
    add_standard_labels(
        fig,
        title=CHART_TITLE,
        subtitle=CHART_SUBTITLE,
        citation=STANDARD_CITATION,
        title_y=0.98,
        subtitle_y=0.95,
        citation_y=0.02,
    )

    # Adjust layout
    fig.subplots_adjust(left=0.10, right=0.95, top=0.85, bottom=0.18)

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=DPI, format="png")
    plt.close(fig)

    print(f"  Saved: {output_path}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    """Main execution function."""
    print("=" * 70)
    print("Generating Custom Chart")
    print("=" * 70)

    # Load and prepare data
    df = load_and_prepare_data()

    # Create output path
    PROJECT_ROOT = get_project_root()
    output_dir = PROJECT_ROOT / "outputs" / "custom_charts"
    output_path = output_dir / OUTPUT_FILENAME

    # Create chart
    create_chart(df, output_path)

    print("=" * 70)
    print("✓ Complete!")
    print(f"  Output: {output_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
