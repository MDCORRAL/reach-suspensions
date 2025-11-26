#!/usr/bin/env python3
"""Generate smoothed statewide suspension reason trends without data labels.

This custom chart shows the same data as the standard
'20_suspension_reason_trends_all_traditional_statewide.png' output
but with smoothed lines and no data labels for a cleaner, more
polished presentation suitable for publications and presentations.

Features:
- Smooth interpolated curves instead of point-to-point lines
- No data labels (cleaner visual)
- Same UCLA branding and colors
- Standard citation and subtitle

Output: outputs/custom_charts/smooth_statewide_reasons.png
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
    slugify_filename,
)
from data_validations import audit_counts_against_enrollment, sanitize_rate_column, ensure_audit_dir

# ============================================================================
# CONFIGURATION
# ============================================================================

# Chart details
CHART_TITLE = "All Traditional Schools — Statewide Suspension Rates by Reason"
CHART_SUBTITLE = "All traditional public schools, 2017-18 through 2023-24 (no statewide reporting in 2020-21)"
OUTPUT_FILENAME = "smooth_statewide_reasons.png"

# Data filters
SCHOOL_LEVELS = ["Elementary", "Middle", "High"]  # Included levels
SUBGROUP = "All Students"

# Styling
SMOOTHNESS = 300  # Number of interpolation points (higher = smoother)
LINE_WIDTH = 2.5
DPI = 300
FIGURE_SIZE = (10, 6)

# Colors
TEXT_COLOR = DISCIPLINE_BASE_PALETTE["Darkest Blue"]
CAPTION_COLOR = DISCIPLINE_BASE_PALETTE["Grey"]
GRID_COLOR = DISCIPLINE_BASE_PALETTE["Lighter Blue"]

# Reason definitions (matching main scripts)
REASON_COLUMNS = {
    "suspension_count_violent_incident_injury": "Violent (Injury)",
    "suspension_count_violent_incident_no_injury": "Violent (No Injury)",
    "suspension_count_weapons_possession": "Weapons",
    "suspension_count_illicit_drug_related": "Illicit Drugs",
    "suspension_count_defiance_only": "Willful Defiance",
    "suspension_count_other_reasons": "Other",
}

REASON_PALETTE = DISCIPLINE_REASON_PALETTE.copy()

# ============================================================================
# DATA LOADING AND PREPARATION
# ============================================================================

def load_and_prepare_data() -> pd.DataFrame:
    """Load and prepare statewide traditional school data.

    Returns:
        DataFrame with academic_year, reason_label, rate columns
    """
    print("Loading suspension data...")

    # Load data
    columns = [
        "academic_year",
        "school_level",
        "subgroup",
        "category_type",
        "cumulative_enrollment",
        "charter_yn_std",
        *REASON_COLUMNS.keys(),
    ]

    df = load_susp_v6_long(columns=columns)

    # Filter to traditional schools
    df = filter_traditional_schools(df, charter_column="charter_yn_std")

    # Filter to relevant data
    df = df[
        (df["category_type"] == "Race/Ethnicity")
        & (df["subgroup"] == SUBGROUP)
        & (df["school_level"].isin(SCHOOL_LEVELS))
    ].copy()

    print(f"  Loaded {len(df):,} records")

    # Aggregate by year
    agg_dict = {col: "sum" for col in REASON_COLUMNS.keys()}
    agg_dict["cumulative_enrollment"] = "sum"

    aggregated = (
        df
        .groupby(["academic_year"], observed=True, dropna=False)
        .agg(agg_dict)
        .reset_index()
    )

    # Validate data quality
    PROJECT_ROOT = get_project_root()
    AUDIT_DIR = ensure_audit_dir(PROJECT_ROOT)

    aggregated = audit_counts_against_enrollment(
        aggregated,
        count_columns=list(REASON_COLUMNS.keys()),
        enrollment_column="cumulative_enrollment",
        context="smooth_statewide_reasons",
        audit_dir=AUDIT_DIR,
    )

    # Melt to long format
    melted = aggregated.melt(
        id_vars=["academic_year", "cumulative_enrollment"],
        value_vars=list(REASON_COLUMNS.keys()),
        var_name="reason",
        value_name="count",
    )

    # Add labels and calculate rates
    melted["reason_label"] = melted["reason"].map(REASON_COLUMNS)
    melted["rate"] = melted.apply(
        lambda row: row["count"] / row["cumulative_enrollment"]
        if row["cumulative_enrollment"] > 0
        else float("nan"),
        axis=1,
    )

    melted = melted.dropna(subset=["reason_label", "rate"])

    # Sanitize rates
    melted = sanitize_rate_column(
        melted,
        rate_column="rate",
        context="smooth_statewide_reasons",
        audit_dir=AUDIT_DIR,
    )

    # Sort academic years
    melted["academic_year"] = pd.Categorical(
        melted["academic_year"],
        ordered=True,
        categories=sorted(melted["academic_year"].unique()),
    )

    print(f"  Prepared data: {len(melted['academic_year'].unique())} years, {len(REASON_COLUMNS)} reasons")

    return melted


# ============================================================================
# PLOTTING
# ============================================================================

def create_smooth_chart(df: pd.DataFrame, output_path: Path) -> None:
    """Create smoothed line chart without data labels.

    Args:
        df: Prepared DataFrame with academic_year, reason_label, rate
        output_path: Path to save PNG file
    """
    print("Creating chart...")

    # Extract years
    years = df["academic_year"].cat.categories.tolist()
    x_positions = {year: idx for idx, year in enumerate(years)}

    # Calculate y-axis limit
    max_rate = df["rate"].max()
    y_limit = calculate_y_limit(max_rate, padding=0.18)

    # Create figure
    fig, ax = plt.subplots(figsize=FIGURE_SIZE, dpi=DPI)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    # Plot each reason with smooth curves
    for reason_label, color in REASON_PALETTE.items():
        reason_df = df[df["reason_label"] == reason_label].copy()
        if reason_df.empty:
            continue

        reason_df = reason_df.sort_values("academic_year")

        # Get data points
        xs = [x_positions[year] for year in reason_df["academic_year"]]
        ys = reason_df["rate"].to_numpy()

        # Smooth the line
        x_smooth, y_smooth = smooth_line_data(xs, ys, smoothness=SMOOTHNESS)

        # Special styling for Willful Defiance (dashed line)
        linestyle = "--" if reason_label == "Willful Defiance" else "-"

        # Plot smooth line (NO MARKERS, NO LABELS)
        ax.plot(
            x_smooth,
            y_smooth,
            label=reason_label,
            color=color,
            linewidth=LINE_WIDTH,
            linestyle=linestyle,
            zorder=3,  # Lines on top of grid
        )

    # Apply UCLA styling
    apply_ucla_style(ax, years, y_limit, show_x_grid=True, show_y_grid=True)

    # Axis labels
    ax.set_ylabel("Suspension Rate (Percent)", color=TEXT_COLOR, fontweight="bold")
    ax.set_xlabel("Academic Year", color=TEXT_COLOR, fontweight="bold")

    # Grid styling
    ax.grid(True, axis="y", color=GRID_COLOR, linestyle="-", linewidth=0.8)
    ax.grid(False, axis="x")

    # Remove spines
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Legend
    legend = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.2),
        ncol=3,
        frameon=False,
        labelcolor=TEXT_COLOR,
    )
    for text in legend.get_texts():
        text.set_fontweight("bold")

    # Add title, subtitle and citation using fig.text for complete control
    fig.text(0.10, 0.96, CHART_TITLE, fontsize=14, fontweight="bold", ha="left", color=TEXT_COLOR)
    fig.text(0.10, 0.93, CHART_SUBTITLE, fontsize=10, ha="left", color=TEXT_COLOR)
    fig.text(0.10, 0.04, STANDARD_CITATION, fontsize=8, ha="left", color=CAPTION_COLOR)

    # Adjust layout to accommodate labels
    fig.subplots_adjust(left=0.12, right=0.96, top=0.88, bottom=0.26)

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=DPI, format="png", bbox_inches='tight')
    plt.close(fig)

    print(f"  Saved: {output_path}")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    """Main execution function."""
    print("=" * 70)
    print("Generating Smooth Statewide Suspension Reasons Chart")
    print("=" * 70)

    # Load and prepare data
    df = load_and_prepare_data()

    # Create output path
    PROJECT_ROOT = get_project_root()
    output_dir = PROJECT_ROOT / "outputs" / "custom_charts"
    output_path = output_dir / OUTPUT_FILENAME

    # Create chart
    create_smooth_chart(df, output_path)

    print("=" * 70)
    print("✓ Complete!")
    print(f"  Output: {output_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
