"""Generate UCLA-branded suspension reason composition chart for statewide traditional schools.

This module creates a stacked area chart showing how the composition of suspensions
has changed over time. Rather than raw rates, it shows the proportion of total
suspensions accounted for by "Willful Defiance" vs "Other Reasons" combined.

For each year, share is calculated as: rate_category / sum(rate_all_categories).

The chart aggregates all traditional (non-charter) schools statewide across
elementary, middle, and high school levels.
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
        "Missing required Python packages for 21_suspension_reason_composition_statewide.\n"
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

REASON_COLUMNS = {
    "suspension_count_violent_incident_injury": "Violent (Injury)",
    "suspension_count_violent_incident_no_injury": "Violent (No Injury)",
    "suspension_count_weapons_possession": "Weapons",
    "suspension_count_illicit_drug_related": "Illicit Drugs",
    "suspension_count_defiance_only": "Willful Defiance",
    "suspension_count_other_reasons": "Other",
}

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
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "21_suspension_reason_composition"
DEFAULT_IMAGE_FORMAT = "png"
AUDIT_DIR = ensure_audit_dir(PROJECT_ROOT)

# Methodology explanation for charts
METHODOLOGY_TEXT = (
    "Methodology: Suspension reason composition is calculated by dividing each reason "
    "category's rate by the sum of all reason category rates for that year, showing "
    "the percentage of total suspensions attributable to each category. Data aggregated "
    "from all traditional (non-charter) elementary, middle, and high schools statewide. "
    "Rates represent suspensions divided by cumulative enrollment."
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


def prepare_composition_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Filter and aggregate suspension reasons to calculate composition shares.

    Returns a DataFrame with academic_year, reason_group, and share columns.
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
        context="21_composition.aggregated",
        audit_dir=AUDIT_DIR,
    )

    # Calculate rates for each reason
    for reason_col in REASON_COLUMNS.keys():
        rate_col = f"{reason_col}_rate"
        aggregated[rate_col] = np.where(
            aggregated["cumulative_enrollment"] > 0,
            aggregated[reason_col] / aggregated["cumulative_enrollment"],
            np.nan,
        )

    # Calculate total rate (sum of all reason rates)
    rate_cols = [f"{col}_rate" for col in REASON_COLUMNS.keys()]
    aggregated["total_rate"] = aggregated[rate_cols].sum(axis=1)

    # Calculate shares (rate / total_rate)
    share_data = []
    for idx, row in aggregated.iterrows():
        year = row["academic_year"]
        total_rate = row["total_rate"]

        if pd.isna(total_rate) or total_rate == 0:
            continue

        # Willful Defiance share
        defiance_rate = row["suspension_count_defiance_only_rate"]
        defiance_share = defiance_rate / total_rate if not pd.isna(defiance_rate) else 0

        # Other reasons share (sum of all non-defiance rates)
        other_rate = sum([
            row[f"{col}_rate"]
            for col in REASON_COLUMNS.keys()
            if col != "suspension_count_defiance_only" and not pd.isna(row[f"{col}_rate"])
        ])
        other_share = other_rate / total_rate if total_rate > 0 else 0

        share_data.append({
            "academic_year": year,
            "reason_group": "Willful Defiance",
            "share": defiance_share,
            "enrollment": row["cumulative_enrollment"]
        })
        share_data.append({
            "academic_year": year,
            "reason_group": "Other Reasons",
            "share": other_share,
            "enrollment": row["cumulative_enrollment"]
        })

    result_df = pd.DataFrame(share_data)

    if result_df.empty:
        raise SystemExit("No suspension composition data available after filtering.")

    # Ensure shares are in valid range [0, 1]
    result_df["share"] = result_df["share"].clip(0, 1)

    return result_df


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


def plot_composition_stacked_area(
    df: pd.DataFrame,
    output_dir: Path,
    image_format: str = DEFAULT_IMAGE_FORMAT,
    dpi: int | None = None,
) -> None:
    """Render a stacked area chart showing composition of suspensions over time."""

    if df.empty:
        print("Skipping composition chart: no data to plot.")
        return

    # Get unique years
    if isinstance(df["academic_year"].dtype, pd.CategoricalDtype):
        years = df["academic_year"].cat.categories
    else:
        years = sorted(df["academic_year"].unique())

    x_positions = {year: idx for idx, year in enumerate(years)}

    # Pivot to get shares by year and reason_group
    pivot_df = df.pivot(index="academic_year", columns="reason_group", values="share")

    # Ensure both columns exist
    if "Willful Defiance" not in pivot_df.columns:
        pivot_df["Willful Defiance"] = 0
    if "Other Reasons" not in pivot_df.columns:
        pivot_df["Other Reasons"] = 0

    pivot_df = pivot_df.fillna(0)

    # Prepare data for stacked area
    xs = [x_positions[year] for year in pivot_df.index]
    defiance_values = pivot_df["Willful Defiance"].to_numpy() * 100
    other_values = pivot_df["Other Reasons"].to_numpy() * 100

    # Create figure
    fig, ax = plt.subplots(figsize=(11, 7.25), dpi=dpi or 300)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    # Colors for stacked area
    defiance_color = DISCIPLINE_REASON_PALETTE.get("Willful Defiance", "#FFB81C")
    other_color = DISCIPLINE_BASE_PALETTE["UCLA Blue"]

    # Plot stacked area
    ax.fill_between(
        xs,
        0,
        other_values,
        color=other_color,
        alpha=0.7,
        label="Other Reasons",
        linewidth=0
    )
    ax.fill_between(
        xs,
        other_values,
        other_values + defiance_values,
        color=defiance_color,
        alpha=0.7,
        label="Willful Defiance",
        linewidth=0
    )

    # Add boundary lines for clarity
    ax.plot(xs, other_values + defiance_values, color=TEXT_COLOR, linewidth=1.5, alpha=0.8)
    ax.plot(xs, other_values, color=TEXT_COLOR, linewidth=1.5, alpha=0.8)

    # Add percentage labels
    for i, year in enumerate(pivot_df.index):
        x = x_positions[year]

        # Label for "Other Reasons" (bottom section)
        other_pct = pivot_df.loc[year, "Other Reasons"]
        other_y = other_values[i] / 2  # Middle of the section
        if other_pct > 0.05:  # Only show label if section is large enough
            ax.text(
                x,
                other_y,
                _format_percent(other_pct),
                color="white",
                fontsize=9,
                fontweight="bold",
                ha="center",
                va="center",
            )

        # Label for "Willful Defiance" (top section)
        defiance_pct = pivot_df.loc[year, "Willful Defiance"]
        defiance_y = other_values[i] + (defiance_values[i] / 2)  # Middle of the section
        if defiance_pct > 0.05:  # Only show label if section is large enough
            ax.text(
                x,
                defiance_y,
                _format_percent(defiance_pct),
                color=TEXT_COLOR,
                fontsize=9,
                fontweight="bold",
                ha="center",
                va="center",
            )

    # Configure axes
    ax.set_xticks(list(x_positions.values()))
    ax.set_xticklabels(years, rotation=45, ha="right", color=TEXT_COLOR)
    ax.tick_params(axis="y", colors=TEXT_COLOR)

    ax.set_ylabel("Share of Total Suspensions (Percent)", color=TEXT_COLOR, fontweight="bold")
    ax.set_xlabel("Academic Year", color=TEXT_COLOR, fontweight="bold")

    ax.grid(True, axis="y", color=GRID_COLOR, linestyle="-", linewidth=0.8, alpha=0.5)
    ax.grid(False, axis="x")

    for spine in ax.spines.values():
        spine.set_visible(False)

    # Legend
    legend = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.14),
        ncol=2,
        frameon=False,
        labelcolor=TEXT_COLOR,
    )
    for text in legend.get_texts():
        text.set_fontweight("bold")

    ax.set_ylim(0, 100)
    ax.margins(x=0.02)

    # Add title, subtitle, methodology, and citation
    title = "All Traditional Schools — Suspension Reason Composition"
    subtitle = "Statewide, 2017-18 through 2023-24 (no statewide reporting in 2020-21)"

    fig.text(0.10, 0.96, title, fontsize=13, fontweight="bold", ha="left", color=TEXT_COLOR)
    fig.text(0.10, 0.92, subtitle, fontsize=9, ha="left", color=TEXT_COLOR)

    _add_wrapped_text(
        fig,
        0.10,
        0.89,
        METHODOLOGY_TEXT,
        fontsize=7,
        color=TEXT_COLOR,
        ha="left",
        va="top",
        max_width=180,
    )
    _add_wrapped_text(
        fig,
        0.10,
        0.02,
        STANDARD_CITATION,
        fontsize=6,
        color=CAPTION_COLOR,
        ha="left",
        va="bottom",
        max_width=180,
    )

    fig.subplots_adjust(left=0.12, right=0.96, top=0.78, bottom=0.32)

    # Save figure
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = image_format.lower().lstrip(".")
    output_path = output_dir / f"21_suspension_reason_composition_statewide.{suffix}"
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
    composition_df = prepare_composition_data(raw_df)
    dpi = args.dpi if args.image_format != "svg" else None
    plot_composition_stacked_area(
        composition_df,
        output_dir=args.output_dir,
        image_format=args.image_format,
        dpi=dpi,
    )
    print(f"Composition chart generation complete.")


if __name__ == "__main__":
    main()
