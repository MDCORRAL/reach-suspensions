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
import importlib.util
import sys
import textwrap
from contextlib import redirect_stdout
from pathlib import Path
from typing import Iterable

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
    "adjustText": "pip install adjustText",
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
        "Missing required Python packages for 20_suspension_reason_trends_by_level_and_locale.\n"
        "Install the dependencies before running (e.g., pip install -r graph_scripts/requirements.txt).\n"
        "Missing modules:\n" + "\n".join(hints)
    )
    raise SystemExit(message)

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from adjustText import adjust_text

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

REASON_PALETTE = DISCIPLINE_REASON_PALETTE.copy()

LEVEL_ORDER = ["Elementary", "Middle", "High"]
LOCALE_COLUMN = "locale_simple"
LOCALE_ORDER = ["City", "Suburban", "Town", "Rural", "Unknown"]

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
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "20_suspension_reason_trends_by_level_and_locale"
DEFAULT_IMAGE_FORMAT = "png"
AUDIT_DIR = ensure_audit_dir(PROJECT_ROOT)

# Methodology explanation for charts
METHODOLOGY_TEXT = (
    "Methodology: Suspension rates are calculated as total suspensions divided by cumulative "
    "enrollment for each academic year. Data aggregated by school level and locale from "
    "individual school-level reports. Rates represent the percentage of enrolled students "
    "who received at least one suspension for each reason category."
)

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

    aggregated = audit_counts_against_enrollment(
        aggregated,
        count_columns=list(REASON_COLUMNS.keys()),
        enrollment_column="cumulative_enrollment",
        context="20_level_locale.aggregated",
        audit_dir=AUDIT_DIR,
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
        statewide_melted = sanitize_rate_column(
            statewide_melted,
            rate_column="rate",
            context="20_level_locale.statewide_melted",
            audit_dir=AUDIT_DIR,
        )
        
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
    melted = sanitize_rate_column(
        melted,
        rate_column="rate",
        context="20_level_locale.melted",
        audit_dir=AUDIT_DIR,
    )

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

    fig, ax = plt.subplots(figsize=(11, 7.25), dpi=dpi or 300)
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
        y_offset = max(ys) * 0.02 + 0.1 if len(ys) else 0
        for x_val, y_val, rate_val in zip(xs, ys, reason_df["rate"]):
            label = _format_percent(rate_val)
            text = ax.text(
                x_val,
                y_val + y_offset,
                label,
                color=color,
                fontsize=9,
                fontweight="bold",
                ha="center",
                va="bottom",
            )
            texts.append(text)

    if texts:
        with redirect_stdout(io.StringIO()):
            adjust_text(
                texts,
                ax=ax,
                expand_points=(1.3, 1.6),
                expand_text=(1.2, 1.6),
                only_move={"points": "y", "text": "xy"},
                autoalign="y",
            )

    ax.set_xticks(list(x_positions.values()))
    ax.set_xticklabels(years, rotation=45, ha="right", color=TEXT_COLOR)
    ax.tick_params(axis="y", colors=TEXT_COLOR)

    ax.set_ylabel("Suspension Rate (Percent)", color=TEXT_COLOR, fontweight="bold")
    ax.set_xlabel("Academic Year", color=TEXT_COLOR, fontweight="bold")

    ax.grid(True, axis="y", color=GRID_COLOR, linestyle="-", linewidth=0.8)
    ax.grid(False, axis="x")

    for spine in ax.spines.values():
        spine.set_visible(False)

    legend = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.22),
        ncol=3,
        frameon=False,
        labelcolor=TEXT_COLOR,
    )
    for text in legend.get_texts():
        text.set_fontweight("bold")

    ax.set_ylim(bottom=0)
    ax.margins(x=0.02, y=0.05)

    # Add title, subtitle, methodology, and citation using fig.text for complete control
    title = f"{level} Schools ({locale}) — Suspension Rates by Reason"
    subtitle = "Traditional schools, 2017-18 through 2023-24 (no statewide reporting in 2020-21)"

    fig.text(0.10, 0.96, title, fontsize=13, fontweight="bold", ha="left", color=TEXT_COLOR)
    fig.text(0.10, 0.92, subtitle, fontsize=9, ha="left", color=TEXT_COLOR)

    _add_wrapped_text(
        fig,
        0.10,
        0.89, #updated
        METHODOLOGY_TEXT,
        fontsize=7,
        color=TEXT_COLOR,
        ha="left",
        va="top", #updated
        max_width=180, #updated
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

    fig.subplots_adjust(left=0.06, right=0.94, top=0.78, bottom=0.30) #updated
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

    fig, ax = plt.subplots(figsize=(11, 7.25), dpi=dpi or 300)
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
        y_offset = max(ys) * 0.02 + 0.1 if len(ys) else 0
        for x_val, y_val, rate_val in zip(xs, ys, reason_df["rate"]):
            label = _format_percent(rate_val)
            text = ax.text(
                x_val,
                y_val + y_offset,
                label,
                color=color,
                fontsize=9,
                fontweight="bold",
                ha="center",
                va="bottom",
            )
            texts.append(text)

    if texts:
        with redirect_stdout(io.StringIO()):
            adjust_text(
                texts,
                ax=ax,
                expand_points=(1.3, 1.6),
                expand_text=(1.2, 1.6),
                only_move={"points": "y", "text": "xy"},
                autoalign="y",
            )

    ax.set_xticks(list(x_positions.values()))
    ax.set_xticklabels(years, rotation=45, ha="right", color=TEXT_COLOR)
    ax.tick_params(axis="y", colors=TEXT_COLOR)

    ax.set_ylabel("Suspension Rate (Percent)", color=TEXT_COLOR, fontweight="bold")
    ax.set_xlabel("Academic Year", color=TEXT_COLOR, fontweight="bold")

    ax.grid(True, axis="y", color=GRID_COLOR, linestyle="-", linewidth=0.8)
    ax.grid(False, axis="x")

    for spine in ax.spines.values():
        spine.set_visible(False)

    legend = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.14),
        ncol=3,
        frameon=False,
        labelcolor=TEXT_COLOR,
    )
    for text in legend.get_texts():
        text.set_fontweight("bold")

    ax.set_ylim(bottom=0)
    ax.margins(x=0.02, y=0.05)

    # Add title, subtitle, methodology, and citation using fig.text for complete control
    title = "All Traditional Schools — Statewide Suspension Rates by Reason"
    subtitle = "All traditional public schools, 2017-18 through 2023-24 (no statewide reporting in 2020-21)"

    fig.text(0.10, 0.96, title, fontsize=13, fontweight="bold", ha="left", color=TEXT_COLOR)
    fig.text(0.10, 0.92, subtitle, fontsize=9, ha="left", color=TEXT_COLOR)

    # Add methodology and citation in the bottom margin to keep the plot area clear
    _add_wrapped_text(fig, 0.10, 0.12, METHODOLOGY_TEXT, fontsize=7, color=TEXT_COLOR,
                      ha="left", max_width=110)
    _add_wrapped_text(fig, 0.10, 0.07, STANDARD_CITATION, fontsize=6, color=CAPTION_COLOR,
                      ha="left", max_width=110)

    fig.subplots_adjust(left=0.12, right=0.96, top=0.84, bottom=0.32)
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
