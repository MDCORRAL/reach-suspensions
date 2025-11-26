"""Shared plotting utilities for creating UCLA-branded charts.

This module provides reusable functions for consistent chart styling,
data smoothing, and layout across all visualization scripts.
"""

from __future__ import annotations

import math
from typing import Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter
from scipy import interpolate

from palette_utils import DISCIPLINE_BASE_PALETTE, STANDARD_CITATION


# Default UCLA colors for charts
TEXT_COLOR = DISCIPLINE_BASE_PALETTE["Darkest Blue"]
CAPTION_COLOR = DISCIPLINE_BASE_PALETTE["Grey"]
GRID_COLOR_PRIMARY = DISCIPLINE_BASE_PALETTE["Lighter Blue"]
GRID_COLOR_SECONDARY = DISCIPLINE_BASE_PALETTE["Grey"]


def apply_ucla_style(
    ax: plt.Axes,
    year_labels: Sequence[str],
    y_limit: float,
    *,
    show_x_grid: bool = True,
    show_y_grid: bool = True,
    rotate_x_labels: bool = True,
) -> None:
    """Apply standard UCLA chart styling to an axis.

    Args:
        ax: Matplotlib axis to style
        year_labels: List of academic year labels for x-axis
        y_limit: Maximum y-axis value
        show_x_grid: Whether to show vertical grid lines (default: True)
        show_y_grid: Whether to show horizontal grid lines (default: True)
        rotate_x_labels: Whether to rotate x-axis labels 45° (default: True)
    """
    # White background
    ax.set_facecolor("white")

    # Remove spines
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Grid styling
    if show_y_grid:
        ax.grid(axis="y", color=GRID_COLOR_PRIMARY, linewidth=0.8, alpha=0.9)

    if show_x_grid:
        ax.grid(
            axis="x",
            color=GRID_COLOR_PRIMARY,
            linewidth=0.5,
            linestyle="--",
            alpha=0.4,
        )

    # X-axis configuration
    ax.set_xticks(range(len(year_labels)))
    if rotate_x_labels:
        ax.set_xticklabels(year_labels, rotation=45, ha="right")
    else:
        ax.set_xticklabels(year_labels)

    ax.tick_params(axis="x", labelsize=10, pad=6, colors=TEXT_COLOR)
    ax.tick_params(axis="y", labelsize=10, colors=TEXT_COLOR)

    # Y-axis configuration
    ax.set_ylim(0, y_limit)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y * 100:.1f}%"))

    # Margins
    ax.margins(x=0.02)
    ax.set_xlim(-0.35, len(year_labels) - 0.65)


def add_standard_labels(
    fig: plt.Figure,
    *,
    title: str,
    subtitle: str = "",
    citation: str = STANDARD_CITATION,
    title_x: float = 0.07,
    title_y: float = 0.98,
    subtitle_y: float = 0.95,
    citation_y: float = 0.02,
) -> None:
    """Add standard title, subtitle, and citation to a figure.

    Args:
        fig: Matplotlib figure
        title: Main chart title
        subtitle: Descriptive subtitle (time period, scope, etc.)
        citation: Source citation (defaults to STANDARD_CITATION)
        title_x: X position for title (0-1, default: 0.07)
        title_y: Y position for title (0-1, default: 0.98)
        subtitle_y: Y position for subtitle (0-1, default: 0.95)
        citation_y: Y position for citation (0-1, default: 0.02)

    Note:
        Use with fig.subplots_adjust(left=0.10, right=0.95, top=0.85, bottom=0.18)
        to ensure proper spacing for title, subtitle, and citation.
    """
    fig.patch.set_facecolor("white")

    # Title
    fig.text(
        title_x,
        title_y,
        title,
        fontsize=20,
        fontweight="bold",
        ha="left",
        color=TEXT_COLOR,
    )

    # Subtitle (if provided)
    if subtitle:
        fig.text(
            title_x,
            subtitle_y,
            subtitle,
            fontsize=13,
            ha="left",
            color=TEXT_COLOR,
        )

    # Citation
    fig.text(
        title_x,
        citation_y,
        citation,
        fontsize=10,
        color=CAPTION_COLOR,
        ha="left",
    )


def smooth_line_data(
    x_data: Sequence[float],
    y_data: Sequence[float],
    *,
    smoothness: int = 300,
    kind: str = "cubic",
) -> Tuple[np.ndarray, np.ndarray]:
    """Interpolate data points to create smooth curves.

    Args:
        x_data: X coordinates of data points
        y_data: Y coordinates of data points
        smoothness: Number of interpolated points (higher = smoother, default: 300)
        kind: Interpolation method ('linear', 'cubic', 'quadratic', default: 'cubic')

    Returns:
        Tuple of (x_smooth, y_smooth) arrays for plotting smooth curves

    Example:
        >>> xs = [0, 1, 2, 3, 4, 5]
        >>> ys = [0.05, 0.07, 0.06, 0.08, 0.07, 0.06]
        >>> x_smooth, y_smooth = smooth_line_data(xs, ys, smoothness=300)
        >>> ax.plot(x_smooth, y_smooth, color="blue", linewidth=2.2)
    """
    x_array = np.array(x_data)
    y_array = np.array(y_data)

    # Ensure data is sorted by x
    sort_idx = np.argsort(x_array)
    x_sorted = x_array[sort_idx]
    y_sorted = y_array[sort_idx]

    # Remove any NaN or infinite values
    valid_mask = np.isfinite(x_sorted) & np.isfinite(y_sorted)
    x_clean = x_sorted[valid_mask]
    y_clean = y_sorted[valid_mask]

    if len(x_clean) < 2:
        # Not enough points to interpolate
        return x_clean, y_clean

    # Create smooth x values
    x_min, x_max = x_clean[0], x_clean[-1]
    x_smooth = np.linspace(x_min, x_max, smoothness)

    # Interpolate y values
    if kind == "cubic" and len(x_clean) >= 4:
        # Use cubic spline for smooth curves
        spline = interpolate.CubicSpline(x_clean, y_clean)
        y_smooth = spline(x_smooth)
    elif kind == "quadratic" and len(x_clean) >= 3:
        # Use quadratic interpolation
        f = interpolate.interp1d(x_clean, y_clean, kind="quadratic", fill_value="extrapolate")
        y_smooth = f(x_smooth)
    else:
        # Fall back to linear interpolation
        f = interpolate.interp1d(x_clean, y_clean, kind="linear", fill_value="extrapolate")
        y_smooth = f(x_smooth)

    return x_smooth, y_smooth


def format_academic_years(years: Sequence[str]) -> list[str]:
    """Format academic year labels consistently.

    Args:
        years: List of academic years (e.g., ["2017-18", "2018-19", ...])

    Returns:
        Formatted year labels

    Example:
        >>> format_academic_years(["2017-18", "2018-19"])
        ["'17-'18", "'18-'19"]
    """
    formatted = []
    for year in years:
        if "-" in str(year):
            parts = str(year).split("-")
            if len(parts) == 2:
                # Abbreviated format: '17-'18
                formatted.append(f"'{parts[0][-2:]}-'{parts[1]}")
            else:
                formatted.append(str(year))
        else:
            formatted.append(str(year))
    return formatted


def calculate_y_limit(max_value: float, padding: float = 0.18) -> float:
    """Calculate appropriate y-axis limit with padding.

    Args:
        max_value: Maximum data value
        padding: Padding as proportion of max_value (default: 0.18 = 18%)

    Returns:
        Y-axis limit with padding

    Example:
        >>> calculate_y_limit(0.12, padding=0.18)
        0.1416
    """
    if not isinstance(max_value, (int, float)) or not math.isfinite(max_value):
        return 0.05  # Default fallback

    return max_value + max(0.01, max_value * padding)


def create_legend_handles(
    labels: Sequence[str],
    colors: Sequence[str],
    *,
    linestyle: str = "-",
    linewidth: float = 2.2,
    marker: str | None = "o",
    markersize: float = 5.3,
) -> list[plt.Line2D]:
    """Create custom legend handles for manual legend creation.

    Args:
        labels: Legend labels
        colors: Line colors (must match length of labels)
        linestyle: Line style (default: "-")
        linewidth: Line width (default: 2.2)
        marker: Marker style (default: "o", None for no marker)
        markersize: Marker size (default: 5.3)

    Returns:
        List of Line2D objects for legend

    Example:
        >>> handles = create_legend_handles(
        ...     labels=["Category A", "Category B"],
        ...     colors=["#2774AE", "#FFD100"],
        ... )
        >>> ax.legend(handles=handles, labels=labels)
    """
    from matplotlib.lines import Line2D

    handles = []
    for label, color in zip(labels, colors):
        handle = Line2D(
            [0],
            [0],
            color=color,
            linewidth=linewidth,
            linestyle=linestyle,
            marker=marker,
            markersize=markersize,
            markeredgecolor="white",
            markeredgewidth=0.6 if marker else 0,
        )
        handles.append(handle)

    return handles


def slugify_filename(text: str) -> str:
    """Convert text to filesystem-friendly slug.

    Args:
        text: Text to slugify

    Returns:
        Lowercase slug with underscores

    Example:
        >>> slugify_filename("Elementary Schools — City Locale")
        "elementary_schools_city_locale"
    """
    # Replace special characters
    slug = text.lower()
    slug = slug.replace("—", "_").replace("–", "_")
    slug = slug.replace("/", "_").replace(" ", "_")
    slug = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in slug)

    # Remove consecutive underscores
    while "__" in slug:
        slug = slug.replace("__", "_")

    return slug.strip("_")
