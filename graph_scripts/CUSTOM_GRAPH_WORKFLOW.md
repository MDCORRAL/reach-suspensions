# Custom Graph Workflow Guide

**Purpose**: This guide explains how to create individual custom graphs without running large scripts that generate multiple outputs.

---

## Philosophy

The graph scripts follow a **modular architecture** that separates:
- **Data loading** (reusable across scripts)
- **Data preparation** (filtering, aggregation)
- **Plotting logic** (visualization)
- **Styling** (UCLA brand colors, layout)

This allows you to:
1. Generate single graphs on demand
2. Create custom variations easily
3. Experiment with different visualizations
4. Avoid regenerating all outputs when you only need one

---

## Directory Structure

```
graph_scripts/
├── palette_utils.py           # Shared: UCLA colors, citation
├── data_validations.py        # Shared: Data quality checks
├── plotting_helpers.py        # Shared: Reusable plotting functions (NEW)
├── data_sources.py            # Shared: Data loading utilities (NEW)
│
├── 06_statewide_trends.py     # Full pipeline: multiple statewide charts
├── 20_suspension_reason_trends_*.py  # Full pipelines
│
├── custom_charts/             # Individual custom charts (NEW)
│   ├── README.md              # Guide for custom charts
│   ├── template_custom_chart.py  # Copy this to start new charts
│   └── smooth_statewide_reasons.py  # Example: smoothed line chart
│
└── CUSTOM_GRAPH_WORKFLOW.md  # This file
```

---

## Workflow: Creating a Custom Graph

### Option 1: Quick Custom Chart (Recommended)

**Use Case**: You want a one-off variation of an existing chart

**Steps**:

1. **Copy the template**:
   ```bash
   cd graph_scripts/custom_charts
   cp template_custom_chart.py my_custom_chart.py
   ```

2. **Edit the configuration section**:
   ```python
   # Configure your chart
   CHART_TITLE = "My Custom Chart Title"
   CHART_SUBTITLE = "Description of scope and time period"
   OUTPUT_FILENAME = "my_custom_chart.png"

   # Customize data filters
   SCHOOL_LEVELS = ["Elementary", "Middle", "High"]
   LOCALES = ["City", "Suburban"]
   ```

3. **Customize the plotting function**:
   - Modify colors, line styles, markers
   - Add/remove data labels
   - Adjust smoothing, interpolation
   - Change layout and sizing

4. **Run your custom script**:
   ```bash
   source ../.venv/bin/activate
   python my_custom_chart.py
   ```

### Option 2: Add to Main Scripts

**Use Case**: You want a permanent addition to the standard outputs

**Steps**:

1. **Add a new function** to the appropriate main script (e.g., `06_statewide_trends.py`)
2. **Follow the existing pattern**:
   - Data preparation function
   - Plotting function with `render` flag
   - Call from `main()` with optional flag
3. **Add command-line argument** to enable/disable the new output
4. **Document** in the script's docstring

---

## Shared Utilities

### `palette_utils.py`

**What it provides**:
- `DISCIPLINE_BASE_PALETTE`: UCLA brand colors
- `DISCIPLINE_REASON_PALETTE`: Suspension reason colors
- `STANDARD_CITATION`: Source attribution text

**Usage**:
```python
from palette_utils import DISCIPLINE_BASE_PALETTE, STANDARD_CITATION

TEXT_COLOR = DISCIPLINE_BASE_PALETTE["Darkest Blue"]
CAPTION_COLOR = DISCIPLINE_BASE_PALETTE["Grey"]

# Add citation to your chart
fig.text(0.07, 0.02, STANDARD_CITATION, fontsize=9, color=CAPTION_COLOR)
```

### `data_validations.py`

**What it provides**:
- `audit_counts_against_enrollment()`: Validate suspension counts
- `sanitize_rate_column()`: Clean rate calculations
- `ensure_audit_dir()`: Create audit directory

**Usage**:
```python
from data_validations import audit_counts_against_enrollment, sanitize_rate_column

# Validate data quality
df = audit_counts_against_enrollment(
    df,
    count_columns=["total_suspensions"],
    enrollment_column="cumulative_enrollment",
    context="my_custom_chart",
    audit_dir=AUDIT_DIR,
)

# Clean rates
df = sanitize_rate_column(df, rate_column="rate", context="my_custom_chart")
```

### `data_sources.py` (NEW)

**What it provides**:
- `load_susp_v6_long()`: Load long-format suspension data
- `load_susp_v6_features()`: Load wide-format features
- `filter_traditional_schools()`: Apply standard filters
- `prepare_reason_data()`: Prepare suspension reason data

**Usage**:
```python
from data_sources import load_susp_v6_long, filter_traditional_schools

# Load data with standard filters
df = load_susp_v6_long()
df = filter_traditional_schools(df)
```

### `plotting_helpers.py` (NEW)

**What it provides**:
- `apply_ucla_style()`: Apply standard UCLA chart styling
- `add_standard_labels()`: Add title, subtitle, citation
- `smooth_line_data()`: Interpolate data for smooth curves
- `format_academic_years()`: Format year labels consistently

**Usage**:
```python
from plotting_helpers import apply_ucla_style, add_standard_labels, smooth_line_data

# Apply UCLA styling to axis
apply_ucla_style(ax, year_labels, y_limit=0.15)

# Smooth line for prettier curves
x_smooth, y_smooth = smooth_line_data(x_data, y_data, smoothness=300)

# Add standard labels
add_standard_labels(
    fig,
    title="My Chart Title",
    subtitle="Time period and scope",
    citation=STANDARD_CITATION,
)
```

---

## Common Customizations

### Remove Data Labels

**Before** (with labels):
```python
for x_val, y_val, rate_val in zip(xs, ys, df["rate"]):
    label = f"{rate_val * 100:.1f}%"
    ax.text(x_val, y_val, label, color=color, fontsize=9, fontweight="bold")
```

**After** (no labels):
```python
# Skip the ax.text() loop entirely
```

### Smooth Lines Instead of Point-to-Point

**Before** (straight lines):
```python
ax.plot(xs, ys, color=color, linewidth=2.2, marker="o")
```

**After** (smooth curves):
```python
from plotting_helpers import smooth_line_data

# Generate smooth curve
x_smooth, y_smooth = smooth_line_data(xs, ys, smoothness=300)

# Plot smooth line without markers
ax.plot(x_smooth, y_smooth, color=color, linewidth=2.2)
```

### Change Line Styles

```python
# Dashed line
ax.plot(xs, ys, linestyle="--", color=color)

# Dotted line
ax.plot(xs, ys, linestyle=":", color=color)

# Thicker line
ax.plot(xs, ys, linewidth=3.5, color=color)

# No markers
ax.plot(xs, ys, marker=None, color=color)

# Different marker styles
ax.plot(xs, ys, marker="s", markersize=8, color=color)  # Square
ax.plot(xs, ys, marker="^", markersize=8, color=color)  # Triangle
```

### Customize Colors

```python
# Use different UCLA colors
from palette_utils import DISCIPLINE_BASE_PALETTE

line_color = DISCIPLINE_BASE_PALETTE["Darker Gold"]
background_color = DISCIPLINE_BASE_PALETTE["Lightest Blue"]

# Custom palette for specific chart
CUSTOM_PALETTE = {
    "Category A": DISCIPLINE_BASE_PALETTE["UCLA Blue"],
    "Category B": DISCIPLINE_BASE_PALETTE["Purple"],
    "Category C": DISCIPLINE_BASE_PALETTE["Darker Gold"],
}
```

### Adjust Figure Size and DPI

```python
# Larger figure
fig, ax = plt.subplots(figsize=(14, 8), dpi=300)

# Smaller figure
fig, ax = plt.subplots(figsize=(8, 5), dpi=300)

# High-resolution for print
fig.savefig(output_path, dpi=600)
```

---

## Examples

### Example 1: Generate Single Chart from Existing Script

**Scenario**: You only want the Elementary school chart from `06_statewide_trends.py`

**Solution**: Run with filters

```bash
# Option A: Modify LEVEL_ORDER in the script temporarily
# Edit 06_statewide_trends.py line 168:
LEVEL_ORDER = ["Elementary"]  # Only Elementary

# Run script
python 06_statewide_trends.py

# Option B: Create custom script that calls the function
# See custom_charts/single_level_chart.py
```

### Example 2: Custom Color Palette

**Scenario**: You want a chart with different colors

**Solution**: Override palette in custom script

```python
from palette_utils import DISCIPLINE_BASE_PALETTE

# Custom palette for your chart
MY_PALETTE = {
    "Black/African American": DISCIPLINE_BASE_PALETTE["Darkest Blue"],
    "Hispanic/Latino": DISCIPLINE_BASE_PALETTE["Purple"],
    "White": DISCIPLINE_BASE_PALETTE["Grey"],
}

# Use in plot
for race, color in MY_PALETTE.items():
    ax.plot(x, y, color=color, label=race)
```

### Example 3: Smoothed Line Chart

**Scenario**: You want smooth curves instead of point-to-point lines

**Solution**: Use `smooth_line_data()` helper

```python
from plotting_helpers import smooth_line_data

# Original data points
xs = [0, 1, 2, 3, 4, 5, 6]
ys = [0.05, 0.07, 0.06, 0.08, 0.07, 0.06, 0.05]

# Generate smooth curve
x_smooth, y_smooth = smooth_line_data(xs, ys, smoothness=300)

# Plot smooth line
ax.plot(x_smooth, y_smooth, color=color, linewidth=2.2)
```

---

## Best Practices

### 1. Always Use Shared Utilities

✅ **Do**:
```python
from palette_utils import DISCIPLINE_BASE_PALETTE, STANDARD_CITATION
from data_validations import audit_counts_against_enrollment

TEXT_COLOR = DISCIPLINE_BASE_PALETTE["Darkest Blue"]
```

❌ **Don't**:
```python
# Hard-coded colors
TEXT_COLOR = "#003B5C"  # Don't do this!

# Skip validation
# (No audit_counts_against_enrollment call)
```

### 2. Add Citations to All Charts

✅ **Do**:
```python
from palette_utils import STANDARD_CITATION

fig.text(0.07, 0.02, STANDARD_CITATION, fontsize=9, color=CAPTION_COLOR)
```

❌ **Don't**:
```python
# No citation (not publication-ready!)
```

### 3. Use Descriptive Output Names

✅ **Do**:
```python
output_path = "custom_elementary_smooth_trends_2024.png"
```

❌ **Don't**:
```python
output_path = "test.png"  # Too generic!
output_path = "chart1.png"  # Not descriptive!
```

### 4. Document Your Custom Scripts

✅ **Do**:
```python
"""Generate smoothed statewide suspension reason trends.

This custom chart shows the same data as the standard statewide
reasons chart but with smoothed lines and no data labels for a
cleaner presentation suitable for publications.

Output: custom_smooth_statewide_reasons.png
"""
```

### 5. Save Custom Charts to Organized Directory

✅ **Do**:
```python
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "custom_charts"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
```

---

## Troubleshooting

### Issue: Import errors for shared modules

**Error**: `ModuleNotFoundError: No module named 'palette_utils'`

**Solution**: Add graph_scripts to Python path

```python
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
GRAPH_SCRIPTS_DIR = SCRIPT_DIR.parent  # If in custom_charts/
if str(GRAPH_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(GRAPH_SCRIPTS_DIR))

from palette_utils import DISCIPLINE_BASE_PALETTE
```

### Issue: Data file not found

**Error**: `FileNotFoundError: data-stage/susp_v6_long.parquet`

**Solution**: Ensure you're running from correct directory or use absolute paths

```python
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_PATH = PROJECT_ROOT / "data-stage" / "susp_v6_long.parquet"
```

### Issue: Smooth lines look jagged

**Solution**: Increase smoothness parameter

```python
# More points = smoother curve
x_smooth, y_smooth = smooth_line_data(xs, ys, smoothness=500)  # Increase from 300
```

---

## Next Steps

1. **Explore existing scripts** to understand the patterns
2. **Copy the template** from `custom_charts/template_custom_chart.py`
3. **Customize** for your specific needs
4. **Test** with your data
5. **Document** your custom chart in its docstring
6. **Share** reusable components back to `plotting_helpers.py`

---

## Additional Resources

- **UCLA Brand Guidelines**: `docs/protocols/UCLA-Brand-Colors.md`
- **Citation Standard**: `palette_utils.py` (STANDARD_CITATION)
- **Consistency Review**: `graph_scripts/CONSISTENCY_REVIEW_REPORT.md`
- **Main Documentation**: `graph_scripts/README.md`

---

**Questions?** Review the existing scripts in `graph_scripts/` or check the consistency review report for examples of proper styling and usage patterns.
