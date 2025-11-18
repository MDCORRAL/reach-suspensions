# Citation Standard for REACH Suspension Analysis

## Overview

All outputs from this repository (graphs, tables, reports, dashboards) use a **standardized, human-readable citation** that clearly describes the data source and analysis methodology. This ensures that anyone reading our analyses can easily understand what data was used and how it was processed.

## Standard Citation Text

The standardized citation used across all outputs is:

> **Source:** REACH analysis of 2017-18 through 2023-24 suspension data from the California Department of Education's California Longitudinal Pupil Achievement Data System (CALPADS). Analysis includes traditional public schools aggregated at the school level, with suspension rates calculated as total suspensions divided by cumulative enrollment.

## Why This Citation?

This citation format was chosen to provide clear, accessible information:

1. **Who did the analysis**: REACH (not just "we" or unnamed)
2. **What time period**: Specific academic years (2017-18 through 2023-24)
3. **What data source**: California Department of Education's CALPADS system (not just file names like "susp_v6_long.parquet")
4. **What was analyzed**: Traditional public schools
5. **How rates were calculated**: Total suspensions divided by cumulative enrollment

### What We Changed

**Before** (cryptic):
```
Source: California statewide suspension data (susp_v6_long + v6 features)
```

**After** (human-readable):
```
Source: REACH analysis of 2017-18 through 2023-24 suspension data from the
California Department of Education's California Longitudinal Pupil Achievement
Data System (CALPADS). Analysis includes traditional public schools aggregated
at the school level, with suspension rates calculated as total suspensions
divided by cumulative enrollment.
```

## How to Use the Standard Citation

### In R Scripts

Use the `standard_citation()` function from `graph_scripts/graph_utils.R`:

```r
# Load the utilities
source(here::here("graph_scripts", "graph_utils.R"))

# Use in ggplot2 captions
ggplot(data, aes(x, y)) +
  geom_line() +
  labs(
    title = "My Title",
    caption = standard_citation()  # Standard citation
  )

# Or with text wrapping for long labels
labs(
  caption = standard_citation(wrap_width = 120)
)
```

### In Python Scripts

Import the `STANDARD_CITATION` constant from `graph_scripts/palette_utils.py`:

```python
from palette_utils import STANDARD_CITATION

# Use in matplotlib captions
fig.text(0.07, 0.05, STANDARD_CITATION, fontsize=10, ha="left")
```

### In HTML/Dashboards

Use the full citation text in data source sections:

```html
<section aria-labelledby="data-sources-heading">
  <h3 id="data-sources-heading">Data sources</h3>
  <p>
    <strong>Source:</strong> REACH analysis of 2017-18 through 2023-24
    suspension data from the California Department of Education's California
    Longitudinal Pupil Achievement Data System (CALPADS). Analysis includes
    traditional public schools aggregated at the school level, with suspension
    rates calculated as total suspensions divided by cumulative enrollment.
  </p>
</section>
```

## Updated Files

The following files have been updated to use the standardized citation:

### R Graph Scripts
- `graph_scripts/01_statewide_disparities.R`
- `graph_scripts/02_statewide_quartiles.R`
- `graph_scripts/03_elementary_disparities.R`
- `graph_scripts/04_elementary_quartiles.R`
- `graph_scripts/05_unequal_burden.R`
- `graph_scripts/07_quartile_enrollment_comparison.R`
- `graph_scripts/08_comprehensive_rates_plots.R`
- `graph_scripts/09_nonrace_demographic_trends.R`
- `graph_scripts/21_black_quartile_suspension_trends.R`

### Python Graph Scripts
- `graph_scripts/06_statewide_trends.py`

### Utility Files
- `graph_scripts/graph_utils.R` - Added `standard_citation()` function
- `graph_scripts/palette_utils.py` - Added `STANDARD_CITATION` constant

### HTML Dashboards
- `tail_concentration_dashboard.html`

## Data Details

### Academic Years Covered
The data covers the following academic years:
- 2017-18
- 2018-19
- 2019-20
- 2021-22 (2020-21 is missing due to COVID-19 disruptions)
- 2022-23
- 2023-24

### Data Source
- **Primary Source**: California Department of Education (CDE)
- **Specific System**: California Longitudinal Pupil Achievement Data System (CALPADS)
- **Data Type**: School-level suspension records with demographic breakdowns

### Analysis Scope
- **Schools Included**: Traditional public schools (excludes alternative settings such as continuation schools, community day schools, etc.)
- **Geographic Scope**: California statewide
- **Aggregation Level**: School (campus) level
- **Rate Calculation**: Total suspensions ÷ cumulative enrollment

## Questions or Updates

If you need to modify the citation (e.g., to reflect updated year ranges or different data sources), update:

1. **For R scripts**: The `standard_citation()` function in `graph_scripts/graph_utils.R`
2. **For Python scripts**: The `STANDARD_CITATION` constant in `graph_scripts/palette_utils.py`
3. **For HTML**: Update each HTML file's data sources section individually

All changes should maintain the same level of clarity and detail.
